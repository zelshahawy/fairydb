// Reference: https://github.com/rotaki/decorrelator

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use common::{
    catalog::get_column_index_from_temp_col_id, query::origin_expr::OriginExpression,
    query::rules::RulesRef,
};
use common::{
    catalog::CatalogRef,
    datatypes::{default_decimal_precision, default_decimal_scale},
    ids::ColumnId,
    logical_expr::prelude::{Expression, JoinType},
    physical::col_id_generator::ColIdGeneratorRef,
    traits::plan::Plan,
    AggOp, BinaryOp,
};
use common::{logical_expr::prelude::LogicalRelExpr, Field};
use common::{CrustyError, DataType};
use sqlparser::ast::{self, ExactNumberInfo};

/// Retrieve the name from the command parser object.
///
/// # Argument
///
/// * `name` - Name object from the command parser.
///   FIXME: This function serves the same purpose as `get_table_name` below.
pub fn get_name(name: &ast::ObjectName) -> Result<String, CrustyError> {
    if name.0.len() > 1 {
        Err(CrustyError::CrustyError(String::from(
            "Error no . names supported",
        )))
    } else {
        Ok(name.0[0].value.clone())
    }
}

/// Retrieve the dtype from the command parser object.
///
/// # Argument
///
/// * `dtype` - Name object from the command parser.
pub fn get_attr(dtype: &ast::DataType) -> Result<DataType, CrustyError> {
    match dtype {
        ast::DataType::Int(_) | ast::DataType::Integer(_) => Ok(DataType::BigInt),
        ast::DataType::Varchar(_) => Ok(DataType::String),
        ast::DataType::Char(_) => Ok(DataType::String),
        ast::DataType::Date => Ok(DataType::Date),
        ast::DataType::Decimal(exact_num_info) => match exact_num_info {
            ExactNumberInfo::PrecisionAndScale(p, s) => Ok(DataType::Decimal(*p as u32, *s as u32)),
            ExactNumberInfo::Precision(p) => {
                Ok(DataType::Decimal(*p as u32, default_decimal_scale()))
            }
            ExactNumberInfo::None => Ok(DataType::Decimal(
                default_decimal_precision(),
                default_decimal_scale(),
            )),
        },
        _ => Err(CrustyError::CrustyError(format!(
            "Unsupported data type {:?}",
            dtype
        ))),
    }
}

pub type EnvironmentRef = Arc<Environment>;

#[derive(Debug, Clone)]
pub struct Environment {
    outer: Option<EnvironmentRef>,
    columns: Arc<RwLock<HashMap<String, ColumnId>>>,

    /// The opposite of `rename`: maps the uniquely generated column id to the
    /// original column index and container id
    id_to_origin: Arc<RwLock<HashMap<ColumnId, OriginExpression>>>,

    /// Catalog: used to find where the column is coming from
    catalog: CatalogRef,
}

impl Environment {
    fn new(catalog: CatalogRef) -> Environment {
        Environment {
            outer: None,
            columns: Arc::new(RwLock::new(HashMap::new())),
            id_to_origin: Arc::new(RwLock::new(HashMap::new())),
            catalog,
        }
    }

    fn new_with_outer(outer: EnvironmentRef, catalog: CatalogRef) -> Environment {
        Environment {
            outer: Some(outer),
            columns: Arc::new(RwLock::new(HashMap::new())),
            id_to_origin: Arc::new(RwLock::new(HashMap::new())),
            catalog,
        }
    }

    fn get(&self, name: &str) -> Option<usize> {
        if let Some(index) = self.columns.read().unwrap().get(name) {
            return Some(*index);
        }

        if let Some(outer) = &self.outer {
            return outer.get(name);
        }

        None
    }

    fn get_at(&self, distance: usize, name: &str) -> Option<usize> {
        if distance == 0 {
            if let Some(index) = self.columns.read().unwrap().get(name) {
                return Some(*index);
            } else {
                return None;
            }
        }

        if let Some(outer) = &self.outer {
            return outer.get_at(distance - 1, name);
        }

        None
    }

    fn set(&self, name: &str, index: usize) {
        self.columns
            .write()
            .unwrap()
            .insert(name.to_string(), index);
    }

    fn add_to_origin_map(&self, col_id: ColumnId, origin: OriginExpression) {
        self.id_to_origin.write().unwrap().insert(col_id, origin);
    }

    fn get_names(&self, col_id: usize) -> Vec<String> {
        let mut names = Vec::new();
        for (name, index) in self.columns.read().unwrap().iter() {
            if *index == col_id {
                names.push(name.clone());
            }
        }
        names
    }

    /// Express OriginExpression in terms of BaseCidAndIndex (Convert DerivedColRef
    /// to BaseCidAndIndex)
    pub fn get_origin(&self, expr: &OriginExpression) -> OriginExpression {
        match expr {
            OriginExpression::BaseCidAndIndex { .. } => expr.clone(),
            OriginExpression::DerivedColRef { col_id } => {
                if let Some(derived_id) = self.id_to_origin.read().unwrap().get(col_id) {
                    self.get_origin(derived_id)
                } else {
                    // Not found in `id_to_origin`: Try to search in the `columns`
                    let names = self.get_names(*col_id);
                    for name in names {
                        // parse the name to get the table and column name
                        let parts: Vec<&str> = name.split('.').collect();
                        if parts.len() == 2 {
                            let table_name = parts[0];
                            let col_name = parts[1];
                            if let Some(cid) = self.catalog.get_table_id_if_exists(table_name) {
                                let table = self.catalog.get_table(cid).unwrap();
                                for (i, att) in table.schema.attributes().enumerate() {
                                    if att.name() == col_name {
                                        return OriginExpression::BaseCidAndIndex { cid, index: i };
                                    }
                                }
                            }
                        }
                    }
                    if let Some(outer) = &self.outer {
                        outer.get_origin(expr)
                    } else {
                        panic!("Column {} not found in environment {:?}", col_id, self);
                    }
                }
            }
            OriginExpression::Field { .. } => expr.clone(),
            OriginExpression::Binary { op, left, right } => OriginExpression::Binary {
                op: *op,
                left: Box::new(self.get_origin(left)),
                right: Box::new(self.get_origin(right)),
            },
            OriginExpression::Case {
                expr,
                whens,
                else_expr,
            } => OriginExpression::Case {
                expr: Box::new(self.get_origin(expr)),
                whens: whens
                    .iter()
                    .map(|(when, then)| (self.get_origin(when), self.get_origin(then)))
                    .collect(),
                else_expr: Box::new(self.get_origin(else_expr)),
            },
        }
    }
}

pub struct Translator {
    catalog_ref: CatalogRef,
    enabled_rules: RulesRef,
    col_id_gen: ColIdGeneratorRef,
    env: EnvironmentRef, // Variables in the current scope
}

#[derive(Debug, Clone)]
pub struct Query {
    env: EnvironmentRef,
    plan: LogicalRelExpr,
}

impl Query {
    pub fn get_plan(&self) -> &LogicalRelExpr {
        &self.plan
    }

    pub fn get_environment(&self) -> &EnvironmentRef {
        &self.env
    }
}

#[derive(Debug)]
pub enum TranslatorError {
    ColumnNotFound(String),
    TableNotFound(String),
    InvalidSQL(String),
    UnsupportedSQL(String),
}

impl std::fmt::Display for TranslatorError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            TranslatorError::ColumnNotFound(s) => write!(f, "Column not found: {}", s),
            TranslatorError::TableNotFound(s) => write!(f, "Table not found: {}", s),
            TranslatorError::InvalidSQL(s) => write!(f, "Invalid SQL: {}", s),
            TranslatorError::UnsupportedSQL(s) => write!(f, "Unsupported SQL: {}", s),
        }
    }
}

macro_rules! translation_err {
    (ColumnNotFound, $($arg:tt)*) => {
        TranslatorError::ColumnNotFound(format!($($arg)*))
    };
    (TableNotFound, $($arg:tt)*) => {
        TranslatorError::TableNotFound(format!($($arg)*))
    };
    (InvalidSQL, $($arg:tt)*) => {
        TranslatorError::InvalidSQL(format!($($arg)*))
    };
    (UnsupportedSQL, $($arg:tt)*) => {
        TranslatorError::UnsupportedSQL(format!($($arg)*))
    };
}

impl Translator {
    pub fn new(
        catalog: &CatalogRef,
        enabled_rules: &RulesRef,
        col_id_gen: &ColIdGeneratorRef,
    ) -> Translator {
        Translator {
            catalog_ref: catalog.clone(),
            enabled_rules: enabled_rules.clone(),
            col_id_gen: col_id_gen.clone(),
            env: Arc::new(Environment::new(catalog.clone())),
        }
    }

    fn new_with_outer(
        catalog: &CatalogRef,
        enabled_rules: &RulesRef,
        col_id_gen: &ColIdGeneratorRef,
        outer: &EnvironmentRef,
    ) -> Translator {
        Translator {
            col_id_gen: col_id_gen.clone(),
            enabled_rules: enabled_rules.clone(),
            catalog_ref: catalog.clone(),
            env: Arc::new(Environment::new_with_outer(outer.clone(), catalog.clone())),
        }
    }

    pub fn from_sql(
        sql: &sqlparser::ast::Query,
        catalog: &CatalogRef,
        enabled_rules: &RulesRef,
        col_id_gen: &ColIdGeneratorRef,
    ) -> Result<Query, TranslatorError> {
        let mut translator = Translator::new(catalog, enabled_rules, col_id_gen);
        translator.process_query(sql)
    }

    pub fn process_query(
        &mut self,
        query: &sqlparser::ast::Query,
    ) -> Result<Query, TranslatorError> {
        let select = match query.body.as_ref() {
            sqlparser::ast::SetExpr::Select(select) => select,
            _ => {
                return Err(translation_err!(
                    UnsupportedSQL,
                    "Only SELECT queries are supported"
                ))
            }
        };

        let plan = self.process_from(&select.from)?;
        let plan = self.process_where(plan, &select.selection)?;
        let plan = self.process_projection(
            plan,
            &select.projection,
            &select.from,
            &query.order_by,
            &query.limit,
            &select.group_by,
            &select.having,
            &select.distinct,
        )?;

        Ok(Query {
            env: self.env.clone(),
            plan,
        })
    }

    fn process_from(
        &mut self,
        from: &[sqlparser::ast::TableWithJoins],
    ) -> Result<LogicalRelExpr, TranslatorError> {
        if from.is_empty() {
            return Err(translation_err!(InvalidSQL, "FROM clause is empty"));
        }

        let mut join_exprs = Vec::with_capacity(from.len());
        for table_with_joins in from {
            let join_expr = self.process_table_with_joins(table_with_joins)?;
            join_exprs.push(join_expr);
        }
        let (mut plan, _) = join_exprs.remove(0);
        for (join_expr, is_subquery) in join_exprs.into_iter() {
            plan = if is_subquery {
                plan.flatmap(true, &self.enabled_rules, &self.col_id_gen, join_expr)
            } else {
                plan.join(
                    true,
                    &self.enabled_rules,
                    &self.col_id_gen,
                    JoinType::CrossJoin,
                    join_expr,
                    vec![],
                )
            }
        }
        Ok(plan)
    }

    fn process_table_with_joins(
        &mut self,
        table_with_joins: &sqlparser::ast::TableWithJoins,
    ) -> Result<(LogicalRelExpr, bool), TranslatorError> {
        let (mut plan, is_sbqry) = self.process_table_factor(&table_with_joins.relation)?;
        for join in &table_with_joins.joins {
            let (right, is_subquery) = self.process_table_factor(&join.relation)?;
            // If it is a subquery, we use flat_map + condition
            // Other wise we use a join
            let (join_type, condition) = self.process_join_operator(&join.join_operator)?;
            plan = if is_subquery {
                if matches!(
                    join_type,
                    JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter
                ) {
                    return Err(translation_err!(
                        UnsupportedSQL,
                        "Unsupported join type with subquery"
                    ));
                }
                plan.flatmap(true, &self.enabled_rules, &self.col_id_gen, right)
                    .select(
                        true,
                        &self.enabled_rules,
                        &self.col_id_gen,
                        condition.into_iter().collect(),
                    )
            } else {
                plan.join(
                    true,
                    &self.enabled_rules,
                    &self.col_id_gen,
                    join_type,
                    right,
                    condition.into_iter().collect(),
                )
            }
        }
        Ok((plan, is_sbqry))
    }

    fn process_join_operator(
        &self,
        join_operator: &sqlparser::ast::JoinOperator,
    ) -> Result<(JoinType, Option<Expression<LogicalRelExpr>>), TranslatorError> {
        use sqlparser::ast::{JoinConstraint, JoinOperator::*};
        match join_operator {
            Inner(JoinConstraint::On(cond)) => {
                Ok((JoinType::Inner, Some(self.process_expr(cond, None)?)))
            }
            LeftOuter(JoinConstraint::On(cond)) => {
                Ok((JoinType::LeftOuter, Some(self.process_expr(cond, None)?)))
            }
            RightOuter(JoinConstraint::On(cond)) => {
                Ok((JoinType::RightOuter, Some(self.process_expr(cond, None)?)))
            }
            FullOuter(JoinConstraint::On(cond)) => {
                Ok((JoinType::FullOuter, Some(self.process_expr(cond, None)?)))
            }
            CrossJoin => Ok((JoinType::CrossJoin, None)),
            _ => Err(translation_err!(
                UnsupportedSQL,
                "Unsupported join operator: {:?}",
                join_operator
            )),
        }
    }

    // Out: (RelExpr, is_subquery: bool)
    fn process_table_factor(
        &mut self,
        table_factor: &sqlparser::ast::TableFactor,
    ) -> Result<(LogicalRelExpr, bool), TranslatorError> {
        match table_factor {
            sqlparser::ast::TableFactor::Table { name, alias, .. } => {
                // Find the actual name from the catalog
                // If name exists in the catalog, then add the columns to the environment
                // Otherwise return an error
                let table_name = get_table_name(name);
                if let Some(cid) = self.catalog_ref.get_table_id_if_exists(&table_name) {
                    let cols = self.catalog_ref.get_cols(&table_name);
                    let plan = LogicalRelExpr::scan(
                        cid,
                        table_name.clone(),
                        cols.iter().map(|(_, id)| *id).collect(),
                    );
                    let att = plan.att();
                    let (plan, mut new_col_ids) =
                        plan.rename(&self.enabled_rules, &self.col_id_gen);

                    for i in att {
                        let new_col_id = new_col_ids.remove(&i).unwrap();
                        // get the name of the column
                        let col_name = cols.iter().find(|(_, id)| *id == i).cloned().unwrap().0;
                        self.env.set(&col_name, new_col_id);
                        self.env
                            .set(&format!("{}.{}", table_name, col_name), new_col_id);
                        self.env.add_to_origin_map(
                            new_col_id,
                            OriginExpression::BaseCidAndIndex {
                                cid,
                                index: get_column_index_from_temp_col_id(i), // Remember that `i` here is cid * MAX_COLUMNS + offset
                            },
                        );

                        // If there is an alias, set the alias in the current environment
                        if let Some(alias) = alias {
                            if is_valid_alias(&alias.name.value) {
                                self.env.set(&format!("{}.{}", alias, col_name), new_col_id);
                            } else {
                                return Err(translation_err!(
                                    InvalidSQL,
                                    "Invalid alias name: {}",
                                    alias.name.value
                                ));
                            }
                        }
                    }

                    Ok((plan, false))
                } else {
                    Err(translation_err!(TableNotFound, "{}", table_name))
                }
            }
            sqlparser::ast::TableFactor::Derived {
                subquery, alias, ..
            } => {
                let mut translator = Translator::new_with_outer(
                    &self.catalog_ref,
                    &self.enabled_rules,
                    &self.col_id_gen,
                    &self.env,
                );
                let subquery = translator.process_query(subquery)?;
                let plan = subquery.plan;
                let att = plan.att();

                for i in att {
                    // get the name of the column from env
                    let names = subquery.env.get_names(i);
                    for name in &names {
                        self.env.set(name, i);
                    }
                    // If there is an alias, set the alias in the current environment
                    if let Some(alias) = alias {
                        if is_valid_alias(&alias.name.value) {
                            for name in &names {
                                self.env.set(&format!("{}.{}", alias, name), i);
                            }
                        } else {
                            return Err(translation_err!(
                                InvalidSQL,
                                "Invalid alias name: {}",
                                alias.name.value
                            ));
                        }
                    }
                }
                Ok((plan, true))
            }
            _ => Err(translation_err!(UnsupportedSQL, "Unsupported table factor")),
        }
    }

    fn process_where(
        &mut self,
        plan: LogicalRelExpr,
        where_clause: &Option<sqlparser::ast::Expr>,
    ) -> Result<LogicalRelExpr, TranslatorError> {
        if let Some(expr) = where_clause {
            match self.process_expr(expr, Some(0)) {
                Ok(expr) => {
                    match expr {
                        Expression::Subquery { expr } => {
                            if expr.att().len() != 1 {
                                panic!("Subquery in WHERE clause returns more than one column")
                            }
                            // Add map first
                            let col_id = self.col_id_gen.next();
                            let subquery_expr = Expression::subquery(*expr);
                            let plan = plan.map(
                                true,
                                &self.enabled_rules,
                                &self.col_id_gen,
                                [(col_id, subquery_expr.clone())],
                            );
                            self.env.add_to_origin_map(col_id, subquery_expr.into());
                            // Add select
                            Ok(plan.select(
                                true,
                                &self.enabled_rules,
                                &self.col_id_gen,
                                vec![Expression::col_ref(col_id)],
                            ))
                        }
                        _ => Ok(plan.select(
                            true,
                            &self.enabled_rules,
                            &self.col_id_gen,
                            vec![expr],
                        )),
                    }
                }
                Err(TranslatorError::ColumnNotFound(_)) => {
                    // Search globally.
                    let expr = self.process_expr(expr, None)?;
                    let col_id = self.col_id_gen.next();
                    self.env.add_to_origin_map(col_id, expr.clone().into());
                    Ok(plan
                        .map(
                            true,
                            &self.enabled_rules,
                            &self.col_id_gen,
                            [(col_id, expr)],
                        )
                        .select(
                            true,
                            &self.enabled_rules,
                            &self.col_id_gen,
                            vec![Expression::col_ref(col_id)],
                        ))
                }
                Err(e) => Err(e),
            }
        } else {
            Ok(plan)
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn process_projection(
        &mut self,
        mut plan: LogicalRelExpr,
        projection: &Vec<sqlparser::ast::SelectItem>,
        _from: &[sqlparser::ast::TableWithJoins],
        _order_by: &[sqlparser::ast::OrderByExpr],
        _limit: &Option<sqlparser::ast::Expr>,
        group_by: &sqlparser::ast::GroupByExpr,
        having: &Option<sqlparser::ast::Expr>,
        _distinct: &Option<sqlparser::ast::Distinct>,
    ) -> Result<LogicalRelExpr, TranslatorError> {
        let mut projected_cols = Vec::new();
        let mut aggregations = Vec::new();
        let mut maps = Vec::new();
        let mut is_wildcard = false;
        for item in projection {
            match item {
                sqlparser::ast::SelectItem::Wildcard(_) => {
                    is_wildcard = true;
                    break;
                }
                sqlparser::ast::SelectItem::UnnamedExpr(expr) => {
                    if !has_agg(expr) {
                        match self.process_expr(expr, Some(0)) {
                            Ok(expr) => {
                                let col_id = if let Expression::ColRef { id } = expr {
                                    id
                                } else {
                                    // create a new col_id for the expression
                                    let col_id = self.col_id_gen.next();
                                    plan = plan.map(
                                        true,
                                        &self.enabled_rules,
                                        &self.col_id_gen,
                                        [(col_id, expr.clone())],
                                    );
                                    self.env.add_to_origin_map(col_id, expr.into());
                                    col_id
                                };
                                projected_cols.push(col_id);
                            }
                            Err(TranslatorError::ColumnNotFound(_)) => {
                                // Search globally.
                                let expr = self.process_expr(expr, None)?;
                                // Add a map to the plan
                                let col_id = self.col_id_gen.next();
                                self.env.add_to_origin_map(col_id, expr.clone().into());
                                plan = plan.map(
                                    true,
                                    &self.enabled_rules,
                                    &self.col_id_gen,
                                    [(col_id, expr)],
                                );
                                projected_cols.push(col_id);
                            }
                            Err(e) => return Err(e),
                        }
                    } else {
                        // The most complicated case will be:
                        // Agg(a + b) + Agg(c + d) + 4
                        // if we ignore nested aggregation.
                        //
                        // In this case,
                        // Level1: | map a + b to col_id1
                        //         | map c + d to col_id2
                        // Level2: |Agg(col_id1) to col_id3
                        //         |Agg(col_id2) to col_id4
                        // Level3: |map col_id3 + col_id4 + 4 to col_id5

                        let mut aggs = Vec::new();
                        let res = self.process_aggregation_arguments(plan, expr, &mut aggs);
                        plan = res.0;
                        let expr = res.1;
                        let col_id = if let Expression::ColRef { id } = expr {
                            id
                        } else {
                            // create a new col_id for the expression
                            let col_id = self.col_id_gen.next();
                            self.env.add_to_origin_map(col_id, expr.clone().into());
                            maps.push((col_id, expr));
                            col_id
                        };
                        aggregations.append(&mut aggs);
                        projected_cols.push(col_id);
                    }
                }
                sqlparser::ast::SelectItem::ExprWithAlias { expr, alias } => {
                    // create a new col_id for the expression
                    let col_id = if !has_agg(expr) {
                        let col_id = match self.process_expr(expr, Some(0)) {
                            Ok(expr) => {
                                if let Expression::ColRef { id } = expr {
                                    id
                                } else {
                                    let col_id = self.col_id_gen.next();
                                    self.env.add_to_origin_map(col_id, expr.clone().into());
                                    plan = plan.map(
                                        true,
                                        &self.enabled_rules,
                                        &self.col_id_gen,
                                        [(col_id, expr)],
                                    );
                                    col_id
                                }
                            }
                            Err(TranslatorError::ColumnNotFound(_)) => {
                                // Search globally.
                                let expr = self.process_expr(expr, None)?;
                                let col_id = self.col_id_gen.next();
                                self.env.add_to_origin_map(col_id, expr.clone().into());
                                plan = plan.map(
                                    true,
                                    &self.enabled_rules,
                                    &self.col_id_gen,
                                    [(col_id, expr)],
                                );
                                col_id
                            }
                            Err(e) => return Err(e),
                        };
                        projected_cols.push(col_id);
                        col_id
                    } else {
                        // The most complicated case will be:
                        // Agg(a + b) + Agg(c + d) + 4
                        // if we ignore nested aggregation.
                        //
                        // In this case,
                        // Level1: | map a + b to col_id1
                        //         | map c + d to col_id2
                        // Level2: |Agg(col_id1) to col_id3
                        //         |Agg(col_id2) to col_id4
                        // Level3: |map col_id3 + col_id4 + 4 to col_id5

                        let mut aggs = Vec::new();
                        let res = self.process_aggregation_arguments(plan, expr, &mut aggs);
                        plan = res.0;
                        let expr = res.1;
                        let col_id = if let Expression::ColRef { id } = expr {
                            id
                        } else {
                            // create a new col_id for the expression
                            let col_id = self.col_id_gen.next();
                            self.env.add_to_origin_map(col_id, expr.clone().into());
                            maps.push((col_id, expr));
                            col_id
                        };
                        aggregations.append(&mut aggs);
                        projected_cols.push(col_id);
                        col_id
                    };

                    // Add the alias to the aliases map
                    let alias_name = alias.value.clone();
                    if is_valid_alias(&alias_name) {
                        self.env.set(&alias_name, col_id);
                    } else {
                        return Err(translation_err!(
                            InvalidSQL,
                            "Invalid alias name: {}",
                            alias_name
                        ));
                    }
                }
                _ => {
                    return Err(translation_err!(
                        UnsupportedSQL,
                        "Unsupported select item: {:?}",
                        item
                    ))
                }
            }
        }

        if !aggregations.is_empty() {
            let group_by = match group_by {
                sqlparser::ast::GroupByExpr::All => Err(translation_err!(
                    UnsupportedSQL,
                    "GROUP BY ALL is not supported"
                ))?,
                sqlparser::ast::GroupByExpr::Expressions(exprs) => {
                    let mut group_by = Vec::new();
                    for expr in exprs {
                        let expr = self.process_expr(expr, None)?;
                        let col_id = if let Expression::ColRef { id } = expr {
                            id
                        } else {
                            // create a new col_id for the expression
                            let col_id = self.col_id_gen.next();
                            self.env.add_to_origin_map(col_id, expr.clone().into());
                            plan = plan.map(
                                true,
                                &self.enabled_rules,
                                &self.col_id_gen,
                                [(col_id, expr)],
                            );
                            col_id
                        };
                        group_by.push(col_id);
                    }
                    group_by
                }
            };
            plan = plan.aggregate(group_by, aggregations);
            plan = self.process_where(plan, having)?;
        }
        plan = plan.map(true, &self.enabled_rules, &self.col_id_gen, maps); // This map corresponds to the Level3 in the comment above
        plan = plan.project(
            true,
            &self.enabled_rules,
            &self.col_id_gen,
            projected_cols,
            is_wildcard,
        );
        Ok(plan)
    }

    // DFS until we find an aggregation function
    // If we find an aggregation function, then add the aggregation argument to the plan
    // and put the aggregation function in the aggregation list, return the modified plan with the expression.
    // For example, if SUM(a+b) + AVG(c+d) + 4, then
    // a+b -> col_id1, c+d -> col_id2 will be added to the plan
    // SUM(col_id1) -> col_id3, AVG(col_id2) -> col_id4 will be added to the aggregation list
    // col_id3 + col_id4 + 4 -> col_id5 will be returned with the plan
    fn process_aggregation_arguments(
        &self,
        mut plan: LogicalRelExpr,
        expr: &sqlparser::ast::Expr,
        aggs: &mut Vec<(usize, (usize, AggOp))>,
    ) -> (LogicalRelExpr, Expression<LogicalRelExpr>) {
        match expr {
            sqlparser::ast::Expr::Identifier(_) | sqlparser::ast::Expr::CompoundIdentifier(_) => {
                unreachable!(
                    "Identifier and compound identifier should be processed in the Function branch"
                )
            }
            sqlparser::ast::Expr::Value(_) | sqlparser::ast::Expr::TypedString { .. } => {
                let expr = self.process_expr(expr, Some(0)).unwrap();
                (plan, expr)
            }
            sqlparser::ast::Expr::BinaryOp { left, op, right } => {
                let (plan, left) = self.process_aggregation_arguments(plan, left, aggs);
                let (plan, right) = self.process_aggregation_arguments(plan, right, aggs);
                let bin_op = match op {
                    sqlparser::ast::BinaryOperator::And => BinaryOp::And,
                    sqlparser::ast::BinaryOperator::Or => BinaryOp::Or,
                    sqlparser::ast::BinaryOperator::Plus => BinaryOp::Add,
                    sqlparser::ast::BinaryOperator::Minus => BinaryOp::Sub,
                    sqlparser::ast::BinaryOperator::Multiply => BinaryOp::Mul,
                    sqlparser::ast::BinaryOperator::Divide => BinaryOp::Div,
                    sqlparser::ast::BinaryOperator::Eq => BinaryOp::Eq,
                    sqlparser::ast::BinaryOperator::NotEq => BinaryOp::Neq,
                    sqlparser::ast::BinaryOperator::Lt => BinaryOp::Lt,
                    sqlparser::ast::BinaryOperator::Gt => BinaryOp::Gt,
                    sqlparser::ast::BinaryOperator::LtEq => BinaryOp::Le,
                    sqlparser::ast::BinaryOperator::GtEq => BinaryOp::Ge,
                    _ => unimplemented!("Unsupported binary operator: {:?}", op),
                };
                (plan, Expression::binary(bin_op, left, right))
            }
            sqlparser::ast::Expr::Function(function) => {
                let name = get_table_name(&function.name).to_uppercase();
                let agg_op = match name.as_str() {
                    "COUNT" => AggOp::Count,
                    "SUM" => AggOp::Sum,
                    "AVG" => AggOp::Avg,
                    "MIN" => AggOp::Min,
                    "MAX" => AggOp::Max,
                    _ => unimplemented!("Unsupported aggregation function: {:?}", function),
                };
                if function.args.len() != 1 {
                    unimplemented!("Unsupported aggregation function: {:?}", function);
                }
                let function_arg_expr = match &function.args[0] {
                    sqlparser::ast::FunctionArg::Named { arg, .. } => arg,
                    sqlparser::ast::FunctionArg::Unnamed(arg) => arg,
                };

                let agg_col_id = self.col_id_gen.next();
                match function_arg_expr {
                    sqlparser::ast::FunctionArgExpr::Expr(expr) => {
                        match self.process_expr(expr, Some(0)) {
                            Ok(expr) => {
                                self.env.add_to_origin_map(agg_col_id, expr.clone().into());

                                if let Expression::ColRef { id } = expr {
                                    aggs.push((agg_col_id, (id, agg_op)));
                                    (plan, Expression::col_ref(agg_col_id))
                                } else {
                                    plan = plan.map(
                                        true,
                                        &self.enabled_rules,
                                        &self.col_id_gen,
                                        [(agg_col_id, expr)],
                                    );
                                    aggs.push((agg_col_id, (agg_col_id, agg_op)));
                                    (plan, Expression::col_ref(agg_col_id))
                                }
                            }
                            Err(TranslatorError::ColumnNotFound(_)) => {
                                // Search globally.
                                let expr = self.process_expr(expr, None).unwrap();
                                let col_id = self.col_id_gen.next();
                                self.env.add_to_origin_map(col_id, expr.clone().into());
                                plan = plan.map(
                                    true,
                                    &self.enabled_rules,
                                    &self.col_id_gen,
                                    [(col_id, expr)],
                                );
                                aggs.push((agg_col_id, (col_id, agg_op)));
                                (plan, Expression::col_ref(agg_col_id))
                            }
                            _ => unimplemented!("Unsupported expression: {:?}", expr),
                        }
                    }
                    sqlparser::ast::FunctionArgExpr::QualifiedWildcard(_) => {
                        unimplemented!("QualifiedWildcard is not supported yet")
                    }
                    sqlparser::ast::FunctionArgExpr::Wildcard => {
                        // Wildcard is only supported for COUNT
                        // If wildcard, just need to return Int(1) as it returns the count of rows
                        if matches!(agg_op, AggOp::Count) {
                            let col_id = self.col_id_gen.next();
                            let count_expr = Expression::int(1);
                            self.env
                                .add_to_origin_map(col_id, count_expr.clone().into());
                            plan = plan.map(
                                true,
                                &self.enabled_rules,
                                &self.col_id_gen,
                                [(col_id, count_expr)],
                            );
                            aggs.push((agg_col_id, (col_id, agg_op)));
                            (plan, Expression::col_ref(agg_col_id))
                        } else {
                            panic!("Wildcard is only supported for COUNT");
                        }
                    }
                }
            }
            sqlparser::ast::Expr::Nested(expr) => {
                self.process_aggregation_arguments(plan, expr, aggs)
            }
            _ => unimplemented!("Unsupported expression: {:?}", expr),
        }
    }

    fn process_expr(
        &self,
        expr: &sqlparser::ast::Expr,
        distance: Option<usize>,
    ) -> Result<Expression<LogicalRelExpr>, TranslatorError> {
        match expr {
            sqlparser::ast::Expr::Identifier(ident) => {
                let id = if let Some(distance) = distance {
                    self.env.get_at(distance, &ident.value)
                } else {
                    self.env.get(&ident.value)
                };
                let id = id.ok_or(translation_err!(
                    ColumnNotFound,
                    "{}, env: {}",
                    ident.value,
                    self.env
                        .columns
                        .read()
                        .unwrap()
                        .iter()
                        .map(|(k, v)| format!("{}:{}", k, v))
                        .collect::<Vec<_>>()
                        .join(", ")
                ))?;
                Ok(Expression::col_ref(id))
            }
            sqlparser::ast::Expr::CompoundIdentifier(idents) => {
                let name = idents
                    .iter()
                    .map(|i| i.value.clone())
                    .collect::<Vec<_>>()
                    .join(".");
                let id = if let Some(distance) = distance {
                    self.env.get_at(distance, &name)
                } else {
                    self.env.get(&name)
                };
                let id = id.ok_or(translation_err!(
                    ColumnNotFound,
                    "{}, env: {}",
                    name,
                    self.env
                        .columns
                        .read()
                        .unwrap()
                        .iter()
                        .map(|(k, v)| format!("{}:{}", k, v))
                        .collect::<Vec<_>>()
                        .join(", ")
                ))?;
                Ok(Expression::col_ref(id))
            }
            sqlparser::ast::Expr::BinaryOp { left, op, right } => {
                use sqlparser::ast::BinaryOperator::*;
                let left = self.process_expr(left, distance)?;
                let right = self.process_expr(right, distance)?;
                let bin_op = match op {
                    And => BinaryOp::And,
                    Or => BinaryOp::Or,
                    Plus => BinaryOp::Add,
                    Minus => BinaryOp::Sub,
                    Multiply => BinaryOp::Mul,
                    Divide => BinaryOp::Div,
                    Eq => BinaryOp::Eq,
                    NotEq => BinaryOp::Neq,
                    Lt => BinaryOp::Lt,
                    Gt => BinaryOp::Gt,
                    LtEq => BinaryOp::Le,
                    GtEq => BinaryOp::Ge,
                    _ => {
                        return Err(translation_err!(
                            UnsupportedSQL,
                            "Unsupported binary operator: {:?}",
                            op
                        ));
                    }
                };
                Ok(Expression::binary(bin_op, left, right))
            }
            sqlparser::ast::Expr::Value(value) => match value {
                sqlparser::ast::Value::Number(num, _) => Ok(Expression::int(num.parse().unwrap())),
                sqlparser::ast::Value::SingleQuotedString(s)
                | sqlparser::ast::Value::DoubleQuotedString(s) => Ok(Expression::Field {
                    val: Field::String(s.clone()),
                }),
                sqlparser::ast::Value::Boolean(b) => Ok(Expression::Field {
                    val: Field::Bool(*b),
                }),
                sqlparser::ast::Value::Null => Ok(Expression::Field { val: Field::Null }),
                _ => Err(translation_err!(
                    UnsupportedSQL,
                    "Unsupported value: {:?}",
                    value
                )),
            },
            sqlparser::ast::Expr::Exists { subquery, negated } => {
                let mut translator = Translator::new_with_outer(
                    &self.catalog_ref,
                    &self.enabled_rules,
                    &self.col_id_gen,
                    &self.env,
                );
                let subquery = translator.process_query(subquery)?;
                let mut plan = subquery.plan;
                // Add count(*) to the subquery
                let col_id1 = translator.col_id_gen.next();
                let col1_expr = Expression::int(1);
                self.env
                    .add_to_origin_map(col_id1, col1_expr.clone().into());
                plan = plan.map(
                    true,
                    &translator.enabled_rules,
                    &translator.col_id_gen,
                    [(col_id1, col1_expr)],
                );
                let col_id2 = translator.col_id_gen.next();
                // TODO: Did not add col_id2 to the origin map
                plan = plan.aggregate(vec![], vec![(col_id2, (col_id1, AggOp::Count))]);
                // Add count(*) > 0  to the subquery
                let exists_expr = if *negated {
                    Expression::binary(
                        BinaryOp::Le,
                        Expression::col_ref(col_id2),
                        Expression::int(0),
                    )
                } else {
                    Expression::binary(
                        BinaryOp::Gt,
                        Expression::col_ref(col_id2),
                        Expression::int(0),
                    )
                };
                let col_id3 = self.col_id_gen.next();
                self.env
                    .add_to_origin_map(col_id3, exists_expr.clone().into());
                plan = plan.map(
                    true,
                    &translator.enabled_rules,
                    &translator.col_id_gen,
                    [(col_id3, exists_expr)],
                );
                // Add project count(*) > 0 to the subquery
                plan = plan.project(
                    true,
                    &translator.enabled_rules,
                    &translator.col_id_gen,
                    [col_id3].into_iter().collect(),
                    false,
                );
                Ok(Expression::subquery(plan))
            }
            sqlparser::ast::Expr::TypedString { data_type, value } => {
                let dtype = get_attr(data_type).unwrap();
                let expr = match dtype {
                    DataType::BigInt | DataType::SmallInt | DataType::Int => Expression::int(
                        value
                            .parse()
                            .map_err(|e| translation_err!(InvalidSQL, "{}", e))?,
                    ),
                    DataType::Char(i) => Expression::Field {
                        val: Field::from_str_to_char(value, i)
                            .map_err(|e| translation_err!(InvalidSQL, "{}", e))?,
                    },
                    DataType::String => Expression::Field {
                        val: Field::from_str_to_string(value)
                            .map_err(|e| translation_err!(InvalidSQL, "{}", e))?,
                    },
                    DataType::Bool => Expression::Field {
                        val: Field::from_str_to_bool(value)
                            .map_err(|e| translation_err!(InvalidSQL, "{}", e))?,
                    },
                    DataType::Null => Expression::Field { val: Field::Null },
                    DataType::Date => Expression::Field {
                        val: Field::from_str_to_date(value)
                            .map_err(|e| translation_err!(InvalidSQL, "{}", e))?,
                    },
                    DataType::Decimal(precision, scale) => Expression::Field {
                        val: Field::from_str_to_decimal(value, precision, scale)
                            .map_err(|e| translation_err!(InvalidSQL, "{}", e))?,
                    },
                };
                Ok(expr)
            }
            sqlparser::ast::Expr::Nested(expr) => self.process_expr(expr, distance),
            _ => Err(translation_err!(
                UnsupportedSQL,
                "Unsupported expression: {:?}",
                expr
            )),
        }
    }
}

// Helper functions
fn get_table_name(name: &sqlparser::ast::ObjectName) -> String {
    name.0
        .iter()
        .map(|i| i.value.clone())
        .collect::<Vec<_>>()
        .join(".")
}

fn is_valid_alias(alias: &str) -> bool {
    alias.chars().all(|c| c.is_alphanumeric() || c == '_')
}

fn has_agg(expr: &sqlparser::ast::Expr) -> bool {
    use sqlparser::ast::Expr::*;
    match expr {
        Identifier(_) => false,
        CompoundIdentifier(_) => false,
        Value(_) => false,
        TypedString { .. } => false,

        BinaryOp { left, op: _, right } => has_agg(left) || has_agg(right),
        Function(function) => matches!(
            get_table_name(&function.name).to_uppercase().as_str(),
            "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"
        ),
        Nested(expr) => has_agg(expr),
        _ => unimplemented!("Unsupported expression: {:?}", expr),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common::{
        catalog::Catalog, physical::col_id_generator::ColIdGenerator, query::rules::Rules,
        table::TableInfo, DataType, TableSchema,
    };

    use super::Translator;

    fn get_test_catalog() -> Arc<Catalog> {
        let catalog = Catalog::new();
        let t1_table_name = String::from("t1");
        let t1_names = vec!["a", "b", "p", "q", "r"];
        let t1_dtypes = (0..5).map(|_| DataType::BigInt).collect::<Vec<_>>();
        let t1_schema = TableSchema::from_vecs(t1_names, t1_dtypes);
        let t1_cid = catalog.get_table_id(&t1_table_name);
        let t1_table = TableInfo::new(t1_cid, t1_table_name, t1_schema);
        catalog.add_table(t1_table);

        let t2_table_name = String::from("t2");
        let t2_names = vec!["c", "d"];
        let t2_dtypes = (0..2).map(|_| DataType::BigInt).collect::<Vec<_>>();
        let t2_schema = TableSchema::from_vecs(t2_names, t2_dtypes);
        let t2_cid = catalog.get_table_id(&t2_table_name);
        let t2_table = TableInfo::new(t2_cid, t2_table_name, t2_schema);
        catalog.add_table(t2_table);

        let t3_table_name = String::from("t3");
        let t3_names = vec!["e", "f"];
        let t3_dtypes = (0..2).map(|_| DataType::BigInt).collect::<Vec<_>>();
        let t3_schema = TableSchema::from_vecs(t3_names, t3_dtypes);
        let t3_cid = catalog.get_table_id(&t3_table_name);
        let t3_table = TableInfo::new(t3_cid, t3_table_name, t3_schema);
        catalog.add_table(t3_table);

        catalog
    }

    fn parse_sql(sql: &str) -> sqlparser::ast::Query {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let dialect = GenericDialect {};
        let statements = Parser::new(&dialect)
            .try_with_sql(sql)
            .unwrap()
            .parse_statements()
            .unwrap();
        let query = {
            let statement = statements.into_iter().next().unwrap();
            if let sqlparser::ast::Statement::Query(query) = statement {
                query
            } else {
                panic!("Expected a query");
            }
        };
        *query
    }

    fn get_translator() -> Translator {
        let catalog = get_test_catalog();
        let enabled_rules = Arc::new(Rules::default());
        // enabled_rules.disable(Rule::Decorrelate);
        // enabled_rules.disable(Rule::Hoist);
        // enabled_rules.disable(Rule::ProjectionPushdown);
        let col_id_gen = Arc::new(ColIdGenerator::new());
        Translator::new(&catalog, &enabled_rules, &col_id_gen)
    }

    fn get_plan(sql: &str) -> String {
        let query = parse_sql(sql);
        let mut translator = get_translator();
        let query = translator.process_query(&query).unwrap();
        query.plan.pretty_string()
    }

    #[test]
    fn parse_simple_select() {
        let sql = "SELECT a, b, p, q, r FROM t1";
        println!("{}", get_plan(sql));
    }

    #[test]
    fn parse_simple_select1() {
        let sql = "SELECT a FROM t1";
        println!("{}", get_plan(sql));
    }

    #[test]
    fn parse_simple_select_with_alias() {
        let sql = "SELECT a as x, b as y, p as z, q as w, r as v FROM t1";
        println!("{}", get_plan(sql));
    }

    #[test]
    fn parse_select_wildcard() {
        let sql = "SELECT * FROM t1";
        println!("{}", get_plan(sql));
    }

    #[test]
    fn parse_from_clause() {
        let sql = "SELECT a FROM t1 WHERE a = 1 AND b = 2";
        println!("{}", get_plan(sql));
    }

    #[test]
    fn parse_cross_join() {
        let sql = "SELECT * FROM t1, t2, t3 WHERE a = c AND b = d AND r = e";
        println!("{}", get_plan(sql));

        // Should become inner join
    }

    #[test]
    fn parse_cross_join2() {
        let sql = "SELECT * FROM t1, t2, t3 WHERE a = c AND b = d AND r = e AND a = 1 AND c = 2 AND e = 3";
        println!("{}", get_plan(sql));
    }

    #[test]
    fn parse_cross_join3() {
        let sql = "SELECT * FROM t1, t2 WHERE a = c AND b = 2";
        println!("{}", get_plan(sql));
    }

    #[test]
    #[should_panic]
    fn parse_from_with_subquery() {
        let sql = "SELECT a FROM (SELECT a FROM t1) WHERE a = 1 AND b = 2";
        println!("{}", get_plan(sql));
    }

    #[test]
    fn parse_from_with_subquery_2() {
        let sql = "SELECT a FROM (SELECT a, b FROM t1) AS t WHERE a = 1 AND b = 2";
        println!("{}", get_plan(sql));
    }

    #[test]
    #[should_panic]
    fn parse_from_with_subquery_and_alias() {
        let sql = "SELECT a FROM (SELECT a FROM t1) AS t WHERE a = 1 AND b = 2";
        println!("{}", get_plan(sql));
    }

    #[test]
    fn parse_from_with_join() {
        let sql = "SELECT a FROM t1 JOIN t2 ON t1.a = t2.c WHERE a = 1 AND b = 2";
        println!("{}", get_plan(sql));
    }

    #[test]
    fn parse_from_with_multiple_joins() {
        let sql =
            "SELECT a FROM t1 JOIN t2 ON t1.a = t2.c JOIN t3 ON t2.d = t3.e WHERE a = 1 AND b = 2";
        println!("{}", get_plan(sql));
    }

    #[test]
    #[should_panic]
    fn parse_from_with_subquery_joins() {
        let sql = "SELECT a FROM (SELECT a FROM t1) AS t1 JOIN (SELECT c FROM t2) AS t2 ON t1.a = t2.c WHERE a = 1 AND b = 2";
        println!("{}", get_plan(sql));
    }

    #[test]
    fn parse_from_with_subquery_joins_2() {
        let sql = "SELECT a FROM (SELECT a, b FROM t1) AS t1 JOIN (SELECT c, d FROM t2) AS t2 ON t1.a = t2.c WHERE a = 1 AND b = 2";
        println!("{}", get_plan(sql));
    }

    #[test]
    fn parse_where_clause() {
        let sql = "SELECT a FROM t1 WHERE a = 1 AND b = 2";
        println!("{}", get_plan(sql));
    }

    #[test]
    fn parse_subquery() {
        let sql = "SELECT a, x, y FROM t1, (SELECT COUNT(*) AS x, SUM(c) as y FROM t2 WHERE c = a)";
        println!("{}", get_plan(sql));
    }

    #[test]
    fn parse_subquery_2() {
        // This actually makes sense if we consider AVG(b) as AVG(0+b)
        let sql = "SELECT a, x, y FROM t1, (SELECT AVG(b) AS x, SUM(d) as y FROM t2 WHERE c = a)";
        println!("{}", get_plan(sql));
    }

    #[test]
    fn parse_subquery_3() {
        let sql = "SELECT a, k, x, y FROM t1, (SELECT b as k, c as x, d as y FROM t2 WHERE c = a)";
        println!("{}", get_plan(sql));
    }

    #[test]
    fn parse_subquery_with_alias() {
        let sql =
            "SELECT a, x, y FROM t1, (SELECT COUNT(a) AS x, SUM(b) as y FROM t2 WHERE c = a) AS t2";
        println!("{}", get_plan(sql));
    }

    #[test]
    fn parse_subquery_with_same_tables() {
        let sql = "SELECT x, y FROM (SELECT a as x FROM t1), (SELECT a as y FROM t1) WHERE x = y";
        println!("{}", get_plan(sql));
    }

    #[test]
    fn parse_joins_with_same_name() {
        let sql = "SELECT * FROM t1 as a, t1 as b";
        println!("{}", get_plan(sql));
    }

    #[test]
    fn parser_aggregate() {
        let sql = "SELECT COUNT(a), SUM(b) FROM t1";
        println!("{}", get_plan(sql));
    }

    // #[test]
    // fn parse_subquery_where() {
    //     let sql = "SELECT a FROM t1 WHERE exists (SELECT * FROM t2 WHERE c = a)";
    //     println!("{}", get_plan(sql));

    //     // left_outer_join not supported yet
    // }
}

// Subquery types
// 1. Select clause
//   a. Scalar subquery. A subquery that returns a single row.
//   b. EXISTS subquery. Subquery can return multiple rows.
//   c. ANY subquery. Subquery can return multiple rows.
// 2. From clause
//   a. Subquery can return multiple rows.
//
