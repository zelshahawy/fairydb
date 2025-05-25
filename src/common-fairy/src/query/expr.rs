/*
 * Reference: https://github.com/rotaki/decorrelator/blob/master/src/expressions/expressions.rs
 * https://github.com/justinj/null-bitmap-planner/tree/master/src
 * https://buttondown.email/jaffray/archive/the-little-planner-chapter-4-a-pushdown-party/
 * https://buttondown.email/jaffray/archive/a-very-basic-decorrelator/
 */

use std::collections::{HashMap, HashSet};

use crate::{
    attribute::Attribute, ids::ColumnId, table::TableSchema, traits::plan::Plan, BinaryOp,
    DataType, Field,
};

use super::{
    logical_expr::prelude::LogicalRelExpr, physical_expr::physical_rel_expr::PhysicalRelExpr,
};

#[derive(Debug, Clone)]
pub enum Expression<P: Plan> {
    ColRef {
        id: ColumnId,
    },
    Field {
        val: Field,
    },
    Binary {
        op: BinaryOp,
        left: Box<Expression<P>>,
        right: Box<Expression<P>>,
    },
    Case {
        expr: Box<Expression<P>>,
        whens: Vec<(Expression<P>, Expression<P>)>,
        else_expr: Box<Expression<P>>,
    },
    Subquery {
        expr: Box<P>,
    },
}

impl<P: Plan> Expression<P> {
    pub fn col_ref(id: ColumnId) -> Expression<P> {
        Expression::ColRef { id }
    }

    pub fn int(val: i64) -> Expression<P> {
        Expression::Field {
            val: Field::BigInt(val),
        }
    }

    pub fn binary(op: BinaryOp, left: Expression<P>, right: Expression<P>) -> Expression<P> {
        Expression::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    pub fn eq(self, other: Expression<P>) -> Expression<P> {
        Expression::Binary {
            op: BinaryOp::Eq,
            left: Box::new(self),
            right: Box::new(other),
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn add(self, other: Expression<P>) -> Expression<P> {
        Expression::Binary {
            op: BinaryOp::Add,
            left: Box::new(self),
            right: Box::new(other),
        }
    }

    pub fn subquery(expr: P) -> Expression<P> {
        Expression::Subquery {
            expr: Box::new(expr),
        }
    }

    pub fn has_subquery(&self) -> bool {
        match self {
            Expression::ColRef { id: _ } => false,
            Expression::Field { val: _ } => false,
            Expression::Binary { left, right, .. } => left.has_subquery() || right.has_subquery(),
            Expression::Case { .. } => {
                // Currently, we don't support subqueries in the case expression
                false
            }
            Expression::Subquery { expr: _ } => true,
        }
    }

    pub fn split_conjunction(self) -> Vec<Expression<P>> {
        match self {
            Expression::Binary {
                op: BinaryOp::And,
                left,
                right,
            } => {
                let mut left = left.split_conjunction();
                let mut right = right.split_conjunction();
                left.append(&mut right);
                left
            }
            _ => vec![self],
        }
    }

    // combine binary predicates into one
    pub fn combine_preds(v: &[Expression<P>]) -> Expression<P> {
        if v.len() == 1 {
            return v[0].clone();
        }
        if v.is_empty() {
            return Expression::binary(BinaryOp::Eq, Expression::int(1), Expression::int(1));
        }

        Expression::Binary {
            op: BinaryOp::And,
            left: Box::new(v[0].clone()),
            right: Box::new(Self::combine_preds(&v[1..])),
        }
    }

    /// Replace the variables in the expression with the new column IDs as specified in the
    /// `src_to_dest` mapping.
    pub fn replace_variables(self, src_to_dest: &HashMap<ColumnId, ColumnId>) -> Expression<P> {
        match self {
            Expression::ColRef { id } => {
                if let Some(dest) = src_to_dest.get(&id) {
                    Expression::ColRef { id: *dest }
                } else {
                    Expression::ColRef { id }
                }
            }
            Expression::Field { val } => Expression::Field { val },
            Expression::Binary { op, left, right } => Expression::Binary {
                op,
                left: Box::new(left.replace_variables(src_to_dest)),
                right: Box::new(right.replace_variables(src_to_dest)),
            },
            Expression::Case {
                expr,
                whens,
                else_expr,
            } => Expression::Case {
                expr: Box::new(expr.replace_variables(src_to_dest)),
                whens: whens
                    .into_iter()
                    .map(|(when, then)| {
                        (
                            when.replace_variables(src_to_dest),
                            then.replace_variables(src_to_dest),
                        )
                    })
                    .collect(),
                else_expr: Box::new(else_expr.replace_variables(src_to_dest)),
            },
            Expression::Subquery { expr } => Expression::Subquery {
                expr: Box::new(expr.replace_variables(src_to_dest)),
            },
        }
    }

    /// Replace the variables in the expression with the new expressions as specified in the
    /// `src_to_dest` mapping.
    pub(crate) fn replace_variables_with_exprs(
        self,
        src_to_dest: &HashMap<ColumnId, Expression<P>>,
    ) -> Expression<P> {
        match self {
            Expression::ColRef { id } => {
                if let Some(expr) = src_to_dest.get(&id) {
                    expr.clone()
                } else {
                    Expression::ColRef { id }
                }
            }
            Expression::Field { val } => Expression::Field { val },
            Expression::Binary { op, left, right } => Expression::Binary {
                op,
                left: Box::new(left.replace_variables_with_exprs(src_to_dest)),
                right: Box::new(right.replace_variables_with_exprs(src_to_dest)),
            },
            Expression::Case {
                expr,
                whens,
                else_expr,
            } => Expression::Case {
                expr: Box::new(expr.replace_variables_with_exprs(src_to_dest)),
                whens: whens
                    .into_iter()
                    .map(|(when, then)| {
                        (
                            when.replace_variables_with_exprs(src_to_dest),
                            then.replace_variables_with_exprs(src_to_dest),
                        )
                    })
                    .collect(),
                else_expr: Box::new(else_expr.replace_variables_with_exprs(src_to_dest)),
            },
            Expression::Subquery { expr } => Expression::Subquery {
                // Do nothing for subquery
                expr,
            },
        }
    }

    pub fn pretty_print(&self) {
        println!("{}", self.pretty_string());
    }

    pub fn pretty_string(&self) -> String {
        let mut out = String::new();
        self.print_inner(0, &mut out);
        out
    }

    pub fn print_inner(&self, indent: usize, out: &mut String) {
        match self {
            Expression::ColRef { id } => {
                out.push_str(&format!("@{}", id));
            }
            Expression::Field { val } => {
                out.push_str(&format!("{}", val));
            }
            Expression::Binary { op, left, right } => {
                left.print_inner(indent, out);
                out.push_str(&format!("{}", op));
                right.print_inner(indent, out);
            }
            Expression::Case {
                expr,
                whens,
                else_expr,
            } => {
                out.push_str("case ");
                expr.print_inner(indent, out);
                for (when, then) in whens {
                    out.push_str(" when ");
                    when.print_inner(indent, out);
                    out.push_str(" then ");
                    then.print_inner(indent, out);
                }
                out.push_str(" else ");
                else_expr.print_inner(indent, out);
                out.push_str(" end");
            }
            Expression::Subquery { expr } => {
                out.push_str(&format!("λ.{:?}(\n", expr.free()));
                expr.print_inner(indent + 6, out);
                out.push_str(&format!("{})", " ".repeat(indent + 4)));
            }
        }
    }

    /// Determine the attribute of the expression so that a new schema can be created
    /// PhysicalExpression may use the unique column ID (that is generated by ColIdGenerator)
    /// to refer to the columns in the schema. This function converts the unique column ID
    /// to the column offset in the schema by using the `col_id_to_offset` mapping.
    /// After it grabs the original attribute from the schema, it creates a new attribute
    /// depending on the expression.
    /// Note that this is only used when the expression is used in the physical plan.
    pub fn to_attr(
        &self,
        src_schema: &TableSchema,
        col_id_to_offset: &HashMap<ColumnId, ColumnId>,
    ) -> Attribute {
        match self {
            Self::ColRef { id } => {
                let offset = col_id_to_offset.get(id).unwrap();
                src_schema.get_attribute(*offset).unwrap().clone()
            }
            Self::Field { val } => Attribute::new(self.pretty_string(), val.into()),
            Self::Binary { op, left, right: _ } => {
                let left_attr = left.to_attr(src_schema, col_id_to_offset);
                match op {
                    BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div => left_attr,
                    BinaryOp::Eq
                    | BinaryOp::Neq
                    | BinaryOp::Lt
                    | BinaryOp::Le
                    | BinaryOp::Gt
                    | BinaryOp::Ge
                    | BinaryOp::And
                    | BinaryOp::Or => Attribute::new(self.pretty_string(), DataType::Bool),
                }
            }
            _ => unimplemented!(),
        }
    }
}

// Free variables
// * A column in an expression that is not bound.

// Bound variables
// * A column that gives its values within an expression and is not a
//   parameter that comes from some other context

// Example:
// function(x) {x + y}
// * x is a bound variable
// * y is a free variable

impl<P: Plan> Expression<P> {
    /// Get all variables in the expression.
    /// TODO: `free` might be a misleading name as in reality it returns all column
    /// IDs in the expression.
    pub fn free(&self) -> HashSet<ColumnId> {
        match self {
            Expression::ColRef { id } => {
                let mut set = HashSet::new();
                set.insert(*id);
                set
            }
            Expression::Field { val: _ } => HashSet::new(),
            Expression::Binary { left, right, .. } => {
                let mut set = left.free();
                set.extend(right.free());
                set
            }
            Expression::Case {
                expr,
                whens,
                else_expr,
            } => {
                let mut set = expr.free();
                for (when, then) in whens {
                    set.extend(when.free());
                    set.extend(then.free());
                }
                set.extend(else_expr.free());
                set
            }
            Expression::Subquery { expr } => expr.free(),
        }
    }

    /// Check if all variables in the expression are bound (i.e., its colums refer
    /// to the attributes of the plan node). In other words, check if there are
    /// no free variables in the expression.
    pub fn bound_by(&self, rel: &P) -> bool {
        self.free().is_subset(&rel.att())
    }

    /// Check if any of the variables in the expression come from the expression
    /// node `rel`.
    pub fn intersect_with(&self, rel: &P) -> bool {
        !self.free().is_disjoint(&rel.att())
    }
}

impl Expression<LogicalRelExpr> {
    /// Convert an Expression<LogicalRelExpr> to Expression<PhysicalRelExpr> for
    /// use in LogicalRelExpr to PhysicalRelExpr conversion.
    pub fn to_physical_expression(&self) -> Expression<PhysicalRelExpr> {
        match self {
            Self::ColRef { id } => Expression::col_ref(*id),
            Self::Field { val } => Expression::Field { val: val.clone() },
            Self::Binary { op, left, right } => Expression::binary(
                *op,
                left.to_physical_expression(),
                right.to_physical_expression(),
            ),
            Self::Case {
                expr,
                whens,
                else_expr,
            } => Expression::Case {
                expr: Box::new(expr.to_physical_expression()),
                whens: whens
                    .iter()
                    .map(|(e1, e2)| (e1.to_physical_expression(), e2.to_physical_expression()))
                    .collect(),
                else_expr: Box::new(else_expr.to_physical_expression()),
            },
            Self::Subquery { expr } => Expression::subquery(expr.to_physical_plan()),
        }
    }
}
