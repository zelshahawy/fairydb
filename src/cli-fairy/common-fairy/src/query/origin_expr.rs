use core::panic;

use crate::{
    ids::{ColumnId, ContainerId},
    logical_expr::prelude::{Expression, LogicalRelExpr},
    physical_expr::physical_rel_expr::PhysicalRelExpr,
    BinaryOp, Field,
};

/// Similar to Expression<P> and MemoExpression. Its purpose is to map the uniquely
/// generated column id to the original column index and container id. This will
/// make cardinality/ selectivity estimation easier. Each generated column ID will be mapped
/// to an OriginExpression (that will be stored in Environment).
#[derive(Debug, Clone)]
pub enum OriginExpression {
    BaseCidAndIndex {
        cid: ContainerId,
        index: usize,
    },

    /// Use this for derived columns, for example, @10002 <- @10000 + @10001
    /// To get base column id and index, you need to perform recursive lookup
    /// in the environment.
    DerivedColRef {
        col_id: ColumnId,
    },
    Field {
        val: Field,
    },
    Binary {
        op: BinaryOp,
        left: Box<Self>,
        right: Box<Self>,
    },
    Case {
        expr: Box<Self>,
        whens: Vec<(Self, Self)>,
        else_expr: Box<Self>,
    },
    // Does not support subquery for now
}

impl OriginExpression {
    pub fn get_base_ids_and_index(&self) -> Vec<(ContainerId, usize)> {
        match self {
            OriginExpression::BaseCidAndIndex { cid, index } => vec![(*cid, *index)],
            OriginExpression::Field { val: _ } => vec![],
            OriginExpression::Binary { left, right, .. } => {
                let mut res = left.get_base_ids_and_index();
                res.extend(right.get_base_ids_and_index());
                res
            }
            OriginExpression::Case {
                expr,
                whens,
                else_expr,
            } => {
                let mut res = expr.get_base_ids_and_index();
                for (when, then) in whens {
                    res.extend(when.get_base_ids_and_index());
                    res.extend(then.get_base_ids_and_index());
                }
                res.extend(else_expr.get_base_ids_and_index());
                res
            }
            // DerivedColRef should already been resolved to BaseCidAndIndex before
            // by calling the `get_origin` function in the environment.
            OriginExpression::DerivedColRef { .. } => {
                panic!("DerivedColRef should already been resolved to BaseCidAndIndex before calling this function. Call the `get_origin` function in the environment.")
            }
        }
    }
}

impl From<Expression<LogicalRelExpr>> for OriginExpression {
    fn from(expr: Expression<LogicalRelExpr>) -> Self {
        match expr {
            Expression::ColRef { id } => OriginExpression::DerivedColRef { col_id: id },
            Expression::Field { val } => OriginExpression::Field { val },
            Expression::Binary { op, left, right } => OriginExpression::Binary {
                op,
                left: Box::new((*left).into()),
                right: Box::new((*right).into()),
            },
            Expression::Case {
                expr,
                whens,
                else_expr,
            } => OriginExpression::Case {
                expr: Box::new((*expr).into()),
                whens: whens
                    .iter()
                    .map(|(when, then)| ((when.clone()).into(), (then.clone()).into()))
                    .collect(),
                else_expr: Box::new((*else_expr).into()),
            },
            Expression::Subquery { .. } => {
                unimplemented!("Subquery is currently not supported in OriginExpression")
            }
        }
    }
}

impl From<Expression<PhysicalRelExpr>> for OriginExpression {
    fn from(expr: Expression<PhysicalRelExpr>) -> Self {
        match expr {
            Expression::ColRef { id } => OriginExpression::DerivedColRef { col_id: id },
            Expression::Field { val } => OriginExpression::Field { val },
            Expression::Binary { op, left, right } => OriginExpression::Binary {
                op,
                left: Box::new((*left).into()),
                right: Box::new((*right).into()),
            },
            Expression::Case {
                expr,
                whens,
                else_expr,
            } => OriginExpression::Case {
                expr: Box::new((*expr).into()),
                whens: whens
                    .iter()
                    .map(|(when, then)| ((when.clone()).into(), (then.clone()).into()))
                    .collect(),
                else_expr: Box::new((*else_expr).into()),
            },
            Expression::Subquery { .. } => {
                unimplemented!("Subquery is currently not supported in OriginExpression")
            }
        }
    }
}

impl From<OriginExpression> for Expression<PhysicalRelExpr> {
    fn from(expr: OriginExpression) -> Self {
        match expr {
            OriginExpression::BaseCidAndIndex { cid: _, index } => Expression::ColRef { id: index },
            OriginExpression::Field { val } => Expression::Field { val },
            OriginExpression::Binary { op, left, right } => Expression::Binary {
                op,
                left: Box::new((*left).into()),
                right: Box::new((*right).into()),
            },
            OriginExpression::Case {
                expr,
                whens,
                else_expr,
            } => Expression::Case {
                expr: Box::new((*expr).into()),
                whens: whens
                    .iter()
                    .map(|(when, then)| ((when.clone()).into(), (then.clone()).into()))
                    .collect(),
                else_expr: Box::new((*else_expr).into()),
            },
            // DerivedColRef should already been resolved to BaseCidAndIndex before
            // calling this function. Call the `get_origin` function in the environment.
            _ => unimplemented!(),
        }
    }
}
