// Reference: https://github.com/rotaki/decorrelator

mod aggregate;
mod flatmap;
mod hoist;
mod join;
mod logical_rel_expr;
mod map;
mod project;
mod rename;
mod scan;
mod select;

pub mod prelude {
    pub use super::logical_rel_expr::LogicalRelExpr;
    pub use crate::ids::ColumnId;
    pub use crate::query::expr::Expression;
    pub use crate::query::join_type::JoinType;
    pub use crate::query::operation::{AggOp, BinaryOp};
    pub use crate::traits::plan::Plan;
}
