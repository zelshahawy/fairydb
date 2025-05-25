// Reference: https://github.com/rotaki/decorrelator

use super::prelude::*;

impl LogicalRelExpr {
    /// Apply aggregation to the current logical relational expression.
    /// aggrs: (dest_column_id, (src_column_id, agg_op))
    pub fn aggregate(
        self,
        group_by: Vec<ColumnId>,
        aggrs: Vec<(ColumnId, (ColumnId, AggOp))>,
    ) -> LogicalRelExpr {
        LogicalRelExpr::Aggregate {
            src: Box::new(self),
            group_by,
            aggrs,
        }
    }
}
