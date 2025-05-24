// Reference: https://github.com/rotaki/decorrelator

use crate::ids::ContainerId;

use super::prelude::*;

impl LogicalRelExpr {
    /// Create a new scan node
    pub fn scan(
        cid: ContainerId,
        table_name: String,
        column_names: Vec<ColumnId>,
    ) -> LogicalRelExpr {
        LogicalRelExpr::Scan {
            cid,
            table_name,
            column_names,
        }
    }
}
