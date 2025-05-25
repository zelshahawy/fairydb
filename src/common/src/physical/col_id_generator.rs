use crate::ids::ColumnId;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

const STARTING_COL_ID: ColumnId = 10000;

/// Reference: https://github.com/rotaki/decorrelator/blob/master/src/col_id_generator.rs
/// https://buttondown.email/jaffray/archive/representing-columns-in-query-optimizers/
/// ColIdGenerator generates unique, temporary IDs to each column. Note that while
/// ColIds will be unique in different columns in different tables, uniqueness
/// is not guaranteed across different databases.
/// For example, we have a table named t1:
/// a (int) | b (int) | p (int) | q (int) | r (int)
/// Query:
/// SELECT a FROM t1 WHERE a = 1 AND b = 2
/// Logical Plan:
/// -> project(@10003)
///   -> select(@10003=1 && @10000=2)
///    -> rename(@10003 <- @0, @10000 <- @1)
///     -> scan("t1", [@0, @1])
/// ColIdGenerator will generate unique IDs for each column in the table. It assigns
/// IDs starting from 10000. The IDs are used to represent columns in the logical plan.
/// These IDs will be useful for self-joins, and other queries that contain temporary
/// columns (e.g., SELECT a+b FROM t1).
#[derive(Serialize, Deserialize)]
pub struct ColIdGenerator {
    current_id: AtomicUsize,
}

impl ColIdGenerator {
    pub fn new() -> ColIdGeneratorRef {
        Arc::new(ColIdGenerator {
            current_id: AtomicUsize::new(STARTING_COL_ID),
        })
    }

    pub fn next(&self) -> ColumnId {
        self.current_id.fetch_add(1, Ordering::AcqRel)
    }
}

pub type ColIdGeneratorRef = Arc<ColIdGenerator>;
