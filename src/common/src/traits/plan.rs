use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
};

use crate::ids::ColumnId;

// This `plan` is implemented by logical (LogicalRelExpr) and physical (PhysicalRelExpr) relational expressions.
pub trait Plan: Clone + Debug {
    /// Replace the variables in the current plan with the dest_ids in the `src_to_dest` map.
    fn replace_variables(self, src_to_dest: &HashMap<ColumnId, ColumnId>) -> Self;

    /// Print the current plan with the given indentation by modifying the `out` string.
    fn print_inner(&self, indent: usize, out: &mut String);

    /// Get the set of columns that the expression node currently has.
    /// For example, `scan` would return all the columns that it reads from the table.
    /// `project` would return the columns that it projects.
    fn att(&self) -> HashSet<ColumnId>;

    /// Get the set of columns that are not bound in the expression
    /// For example, `scan` would return an empty set as all the columns are bound.
    /// For `project`, it would return columns that are not bound by determining
    /// the set difference between 1) the bounded columns returned by the child
    /// and 2) the columns that are projected and the free columns from the child.
    fn free(&self) -> HashSet<ColumnId>;
}
