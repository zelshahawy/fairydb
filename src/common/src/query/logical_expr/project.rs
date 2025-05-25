// Reference: https://github.com/rotaki/decorrelator

use super::prelude::*;
use crate::physical::col_id_generator::ColIdGeneratorRef;
use crate::query::rules::{Rule, RulesRef};
use std::collections::{HashMap, HashSet};

/// Union of free variables and columns. The order of the columns is preserved.
fn union(free: &HashSet<usize>, cols: Vec<usize>) -> Vec<usize> {
    let mut to_add_to_new_cols = Vec::new();
    for f in free {
        let mut found = false;
        for c in cols.iter() {
            if f == c {
                found = true;
                break;
            }
        }
        if !found {
            to_add_to_new_cols.push(*f);
        }
    }
    [cols, to_add_to_new_cols].concat()
}

/// Intersection of free variables and columns. The order of the columns is preserved.
fn intersect(att: &HashSet<usize>, cols: &[usize]) -> Vec<usize> {
    let mut new_cols = Vec::new();
    for c in cols.iter() {
        if att.contains(c) {
            new_cols.push(*c);
        }
    }
    new_cols
}

impl LogicalRelExpr {
    /// Apply projection to the current logical relational expression.
    /// Notice the difference from rotaki/decorrelator. The `is_wildcard` parameter
    /// is added to the function signature. This parameter is used to determine if
    /// all columns should be projected.
    /// For correctness, a vector (instead of a hashset) is also used to represent
    /// the columns to be projected so that the order of the columns is preserved.
    pub fn project(
        self,
        optimize: bool,
        enabled_rules: &RulesRef,
        col_id_gen: &ColIdGeneratorRef,
        cols: Vec<usize>,
        is_wildcard: bool,
    ) -> LogicalRelExpr {
        if is_wildcard {
            return self;
        }

        let outer_refs = self.free();

        if optimize && enabled_rules.is_enabled(&Rule::ProjectionPushdown) {
            match self {
                LogicalRelExpr::Project {
                    src,
                    cols: _no_need_cols,
                } => src.project(true, enabled_rules, col_id_gen, cols, false),
                LogicalRelExpr::Map {
                    input,
                    exprs: mut existing_exprs,
                } => {
                    // Remove the mappings that are not used in the projection.
                    existing_exprs.retain(|(id, _)| cols.contains(id));

                    // Pushdown the projection to the source. Note that we don't push
                    // down the projection of outer columns.
                    let mut free: HashSet<usize> = existing_exprs
                        .iter()
                        .flat_map(|(_, expr)| expr.free())
                        .collect();
                    free = free.difference(&outer_refs).cloned().collect();

                    // From the cols, remove the cols that is created by the map expressions
                    // This is like `union`, but we need to keep the order of the columns
                    let mut new_cols = union(&free, cols.clone());

                    new_cols.retain(|col| !existing_exprs.iter().any(|(id, _)| *id == *col));

                    input
                        .project(true, enabled_rules, col_id_gen, new_cols, false)
                        .map(true, enabled_rules, col_id_gen, existing_exprs)
                        .project(false, enabled_rules, col_id_gen, cols, false)
                }
                LogicalRelExpr::Select { src, predicates } => {
                    // The necessary columns are the free variables of the predicates and the projection columns
                    let free: HashSet<usize> =
                        predicates.iter().flat_map(|pred| pred.free()).collect();
                    let new_cols = union(&free, cols.clone());
                    src.project(true, enabled_rules, col_id_gen, new_cols, false)
                        .select(true, enabled_rules, col_id_gen, predicates)
                        .project(false, enabled_rules, col_id_gen, cols, false)
                }
                LogicalRelExpr::Join {
                    join_type,
                    left,
                    right,
                    predicates,
                } => {
                    // The necessary columns are the free variables of the predicates and the projection columns
                    let free: HashSet<usize> =
                        predicates.iter().flat_map(|pred| pred.free()).collect();
                    let new_cols = union(&free, cols.clone());
                    let left_proj = intersect(&left.att(), &new_cols);
                    let right_proj = intersect(&right.att(), &new_cols);
                    left.project(true, enabled_rules, col_id_gen, left_proj, false)
                        .join(
                            true,
                            enabled_rules,
                            col_id_gen,
                            join_type,
                            right.project(true, enabled_rules, col_id_gen, right_proj, false),
                            predicates,
                        )
                        .project(false, enabled_rules, col_id_gen, cols, false)
                }
                LogicalRelExpr::Rename {
                    src,
                    src_to_dest: mut existing_rename,
                } => {
                    // Remove the mappings that are not used in the projection.
                    existing_rename.retain(|_, dest| cols.contains(dest));

                    // Pushdown the projection to the source. First we need to rewrite the column names
                    let existing_rename_rev: HashMap<usize, usize> = existing_rename
                        .iter()
                        .map(|(src, dest)| (*dest, *src))
                        .collect(); // dest -> src
                    let mut new_cols = Vec::new();
                    for col in cols.iter() {
                        new_cols.push(*existing_rename_rev.get(col).unwrap_or(col));
                    }

                    // Notice that we do not apply a projection node on the `rename` operator
                    // since it is not necessary. But can be added for clarity.
                    src.project(true, enabled_rules, col_id_gen, new_cols, false)
                        .rename_to(existing_rename)
                }
                LogicalRelExpr::Scan {
                    cid,
                    table_name,
                    mut column_names,
                } => {
                    column_names.retain(|col| cols.contains(col));
                    LogicalRelExpr::scan(cid, table_name, column_names)
                }
                _ => self.project(false, enabled_rules, col_id_gen, cols, false),
            }
        } else {
            LogicalRelExpr::Project {
                src: Box::new(self),
                cols,
            }
        }
    }
}
