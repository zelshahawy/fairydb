/*
 * Reference: https://github.com/rotaki/decorrelator/blob/master/src/expressions/expressions.rs
 * https://github.com/justinj/null-bitmap-planner/tree/master/src
 * https://buttondown.email/jaffray/archive/the-little-planner-chapter-4-a-pushdown-party/
 * https://buttondown.email/jaffray/archive/a-very-basic-decorrelator/
 */

use std::{
    collections::{HashMap, HashSet, VecDeque},
    hash::{DefaultHasher, Hash, Hasher},
};

use crate::{
    error::c_err,
    ids::{ColumnId, ContainerId},
    logical_expr::prelude::{Expression, JoinType},
    traits::plan::Plan,
    AggOp, CrustyError,
};

#[derive(Debug, Clone)]
pub enum PhysicalRelExpr {
    Scan {
        cid: ContainerId,
        table_name: String,
        column_names: Vec<ColumnId>,
        tree_hash: Option<u64>, // Optional hash code for representing the plan
    },
    Select {
        // Evaluate the predicate for each row in the source
        src: Box<PhysicalRelExpr>,

        // TODO: Should be a single expression(?) because usually we need to break up conjunctive selection predicates
        // However, this might not be necessary as we have already pushed down the selection predicates
        // in the translation phase (generation of logical plan from ast)
        predicates: Vec<Expression<Self>>,
        tree_hash: Option<u64>, // Optional hash code for representing the plan
    },
    CrossJoin {
        join_type: JoinType,
        left: Box<PhysicalRelExpr>,
        right: Box<PhysicalRelExpr>,
        predicates: Vec<Expression<Self>>,
        tree_hash: Option<u64>, // Optional hash code for representing the plan
    },
    NestedLoopJoin {
        join_type: JoinType,
        left: Box<PhysicalRelExpr>,
        right: Box<PhysicalRelExpr>,
        predicates: Vec<Expression<Self>>,
        tree_hash: Option<u64>, // Optional hash code for representing the plan
    },
    HashJoin {
        join_type: JoinType,
        left: Box<PhysicalRelExpr>,
        right: Box<PhysicalRelExpr>,
        predicates: Vec<Expression<Self>>,
        tree_hash: Option<u64>, // Optional hash code for representing the plan
    },
    SortMergeJoin {
        join_type: JoinType,
        left: Box<PhysicalRelExpr>,
        right: Box<PhysicalRelExpr>,
        predicates: Vec<Expression<Self>>,
        tree_hash: Option<u64>, // Optional hash code for representing the plan
    },
    Project {
        // Reduces the number of columns in the result
        src: Box<PhysicalRelExpr>,
        cols: Vec<ColumnId>,
        tree_hash: Option<u64>, // Optional hash code for representing the plan
    },
    Sort {
        src: Box<PhysicalRelExpr>,
        cols: Vec<(ColumnId, bool, bool)>, // (column_id, asc, nulls_first)
        tree_hash: Option<u64>,            // Optional hash code for representing the plan
    },
    HashAggregate {
        src: Box<PhysicalRelExpr>,
        group_by: Vec<ColumnId>,
        aggrs: Vec<(ColumnId, (ColumnId, AggOp))>, // (dest_column_id, (src_column_id, agg_op)
        tree_hash: Option<u64>,                    // Optional hash code for representing the plan
    },
    Map {
        // Appends new columns to the result
        // This is the only operator that can have a reference to the columns of
        // the outer scope
        input: Box<PhysicalRelExpr>,
        exprs: Vec<(ColumnId, Expression<Self>)>,
        tree_hash: Option<u64>, // Optional hash code for representing the plan
    },
    FlatMap {
        // For each row in the input, call func and append the result to the output
        input: Box<PhysicalRelExpr>,
        func: Box<PhysicalRelExpr>,
        tree_hash: Option<u64>, // Optional hash code for representing the plan
    },
    Rename {
        src: Box<PhysicalRelExpr>,
        src_to_dest: HashMap<ColumnId, ColumnId>, // (src_column_id, dest_column_id)
        tree_hash: Option<u64>,                   // Optional hash code for representing the plan
    },
}

impl Plan for PhysicalRelExpr {
    /// Replace the column names in the relational expression
    /// * src_to_dest: mapping from source column id to the desired destination column id
    fn replace_variables(self, src_to_dest: &HashMap<ColumnId, ColumnId>) -> PhysicalRelExpr {
        match self {
            PhysicalRelExpr::Scan {
                cid,
                table_name,
                column_names,
                tree_hash,
            } => {
                let column_names = column_names
                    .into_iter()
                    .map(|col| *src_to_dest.get(&col).unwrap_or(&col))
                    .collect();
                PhysicalRelExpr::Scan {
                    cid,
                    table_name,
                    column_names,
                    tree_hash,
                }
            }
            PhysicalRelExpr::Select {
                src,
                predicates,
                tree_hash,
            } => PhysicalRelExpr::Select {
                src: Box::new(src.replace_variables(src_to_dest)),
                predicates: predicates
                    .into_iter()
                    .map(|pred| pred.replace_variables(src_to_dest))
                    .collect(),
                tree_hash,
            },
            PhysicalRelExpr::CrossJoin {
                join_type,
                left,
                right,
                predicates,
                tree_hash,
            } => PhysicalRelExpr::CrossJoin {
                join_type,
                left: Box::new(left.replace_variables(src_to_dest)),
                right: Box::new(right.replace_variables(src_to_dest)),
                predicates: predicates
                    .into_iter()
                    .map(|pred| pred.replace_variables(src_to_dest))
                    .collect(),
                tree_hash,
            },
            PhysicalRelExpr::NestedLoopJoin {
                join_type,
                left,
                right,
                predicates,
                tree_hash,
            } => PhysicalRelExpr::NestedLoopJoin {
                join_type,
                left: Box::new(left.replace_variables(src_to_dest)),
                right: Box::new(right.replace_variables(src_to_dest)),
                predicates: predicates
                    .into_iter()
                    .map(|pred| pred.replace_variables(src_to_dest))
                    .collect(),
                tree_hash,
            },
            PhysicalRelExpr::HashJoin {
                join_type,
                left,
                right,
                predicates,
                tree_hash,
            } => PhysicalRelExpr::HashJoin {
                join_type,
                left: Box::new(left.replace_variables(src_to_dest)),
                right: Box::new(right.replace_variables(src_to_dest)),
                predicates: predicates
                    .into_iter()
                    .map(|pred| pred.replace_variables(src_to_dest))
                    .collect(),
                tree_hash,
            },
            PhysicalRelExpr::SortMergeJoin {
                join_type,
                left,
                right,
                predicates,
                tree_hash,
            } => PhysicalRelExpr::SortMergeJoin {
                join_type,
                left: Box::new(left.replace_variables(src_to_dest)),
                right: Box::new(right.replace_variables(src_to_dest)),
                predicates: predicates
                    .into_iter()
                    .map(|pred| pred.replace_variables(src_to_dest))
                    .collect(),
                tree_hash,
            },
            PhysicalRelExpr::Project {
                src,
                cols,
                tree_hash,
            } => PhysicalRelExpr::Project {
                src: Box::new(src.replace_variables(src_to_dest)),
                cols: cols
                    .into_iter()
                    .map(|col| *src_to_dest.get(&col).unwrap_or(&col))
                    .collect(),
                tree_hash,
            },
            PhysicalRelExpr::Sort {
                src,
                cols,
                tree_hash,
            } => PhysicalRelExpr::Sort {
                src: Box::new(src.replace_variables(src_to_dest)),
                cols: cols
                    .into_iter()
                    .map(|(id, asc, nulls_first)| {
                        (*src_to_dest.get(&id).unwrap_or(&id), asc, nulls_first)
                    })
                    .collect(),
                tree_hash,
            },
            PhysicalRelExpr::HashAggregate {
                src,
                group_by,
                aggrs,
                tree_hash,
            } => PhysicalRelExpr::HashAggregate {
                src: Box::new(src.replace_variables(src_to_dest)),
                group_by: group_by
                    .into_iter()
                    .map(|id| *src_to_dest.get(&id).unwrap_or(&id))
                    .collect(),
                aggrs: aggrs
                    .into_iter()
                    .map(|(id, (src_id, op))| {
                        (
                            *src_to_dest.get(&id).unwrap_or(&id),
                            (*src_to_dest.get(&src_id).unwrap_or(&src_id), op),
                        )
                    })
                    .collect(),
                tree_hash,
            },
            PhysicalRelExpr::Map {
                input,
                exprs,
                tree_hash,
            } => PhysicalRelExpr::Map {
                input: Box::new(input.replace_variables(src_to_dest)),
                exprs: exprs
                    .into_iter()
                    .map(|(id, expr)| {
                        (
                            *src_to_dest.get(&id).unwrap_or(&id),
                            expr.replace_variables(src_to_dest),
                        )
                    })
                    .collect(),
                tree_hash,
            },
            PhysicalRelExpr::FlatMap {
                input,
                func,
                tree_hash,
            } => PhysicalRelExpr::FlatMap {
                input: Box::new(input.replace_variables(src_to_dest)),
                func: Box::new(func.replace_variables(src_to_dest)),
                tree_hash,
            },
            PhysicalRelExpr::Rename {
                src,
                src_to_dest: column_mappings,
                tree_hash,
            } => PhysicalRelExpr::Rename {
                src: Box::new(src.replace_variables(src_to_dest)),
                src_to_dest: column_mappings
                    .into_iter()
                    .map(|(src, dest)| {
                        (
                            *src_to_dest.get(&src).unwrap_or(&src),
                            *src_to_dest.get(&dest).unwrap_or(&dest),
                        )
                    })
                    .collect(),
                tree_hash,
            },
        }
    }

    fn print_inner(&self, indent: usize, out: &mut String) {
        match self {
            PhysicalRelExpr::Scan {
                cid: _,
                table_name,
                column_names,
                ..
            } => {
                out.push_str(&format!("{}-> scan({:?}, ", " ".repeat(indent), table_name,));
                let mut split = "";
                out.push('[');
                for col in column_names {
                    out.push_str(split);
                    out.push_str(&format!("@{}", col));
                    split = ", ";
                }
                out.push_str("])\n");
            }
            PhysicalRelExpr::Select {
                src, predicates, ..
            } => {
                out.push_str(&format!("{}-> select(", " ".repeat(indent)));
                let mut split = "";
                for pred in predicates {
                    out.push_str(split);
                    pred.print_inner(0, out);
                    split = " && ";
                }
                out.push_str(")\n");
                src.print_inner(indent + 2, out);
            }
            PhysicalRelExpr::CrossJoin {
                join_type,
                left,
                right,
                predicates,
                ..
            } => {
                out.push_str(&format!(
                    "{}-> Cross {}_join(",
                    " ".repeat(indent),
                    join_type
                ));
                let mut split = "";
                for pred in predicates {
                    out.push_str(split);
                    pred.print_inner(0, out);
                    split = " && ";
                }
                out.push_str(")\n");
                left.print_inner(indent + 2, out);
                right.print_inner(indent + 2, out);
            }
            PhysicalRelExpr::NestedLoopJoin {
                join_type,
                left,
                right,
                predicates,
                ..
            } => {
                out.push_str(&format!(
                    "{}-> Nested loop {}_join(",
                    " ".repeat(indent),
                    join_type
                ));
                let mut split = "";
                for pred in predicates {
                    out.push_str(split);
                    pred.print_inner(0, out);
                    split = " && ";
                }
                out.push_str(")\n");
                left.print_inner(indent + 2, out);
                right.print_inner(indent + 2, out);
            }
            PhysicalRelExpr::HashJoin {
                join_type,
                left,
                right,
                predicates,
                ..
            } => {
                out.push_str(&format!(
                    "{}-> Hash {}_join(",
                    " ".repeat(indent),
                    join_type
                ));
                let mut split = "";
                for pred in predicates {
                    out.push_str(split);
                    pred.print_inner(0, out);
                    split = " && ";
                }
                out.push_str(")\n");
                left.print_inner(indent + 2, out);
                right.print_inner(indent + 2, out);
            }
            PhysicalRelExpr::SortMergeJoin {
                join_type,
                left,
                right,
                predicates,
                ..
            } => {
                out.push_str(&format!(
                    "{}-> Sort merge {}_join(",
                    " ".repeat(indent),
                    join_type
                ));
                let mut split = "";
                for pred in predicates {
                    out.push_str(split);
                    pred.print_inner(0, out);
                    split = " && ";
                }
                out.push_str(")\n");
                left.print_inner(indent + 2, out);
                right.print_inner(indent + 2, out);
            }
            PhysicalRelExpr::Project { src, cols, .. } => {
                out.push_str(&format!("{}-> project(", " ".repeat(indent)));
                let mut split = "";
                for col in cols {
                    out.push_str(split);
                    out.push_str(&format!("@{}", col));
                    split = ", ";
                }
                out.push_str(")\n");
                src.print_inner(indent + 2, out);
            }
            PhysicalRelExpr::Sort { src, cols, .. } => {
                out.push_str(&format!("{}-> order_by({:?})\n", " ".repeat(indent), cols));
                src.print_inner(indent + 2, out);
            }
            PhysicalRelExpr::HashAggregate {
                src,
                group_by,
                aggrs,
                ..
            } => {
                out.push_str(&format!("{}-> aggregate(", " ".repeat(indent)));
                out.push_str("group_by: [");
                let mut split = "";
                for col in group_by {
                    out.push_str(split);
                    out.push_str(&format!("@{}", col));
                    split = ", ";
                }
                out.push_str("], ");
                out.push_str("aggrs: [");
                let mut split = "";
                for (id, (input_id, op)) in aggrs {
                    out.push_str(split);
                    out.push_str(&format!("@{} <- {:?}(@{})", id, op, input_id));
                    split = ", ";
                }
                out.push(']');
                out.push_str(")\n");
                src.print_inner(indent + 2, out);
            }
            PhysicalRelExpr::Map { input, exprs, .. } => {
                out.push_str(&format!("{}-> map(\n", " ".repeat(indent)));
                for (id, expr) in exprs {
                    out.push_str(&format!("{}    @{} <- ", " ".repeat(indent), id));
                    expr.print_inner(indent, out);
                    out.push_str(",\n");
                }
                out.push_str(&format!("{})\n", " ".repeat(indent + 2)));
                input.print_inner(indent + 2, out);
            }
            PhysicalRelExpr::FlatMap { input, func, .. } => {
                out.push_str(&format!("{}-> flatmap\n", " ".repeat(indent)));
                input.print_inner(indent + 2, out);
                out.push_str(&format!("{}  λ.{:?}\n", " ".repeat(indent), func.free()));
                func.print_inner(indent + 2, out);
            }
            PhysicalRelExpr::Rename {
                src,
                src_to_dest: colsk,
                ..
            } => {
                // Rename will be printed as @dest <- @src
                out.push_str(&format!("{}-> rename(", " ".repeat(indent)));
                let mut split = "";
                for (src, dest) in colsk {
                    out.push_str(split);
                    out.push_str(&format!("@{} <- @{}", dest, src));
                    split = ", ";
                }
                out.push_str(")\n");
                src.print_inner(indent + 2, out);
            }
        }
    }

    /// Free set of relational expression
    /// * The set of columns that are not bound in the expression
    /// * From all the columns required to compute the result, remove the columns that are
    ///   internally bound.
    ///
    /// * Examples of internally bound columns:
    ///   * The columns that are bound by the source of the expression (e.g. the columns of a table)
    ///   * The columns that are bound by the projection of the expression
    ///   * The columns that are bound by evaluating an expression
    fn free(&self) -> HashSet<ColumnId> {
        match self {
            PhysicalRelExpr::Scan { .. } => HashSet::new(),
            PhysicalRelExpr::Select {
                src, predicates, ..
            } => {
                // For each predicate, identify the free columns.
                // Take the set difference of the free columns and the src attribute set.
                let mut set = src.free();
                for pred in predicates {
                    set.extend(pred.free());
                }
                set.difference(&src.att()).cloned().collect()
            }
            PhysicalRelExpr::CrossJoin {
                left,
                right,
                predicates,
                ..
            }
            | PhysicalRelExpr::NestedLoopJoin {
                left,
                right,
                predicates,
                ..
            }
            | PhysicalRelExpr::HashJoin {
                left,
                right,
                predicates,
                ..
            }
            | PhysicalRelExpr::SortMergeJoin {
                left,
                right,
                predicates,
                ..
            } => {
                let mut set = left.free();
                set.extend(right.free());
                for pred in predicates {
                    set.extend(pred.free());
                }
                set.difference(&left.att().union(&right.att()).cloned().collect())
                    .cloned()
                    .collect()
            }
            PhysicalRelExpr::Project { src, cols, .. } => {
                let mut set = src.free();
                for col in cols {
                    set.insert(*col);
                }
                set.difference(&src.att()).cloned().collect()
            }
            PhysicalRelExpr::Sort { src, cols, .. } => {
                let mut set = src.free();
                for (id, _, _) in cols {
                    set.insert(*id);
                }
                set.difference(&src.att()).cloned().collect()
            }
            PhysicalRelExpr::HashAggregate {
                src,
                group_by,
                aggrs,
                ..
            } => {
                let mut set = src.free();
                for id in group_by {
                    set.insert(*id);
                }
                for (_, (src_id, _)) in aggrs {
                    set.insert(*src_id);
                }
                set.difference(&src.att()).cloned().collect()
            }
            PhysicalRelExpr::Map { input, exprs, .. } => {
                let mut set = input.free();
                for (_, expr) in exprs {
                    set.extend(expr.free());
                }
                set.difference(&input.att()).cloned().collect()
            }
            PhysicalRelExpr::FlatMap { input, func, .. } => {
                let mut set = input.free();
                set.extend(func.free());
                set.difference(&input.att()).cloned().collect()
            }
            PhysicalRelExpr::Rename { src, .. } => src.free(),
        }
    }

    /// Attribute set of relational expression
    /// * The set of columns that are in the result of the expression.
    /// * Attribute changes when we do a projection or map the columns to a different name.
    ///
    /// Difference between "free" and "att"
    /// * "free" is the set of columns that we need to evaluate the expression
    /// * "att" is the set of columns that we have (the column names of the result of RelExpr)
    fn att(&self) -> HashSet<ColumnId> {
        match self {
            PhysicalRelExpr::Scan {
                cid: _,
                table_name: _,
                column_names,
                tree_hash: _,
            } => column_names.iter().cloned().collect(),
            PhysicalRelExpr::Select { src, .. } => src.att(),
            PhysicalRelExpr::CrossJoin { left, right, .. }
            | PhysicalRelExpr::NestedLoopJoin { left, right, .. }
            | PhysicalRelExpr::HashJoin { left, right, .. }
            | PhysicalRelExpr::SortMergeJoin { left, right, .. } => {
                let mut set = left.att();
                set.extend(right.att());
                set
            }
            PhysicalRelExpr::Project { cols, .. } => cols.iter().cloned().collect(),
            PhysicalRelExpr::Sort { src, .. } => src.att(),
            PhysicalRelExpr::HashAggregate {
                group_by, aggrs, ..
            } => {
                let mut set: HashSet<usize> = group_by.iter().cloned().collect();
                set.extend(aggrs.iter().map(|(id, _)| *id));
                set
            }
            PhysicalRelExpr::Map { input, exprs, .. } => {
                let mut set = input.att();
                set.extend(exprs.iter().map(|(id, _)| *id));
                set
            }
            PhysicalRelExpr::FlatMap { input, func, .. } => {
                let mut set = input.att();
                set.extend(func.att());
                set
            }
            PhysicalRelExpr::Rename {
                src, src_to_dest, ..
            } => {
                let mut set = src.att();
                // rewrite the column names
                for (src, dest) in src_to_dest {
                    set.remove(src);
                    set.insert(*dest);
                }
                set
            }
        }
    }
}

// TODO: move
/// gets unique hash from string
fn compute_hash(data: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    data.hash(&mut hasher);
    hasher.finish()
}

impl PhysicalRelExpr {
    pub fn pretty_print(&self) {
        println!("{}", self.pretty_string());
    }

    pub fn pretty_string(&self) -> String {
        let mut out = String::new();
        self.print_inner(0, &mut out);
        out
    }

    /// Get all tables involved in expression
    pub fn get_tables_involved(&self, container_ids: &mut Vec<ContainerId>) {
        if let PhysicalRelExpr::Scan { cid, .. } = self {
            container_ids.push(*cid);
        }

        if let PhysicalRelExpr::Select { src, .. }
        | PhysicalRelExpr::Project { src, .. }
        | PhysicalRelExpr::Sort { src, .. }
        | PhysicalRelExpr::HashAggregate { src, .. }
        | PhysicalRelExpr::Map { input: src, .. }
        | PhysicalRelExpr::FlatMap { input: src, .. }
        | PhysicalRelExpr::Rename { src, .. } = self
        {
            src.get_tables_involved(container_ids);
        }

        if let PhysicalRelExpr::CrossJoin { left, right, .. }
        | PhysicalRelExpr::NestedLoopJoin { left, right, .. }
        | PhysicalRelExpr::HashJoin { left, right, .. }
        | PhysicalRelExpr::SortMergeJoin { left, right, .. } = self
        {
            left.get_tables_involved(container_ids);
            right.get_tables_involved(container_ids);
        }

        if let PhysicalRelExpr::FlatMap { func, .. } = self {
            func.get_tables_involved(container_ids);
        }
    }

    fn set_tree_hash(&mut self, hash_val: u64) -> Result<(), CrustyError> {
        match self {
            PhysicalRelExpr::Scan { tree_hash, .. }
            | PhysicalRelExpr::Select { tree_hash, .. }
            | PhysicalRelExpr::CrossJoin { tree_hash, .. }
            | PhysicalRelExpr::NestedLoopJoin { tree_hash, .. }
            | PhysicalRelExpr::HashJoin { tree_hash, .. }
            | PhysicalRelExpr::SortMergeJoin { tree_hash, .. }
            | PhysicalRelExpr::Project { tree_hash, .. }
            | PhysicalRelExpr::Sort { tree_hash, .. }
            | PhysicalRelExpr::HashAggregate { tree_hash, .. }
            | PhysicalRelExpr::Map { tree_hash, .. }
            | PhysicalRelExpr::FlatMap { tree_hash, .. }
            | PhysicalRelExpr::Rename { tree_hash, .. } => {
                *tree_hash = Some(hash_val);
                Ok(())
            } // Cannot reach this all are covered currently
              // _ => Err(c_err("set_hash not implemented for expr enum type")),
        }
    }

    pub fn get_tree_hash(&self) -> Result<u64, CrustyError> {
        match self {
            PhysicalRelExpr::Scan { tree_hash, .. }
            | PhysicalRelExpr::Select { tree_hash, .. }
            | PhysicalRelExpr::CrossJoin { tree_hash, .. }
            | PhysicalRelExpr::NestedLoopJoin { tree_hash, .. }
            | PhysicalRelExpr::HashJoin { tree_hash, .. }
            | PhysicalRelExpr::SortMergeJoin { tree_hash, .. }
            | PhysicalRelExpr::Project { tree_hash, .. }
            | PhysicalRelExpr::Sort { tree_hash, .. }
            | PhysicalRelExpr::HashAggregate { tree_hash, .. }
            | PhysicalRelExpr::Map { tree_hash, .. }
            | PhysicalRelExpr::FlatMap { tree_hash, .. }
            | PhysicalRelExpr::Rename { tree_hash, .. } => {
                tree_hash.ok_or_else(|| c_err("tree_hash not set"))
            } // Commenting as all are covered currently
              // _ => Err(c_err("set_hash not implemented for expr enum type")),
        }
    }

    /// Gets the hash of physical plan. This is used to match subplans. The hash value
    /// acts as a unique finger print for our tree and maintains join commutativity
    /// and is column-rename-agnostic, among other things.
    ///
    /// This should only ever be called once. Will throw an error if any already hashed
    /// node is getting hashed.
    pub fn hash_plan(&mut self) -> Result<u64, CrustyError> {
        self.hash_node(None)
    }

    /// Get hash on specific node of expression tree. General idea is that we recursively get
    /// root hashes for children to build up a unique tree identifier, and make sure that in
    /// cases with multiple children, there is a canonical ordering (which differs from merkle
    /// tree hashes).
    ///
    /// We also pass up a column id renaming map to normalize hashes that refer to renamed
    /// columns. The map is updated accordingly (to map col_ids unique to queries to common
    /// ones) as we recurse. When calling this function from the outside, rename_map should
    /// be None.
    fn hash_node(
        &mut self,
        rename_map: Option<&mut HashMap<ColumnId, ColumnId>>,
    ) -> Result<u64, CrustyError> {
        // root_map declared to avoid going out of scope
        let mut root_map = HashMap::new();
        // rename map will be null upon intial call to function so we unwrap into an empty map if that was the case
        let rename_map = rename_map.unwrap_or(&mut root_map);

        match self {
            PhysicalRelExpr::Scan {
                cid,
                table_name,
                column_names,
                ..
            } => {
                // rename col_id aliases and sort for consistency across queries
                let mut renamed_column_names: Vec<ColumnId> = column_names
                    .iter()
                    .map(|col_id| *rename_map.get(col_id).unwrap_or(col_id))
                    .collect();
                renamed_column_names.sort();
                let identifier = format!("{}{}{:?}", cid, table_name, renamed_column_names);
                // set and pass up subtree's hash value
                let res = compute_hash(&identifier);
                self.set_tree_hash(res)?;
                Ok(res)
            }
            PhysicalRelExpr::Select {
                src, predicates, ..
            } => {
                let src_hash = src.hash_node(Some(rename_map))?;
                // use rename_map to rephrase predicate vector
                let mut renamed_predicates: Vec<Expression<PhysicalRelExpr>> = predicates
                    .iter()
                    .map(|expr| expr.clone().replace_variables(rename_map))
                    .collect();
                // define canonical predicate ordering for hash
                renamed_predicates.sort_by_key(|a| a.pretty_string());
                let predicates_hash = compute_hash(&format!("{:?}", renamed_predicates));
                // set and pass up subtree's hash value
                let res = src_hash ^ predicates_hash;
                self.set_tree_hash(res)?;
                Ok(res)
            }
            PhysicalRelExpr::CrossJoin {
                join_type,
                left,
                right,
                predicates,
                ..
            }
            | PhysicalRelExpr::NestedLoopJoin {
                join_type,
                left,
                right,
                predicates,
                ..
            }
            | PhysicalRelExpr::HashJoin {
                join_type,
                left,
                right,
                predicates,
                ..
            }
            | PhysicalRelExpr::SortMergeJoin {
                join_type,
                left,
                right,
                predicates,
                ..
            } => {
                let left_hash = left.hash_node(Some(rename_map))?;
                let right_hash = right.hash_node(Some(rename_map))?;

                // NOT USED ANYMORE BECAUSE XOR IS COMMUTATIVE - might use for tree mathcing later so keeping here
                // // sort hashes to have commutativity in identifier
                // let (hash1, hash2) = if left_hash <= right_hash {
                //     (left_hash, right_hash)
                // } else {
                //     (right_hash, left_hash)
                // };

                // use rename_map to rephrase predicate vector
                let mut renamed_predicates: Vec<Expression<PhysicalRelExpr>> = predicates
                    .iter()
                    .map(|expr| expr.clone().replace_variables(rename_map))
                    .collect();
                // define canonical predicate ordering for hash
                renamed_predicates.sort_by_key(|a| a.pretty_string());
                let predicates_hash = compute_hash(&format!("{:?}", renamed_predicates));
                let join_type_hash = compute_hash(&format!("{}", join_type));
                // set and pass up subtree's hash value
                let res = predicates_hash ^ join_type_hash ^ left_hash ^ right_hash;
                self.set_tree_hash(res)?;
                Ok(res)
            }
            PhysicalRelExpr::Project { src, cols, .. } => {
                let src_hash = src.hash_node(Some(rename_map))?;
                // rename col_id aliases and sort for consistency across queries
                let mut renamed_cols: Vec<ColumnId> = cols
                    .iter()
                    .map(|col_id| *rename_map.get(col_id).unwrap_or(col_id))
                    .collect();
                renamed_cols.sort();
                let cols_hash = compute_hash(&format!("{:?}", renamed_cols));
                // set and pass up subtree's hash value
                let res = src_hash ^ cols_hash;
                self.set_tree_hash(res)?;
                Ok(res)
            }
            PhysicalRelExpr::Sort { src, cols, .. } => {
                let src_hash = src.hash_node(Some(rename_map))?;
                // we don't sort cols because vec order defines col priorities for sorting
                let cols_hash = compute_hash(&format!("{:?}", cols));
                // set and pass up subtree's hash value
                let res = src_hash ^ cols_hash;
                self.set_tree_hash(res)?;
                Ok(res)
            }
            PhysicalRelExpr::Rename {
                src, src_to_dest, ..
            } => {
                let src_hash = src.hash_node(Some(rename_map))?;
                // update map before we go back up the stack
                for (src, dest) in src_to_dest {
                    // reverse map order because we care about reverse mapping for hash use in ancestor nodes
                    rename_map.insert(*dest, *src);
                }
                // we want to make the rename node "invisible" in our hash so we ignore its non-src fields
                // set and pass up subtree's hash value
                self.set_tree_hash(src_hash)?;
                Ok(src_hash)
            }
            PhysicalRelExpr::HashAggregate {
                src,
                group_by,
                aggrs,
                ..
            } => {
                let src_hash = src.hash_node(Some(rename_map))?;
                group_by.sort(); // order doesn't matter for group_by
                let gb_hash = compute_hash(&format!("{:?}", group_by));
                let aggr_hash = compute_hash(&format!("{:?}", aggrs));
                let res = src_hash ^ gb_hash ^ aggr_hash;
                self.set_tree_hash(res)?;
                Ok(res)
            }
            PhysicalRelExpr::Map { input, exprs, .. } => {
                let input_hash = input.hash_node(Some(rename_map))?;
                let renamed_expr: Vec<(usize, Expression<PhysicalRelExpr>)> = exprs
                    .iter()
                    .map(|(n, expr)| (*n, expr.clone().replace_variables(rename_map)))
                    .collect();
                let expr_hash = compute_hash(&format!("{:?}", renamed_expr));

                let res = input_hash ^ expr_hash;
                self.set_tree_hash(res)?;
                Ok(res)
            }
            PhysicalRelExpr::FlatMap { input, func, .. } => {
                let input_hash = input.hash_node(Some(rename_map))?;
                let func_hash = func.hash_node(Some(rename_map))?;
                let res = input_hash ^ func_hash;
                self.set_tree_hash(res)?;
                Ok(res)
            } // Commenting as all are covered
              // _ => Err(c_err(
              //     "tree contains operators for which hash isn't implemented",
              // )),
        }
    }

    /// Returns level-ordered representation of a plan tree's node hashes. Will hash the
    /// tree of that hasn't been done.
    pub fn get_hash_vec(&self) -> Result<Vec<(u64, &PhysicalRelExpr)>, CrustyError> {
        // bfs traverse and populate
        let mut hashes = Vec::new();
        let mut queue = VecDeque::new();
        queue.push_back(self);
        while !queue.is_empty() {
            let node = queue.pop_front().unwrap();
            // check tree + children root hashes to find match
            match node {
                PhysicalRelExpr::Scan { tree_hash, .. } => {
                    hashes.push((tree_hash.unwrap(), node));
                }
                PhysicalRelExpr::Select { src, tree_hash, .. }
                | PhysicalRelExpr::Project { src, tree_hash, .. }
                | PhysicalRelExpr::Sort { src, tree_hash, .. }
                | PhysicalRelExpr::Rename { src, tree_hash, .. }
                | PhysicalRelExpr::HashAggregate { src, tree_hash, .. } => {
                    hashes.push((tree_hash.unwrap(), node));
                    // add next level to back of queue
                    queue.push_back(src);
                }
                PhysicalRelExpr::CrossJoin {
                    left,
                    right,
                    tree_hash,
                    ..
                }
                | PhysicalRelExpr::NestedLoopJoin {
                    left,
                    right,
                    tree_hash,
                    ..
                }
                | PhysicalRelExpr::HashJoin {
                    left,
                    right,
                    tree_hash,
                    ..
                }
                | PhysicalRelExpr::SortMergeJoin {
                    left,
                    right,
                    tree_hash,
                    ..
                } => {
                    hashes.push((tree_hash.unwrap(), node));
                    // add next level to back of queue
                    queue.push_back(left);
                    queue.push_back(right);
                }
                _ => {
                    return Err(c_err(&format!(
                        "haven't implemented hash match for specific PhysicalRelExpr type: {:?}",
                        node
                    )));
                }
            }
        }
        Ok(hashes)
    }

    #[allow(dead_code)]
    /// Looks for the given hash value within tree. We try to return the topmost we ignore renames
    /// and flag with the node right below (because that is likely what is stored) since
    /// rename(X).tree_hash = X.tree_hash.
    fn find_hash_in_tree(&self, hash_val: u64) -> Result<Option<&PhysicalRelExpr>, CrustyError> {
        // bfs through plan and stop when the first non-rename match is found
        let mut queue = VecDeque::new();
        queue.push_back(self);

        while !queue.is_empty() {
            let node = queue.pop_front().unwrap();
            // check tree + children root hashes to find match
            match node {
                PhysicalRelExpr::Scan { tree_hash, .. } => {
                    if tree_hash.unwrap() == hash_val {
                        return Ok(Some(node));
                    }
                    return Ok(None);
                }
                PhysicalRelExpr::Select { src, tree_hash, .. }
                | PhysicalRelExpr::Project { src, tree_hash, .. }
                | PhysicalRelExpr::Sort { src, tree_hash, .. }
                | PhysicalRelExpr::Rename { src, tree_hash, .. } => {
                    if tree_hash.unwrap() == hash_val {
                        return Ok(Some(node));
                    }
                    queue.push_back(src);
                }
                PhysicalRelExpr::CrossJoin {
                    left,
                    right,
                    tree_hash,
                    ..
                }
                | PhysicalRelExpr::NestedLoopJoin {
                    left,
                    right,
                    tree_hash,
                    ..
                }
                | PhysicalRelExpr::HashJoin {
                    left,
                    right,
                    tree_hash,
                    ..
                }
                | PhysicalRelExpr::SortMergeJoin {
                    left,
                    right,
                    tree_hash,
                    ..
                } => {
                    if tree_hash.unwrap() == hash_val {
                        return Ok(Some(node));
                    }
                    // add next level to back of queue
                    queue.push_back(left);
                    queue.push_back(right);
                }
                // PhysicalRelExpr::Rename { src, .. } => {
                //     // tree_hash doesn't matter here because we never want to flag a match when root is a rename node
                //     queue.push_back(src);
                // }
                _ => {
                    return Err(c_err(&format!(
                        "haven't implemented hash match for specific PhysicalRelExpr type: {:?}",
                        node
                    )));
                }
            }
        }
        Ok(None)
    }

    /// Indentifies overlapping elements in current plan with a list of other plans. Will give precedence to
    /// self when identifying subplans (i.e. we iterate through branches of self and compare those hashes to other's hashes)
    ///
    /// We pass a map of other candidates with their BFS-order hashes so that we can compare all candidates to each
    /// node of the og plan as we iterate.
    ///
    /// The return value is a vector with elements of form (a: PhysicalRelExpr, b: PhysicalRelExpr, c: PhysicalRelExpr) where
    /// `b` is the part of the original plan which matches with `c`, the part of the cached plan, `a`, that matches with `b`.
    ///
    /// In short, we return [ ... (CACHED_PLAN, OG_PLAN'S_SUBSET, CACHED_PLAN'S_MATCHING_SUBSET) ... ] to the optimizer for replacement.
    ///
    /// The end result here should match the general highest level subtrees of the og plan and the cached plans, giving preference
    /// to the og plan when prioritizing high matches. NOTE: these are potential because of (highly unlikely) hash collisions.
    pub fn identify_potential_tree_overlaps<'a>(
        &'a self,
        cached_plans_with_hashes: Vec<(&'a PhysicalRelExpr, Vec<(u64, &'a PhysicalRelExpr)>)>,
    ) -> Result<
        Vec<(
            &'a PhysicalRelExpr,
            &'a PhysicalRelExpr,
            &'a PhysicalRelExpr,
        )>,
        CrustyError,
    > {
        let mut overlaps = vec![];

        // bfs through current plan and don't queue if a cache match is found for a node
        let mut queue = VecDeque::new();
        queue.push_back(self);

        while !queue.is_empty() {
            let node = queue.pop_front().unwrap();
            let hash_val = node.get_tree_hash()?;

            let mut found_match = false;

            for (cached_plan, hash_vec) in &cached_plans_with_hashes {
                for (h, cached_pp_subplan) in hash_vec {
                    if *h == hash_val {
                        overlaps.push((*cached_plan, node, *cached_pp_subplan));
                        found_match = true;
                        break; // don't match more than one subtree of the cached_plan
                    }
                }
            }
            if found_match {
                continue; // don't continue iteration down a parent branch that's been matched
            }

            match node {
                PhysicalRelExpr::Scan { .. } => {}
                PhysicalRelExpr::Select { src, .. }
                | PhysicalRelExpr::Project { src, .. }
                | PhysicalRelExpr::Sort { src, .. }
                | PhysicalRelExpr::Rename { src, .. }
                | PhysicalRelExpr::HashAggregate { src, .. } => {
                    queue.push_back(src);
                }
                PhysicalRelExpr::CrossJoin { left, right, .. }
                | PhysicalRelExpr::NestedLoopJoin { left, right, .. }
                | PhysicalRelExpr::HashJoin { left, right, .. }
                | PhysicalRelExpr::SortMergeJoin { left, right, .. } => {
                    queue.push_back(left);
                    queue.push_back(right);
                }
                _ => {
                    return Err(c_err(&format!(
                        "haven't implemented hash match for specific PhysicalRelExpr type: {:?}",
                        node
                    )));
                }
            }
        }
        Ok(overlaps)
    }

    /// Returns a consistent, canonical tree that will be used for matching two trees
    /// whose hashes ended up being the same.
    fn get_canonical_tree(&self) -> PhysicalRelExpr {
        // TODO: diff trees that are logically equivalent should be sent to same tree result
        unimplemented!()
    }

    /// Verify if two PhysicalRelExpr objects are the exact same
    fn compare_trees(&self, _other: &PhysicalRelExpr) -> bool {
        // will be recursive
        unimplemented!()
    }

    /// Checks if two plans are logically equivalent.
    ///
    /// We expect that this function is only called after we have verified that the hash is the same
    /// so that we can get away with some shortcuts, like col rename checks and whatnot.
    #[allow(unreachable_code, unused_variables)] // remove later
    pub fn compare_matching_plans(&self, other: &PhysicalRelExpr) -> bool {
        return true;
        // eventually, we wanna explicitly check tree equality
        let t1 = self.get_canonical_tree();
        let t2 = other.get_canonical_tree();
        t1.compare_trees(&t2)
    }
}
