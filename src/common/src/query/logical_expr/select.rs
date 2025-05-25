// Reference: https://github.com/rotaki/decorrelator

use super::prelude::*;
use crate::physical::col_id_generator::ColIdGeneratorRef;
use crate::query::rules::{Rule, RulesRef};
use std::collections::HashSet;

impl LogicalRelExpr {
    /// Apply selection to the current logical relational expression.
    pub fn select(
        self,
        optimize: bool,
        enabled_rules: &RulesRef,
        col_id_gen: &ColIdGeneratorRef,
        predicates: Vec<Expression<LogicalRelExpr>>,
    ) -> LogicalRelExpr {
        if predicates.is_empty() {
            return self;
        }
        let mut predicates = predicates
            .into_iter()
            .flat_map(|expr| expr.split_conjunction())
            .collect();

        if optimize && enabled_rules.is_enabled(&Rule::SelectionPushdown) {
            match self {
                LogicalRelExpr::Select {
                    src,
                    predicates: mut preds,
                } => {
                    preds.append(&mut predicates);
                    src.select(true, enabled_rules, col_id_gen, preds)
                }
                LogicalRelExpr::Join {
                    join_type,
                    left,
                    right,
                    predicates: mut preds,
                } => {
                    preds.append(&mut predicates);
                    left.join(true, enabled_rules, col_id_gen, join_type, *right, preds)
                }
                LogicalRelExpr::Aggregate {
                    src,
                    group_by,
                    aggrs,
                } => {
                    // If the predicate is bound by the group by columns, we can push it to the source
                    let group_by_cols: HashSet<_> = group_by.iter().cloned().collect();
                    let (push_down, keep): (Vec<_>, Vec<_>) = predicates
                        .into_iter()
                        .partition(|pred| pred.free().is_subset(&group_by_cols));
                    src.select(true, enabled_rules, col_id_gen, push_down)
                        .aggregate(group_by, aggrs)
                        .select(false, enabled_rules, col_id_gen, keep)
                }
                LogicalRelExpr::Map { input, exprs } => {
                    // If the map is a->b and a is not free and b is used as a selection, then
                    // we can replace b with a in the selection
                    // e.g. if @0 and @1 are bound columns, we can rewrite
                    // FROM: select(@2) <- map(@2 <- @1 + @0)
                    // TO:   select(@1 + @0) <- map(@2 <- @1 + @0)
                    for (dest_id, expr) in &exprs {
                        for pred in &mut predicates {
                            if matches!(pred, Expression::ColRef { id } if *id == *dest_id)
                                && expr.bound_by(&input)
                            {
                                // If expr introduces a reference to a column in an outer scope,
                                // bound_by becomes false
                                *pred = expr.clone();
                            }
                        }
                    }
                    // If the predicate does not intersect with the atts of exprs, we can push it to the source
                    let atts = exprs.iter().map(|(id, _)| *id).collect::<HashSet<_>>();
                    let (push_down, keep): (Vec<_>, Vec<_>) = predicates
                        .into_iter()
                        .partition(|pred| pred.free().is_disjoint(&atts));
                    input
                        .select(true, enabled_rules, col_id_gen, push_down)
                        .map(false, enabled_rules, col_id_gen, exprs)
                        .select(false, enabled_rules, col_id_gen, keep)
                }
                _ => self.select(false, enabled_rules, col_id_gen, predicates),
            }
        } else {
            LogicalRelExpr::Select {
                src: Box::new(self),
                predicates,
            }
        }
    }
}
