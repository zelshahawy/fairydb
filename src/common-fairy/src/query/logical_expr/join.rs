// Reference: https://github.com/rotaki/decorrelator

use super::prelude::*;
use crate::physical::col_id_generator::ColIdGeneratorRef;
use crate::query::rules::RulesRef;
use std::collections::HashSet;

impl LogicalRelExpr {
    /// Apply join to the current and the other logical relational expressions.
    pub fn join(
        self,
        optimize: bool,
        enabled_rules: &RulesRef,
        col_id_gen: &ColIdGeneratorRef,
        join_type: JoinType,
        other: LogicalRelExpr,
        mut predicates: Vec<Expression<LogicalRelExpr>>,
    ) -> LogicalRelExpr {
        if predicates.is_empty() {
            return LogicalRelExpr::Join {
                join_type,
                left: Box::new(self),
                right: Box::new(other),
                predicates,
            };
        }

        predicates = predicates
            .into_iter()
            .flat_map(|expr| expr.split_conjunction())
            .collect();

        if optimize {
            if matches!(
                join_type,
                JoinType::Inner | JoinType::LeftOuter | JoinType::CrossJoin
            ) {
                // Notice the difference from rotaki/decorrelator. Determine which
                // predicates can be pushed down to the left and right sides respectively.
                let (push_down_to_left, keep): (
                    Vec<&Expression<LogicalRelExpr>>,
                    Vec<&Expression<LogicalRelExpr>>,
                ) = predicates.iter().partition(|pred| pred.bound_by(&self));
                let (push_down_to_right, keep): (
                    Vec<&Expression<LogicalRelExpr>>,
                    Vec<&Expression<LogicalRelExpr>>,
                ) = keep.iter().partition(|pred| pred.bound_by(&other));

                // TODO: Check if the following condition is correct. What if, for
                // example, `push_down_to_left` is empty but `push_down_to_right` is not?
                if !push_down_to_left.is_empty() || !push_down_to_right.is_empty() {
                    // This condition is necessary to avoid infinite recursion
                    let push_down_to_left = push_down_to_left.into_iter().cloned().collect();
                    let push_down_to_right = push_down_to_right.into_iter().cloned().collect();
                    let keep: Vec<Expression<LogicalRelExpr>> = keep.into_iter().cloned().collect();

                    // Cross join can be made into an inner join if all the predicates
                    // are bound by the left and right sides.
                    let is_inner = keep.iter().all(|expr| expr.intersect_with(&other));
                    let join_type = if is_inner && matches!(join_type, JoinType::CrossJoin) {
                        JoinType::Inner
                    } else {
                        join_type
                    };

                    // Notice the difference from rotaki/decorrelator. Here, we
                    // push down predicates to both sides and then join the two sides.
                    return self
                        .select(true, enabled_rules, col_id_gen, push_down_to_left)
                        .join(
                            false,
                            enabled_rules,
                            col_id_gen,
                            join_type,
                            other.select(true, enabled_rules, col_id_gen, push_down_to_right),
                            keep,
                        );
                }
            }

            if matches!(
                join_type,
                JoinType::Inner | JoinType::RightOuter | JoinType::CrossJoin
            ) {
                let (push_down, keep): (Vec<_>, Vec<_>) =
                    predicates.iter().partition(|pred| pred.bound_by(&other));
                if !push_down.is_empty() {
                    // This condition is necessary to avoid infinite recursion
                    let push_down = push_down.into_iter().cloned().collect();
                    let keep = keep.into_iter().cloned().collect();
                    return self.join(
                        false,
                        enabled_rules,
                        col_id_gen,
                        join_type,
                        other.select(true, enabled_rules, col_id_gen, push_down),
                        keep,
                    );
                }
            }

            // If the remaining predicates are bound by the left and right sides
            if matches!(join_type, JoinType::CrossJoin) {
                #[cfg(debug_assertions)]
                {
                    // The remaining predicates should not contain any free vaiables.
                    // Need to use flatmap or map to reference a free variable.
                    let free = predicates
                        .iter()
                        .flat_map(|expr| expr.free())
                        .collect::<HashSet<_>>();
                    let atts = self.att().union(&other.att()).cloned().collect();
                    assert!(free.is_subset(&atts));
                }

                return self.join(
                    false,
                    enabled_rules,
                    col_id_gen,
                    JoinType::Inner,
                    other,
                    predicates,
                );
            }
        }

        LogicalRelExpr::Join {
            join_type,
            left: Box::new(self),
            right: Box::new(other),
            predicates,
        }
    }
}
