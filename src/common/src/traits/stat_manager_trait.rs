use crate::{
    logical_expr::prelude::Expression, physical::config::ServerConfig,
    physical_expr::physical_rel_expr::PhysicalRelExpr, prelude::*, BinaryOp,
};

pub trait StatManagerTrait {
    fn new(config: &'static ServerConfig, mem_budget: usize) -> Self;

    fn reset(&self) -> Result<(), CrustyError>;

    fn shutdown(&self) -> Result<(), CrustyError>;

    fn deleted_record(&self, value_id: &ValueId) -> Result<(), CrustyError>;

    fn register_table(&self, c_id: ContainerId, schema: TableSchema) -> Result<(), CrustyError>;

    fn updated_record(
        &self,
        tuple: &Tuple,
        value_id: &ValueId,
        old_value_id: Option<&ValueId>,
    ) -> Result<(), CrustyError>;

    fn new_record(&self, tuple: &Tuple, value_id: ValueId) -> Result<(), CrustyError>;

    /// Estimate the number of records and selectivity of the given predicate
    /// Note that the predicate (Expression<PhysicalRelExpr>) should be expressed
    /// in terms of the original indexes of the columns in the container.
    /// Use `OriginExpression` before calling this function. See cardinality_cost_model.rs
    /// for an example.
    fn estimate_count_and_sel(
        &self,
        c_id: ContainerId,
        predicate: &[Expression<PhysicalRelExpr>],
    ) -> Result<(usize, f64), CrustyError>;

    /// Estimate the number of records and selectivity of the given join
    /// `join_ops`, `left_exprs`, and `right_exprs` should have the same length
    /// so that the i-th element of each array corresponds to the same predicate.
    /// Note that the predicates (Expression<PhysicalRelExpr>) should be expressed
    /// in terms of the original indexes of the columns in the container.
    /// Use `OriginExpression` before calling this function. See cardinality_cost_model.rs
    /// for an example.
    fn estimate_join_count_and_sel(
        &self,
        left_c_id: ContainerId,
        right_c_id: ContainerId,
        join_ops: &[BinaryOp],
        left_exprs: &[Expression<PhysicalRelExpr>],
        right_exprs: &[Expression<PhysicalRelExpr>],
    ) -> Result<(usize, f64), CrustyError>;

    /// Given a container and a column, estimate the probability of the attribute
    /// having distinct values. Currently, this function is not used in the codebase.
    /// This should be used to estimate the cardinality of aggregation operations (GROUP BY).
    fn estimate_distinct_prob(
        &self,
        c_id: ContainerId,
        col_id: ColumnId,
    ) -> Result<f64, CrustyError>;

    fn get_container_record_count(&self, c_id: ContainerId) -> Result<usize, CrustyError>;
}
