use crate::{
    logical_expr::prelude::{Expression, LogicalRelExpr},
    physical::config::ServerConfig,
    physical::TupleAssignments,
    prelude::*,
};

pub enum IsolationLevel {
    ReadCommitted,
}

pub trait TransactionManagerTrait {
    fn new(config: &'static ServerConfig) -> Self
    where
        Self: Sized;

    fn shutdown(&self) -> Result<(), CrustyError>;

    fn reset(&self) -> Result<(), CrustyError>;

    fn set_isolation_level(&self, lvl: IsolationLevel) -> Result<(), CrustyError>;

    fn start_transaction(&self, tid: TransactionId) -> Result<(), CrustyError>;

    fn read_record(
        &self,
        tuple: &Tuple,
        value_id: &ValueId,
        tid: &TransactionId,
    ) -> Result<(), CrustyError>;

    fn pre_update_record(
        &self,
        tuple: &mut Tuple,
        value_id: &ValueId,
        tid: &TransactionId,
        changes: &TupleAssignments,
    ) -> Result<(), CrustyError>;

    fn post_update_record(
        &self,
        tuple: &mut Tuple,
        value_id: &ValueId,
        old_value_id: &ValueId,
        tid: &TransactionId,
        changes: &TupleAssignments,
    ) -> Result<(), CrustyError>;

    fn pre_insert_record(&self, tuple: &mut Tuple, tid: TransactionId) -> Result<(), CrustyError>;

    fn post_insert_record(
        &self,
        tuple: &mut Tuple,
        value_id: ValueId,
        tid: TransactionId,
    ) -> Result<(), CrustyError>;

    fn read_predicate(
        &self,
        predicate: Expression<LogicalRelExpr>,
        tid: TransactionId,
    ) -> Result<(), CrustyError>;

    fn validate_txn(&self, tid: TransactionId) -> Result<(), CrustyError>;

    fn rollback_txn(&self, tid: TransactionId) -> Result<(), CrustyError>;

    fn commit_txn(&self, tid: TransactionId) -> Result<(), CrustyError>;
}
