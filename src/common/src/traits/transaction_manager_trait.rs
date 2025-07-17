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

    fn shutdown(&self) -> Result<(), FairyError>;

    fn reset(&self) -> Result<(), FairyError>;

    fn set_isolation_level(&self, lvl: IsolationLevel) -> Result<(), FairyError>;

    fn start_transaction(&self, tid: TransactionId) -> Result<(), FairyError>;

    fn read_record(
        &self,
        tuple: &Tuple,
        value_id: &ValueId,
        tid: &TransactionId,
    ) -> Result<(), FairyError>;

    fn pre_update_record(
        &self,
        tuple: &mut Tuple,
        value_id: &ValueId,
        tid: &TransactionId,
        changes: &TupleAssignments,
    ) -> Result<(), FairyError>;

    fn post_update_record(
        &self,
        tuple: &mut Tuple,
        value_id: &ValueId,
        old_value_id: &ValueId,
        tid: &TransactionId,
        changes: &TupleAssignments,
    ) -> Result<(), FairyError>;

    fn pre_insert_record(&self, tuple: &mut Tuple, tid: TransactionId) -> Result<(), FairyError>;

    fn post_insert_record(
        &self,
        tuple: &mut Tuple,
        value_id: ValueId,
        tid: TransactionId,
    ) -> Result<(), FairyError>;

    fn read_predicate(
        &self,
        predicate: Expression<LogicalRelExpr>,
        tid: TransactionId,
    ) -> Result<(), FairyError>;

    fn validate_txn(&self, tid: TransactionId) -> Result<(), FairyError>;

    fn rollback_txn(&self, tid: TransactionId) -> Result<(), FairyError>;

    fn commit_txn(&self, tid: TransactionId) -> Result<(), FairyError>;
}
