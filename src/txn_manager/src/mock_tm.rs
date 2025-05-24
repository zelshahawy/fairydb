use common::logical_expr::prelude::{Expression, LogicalRelExpr};
use common::physical::config::ServerConfig;
use common::physical::TupleAssignments;
use common::prelude::*;
use common::traits::transaction_manager_trait::{IsolationLevel, TransactionManagerTrait};

#[derive(Default)]
pub struct MockTransactionManager {}

impl MockTransactionManager {
    pub fn new(_config: &'static ServerConfig) -> Self {
        Self {}
    }
}

impl TransactionManagerTrait for MockTransactionManager {
    fn new(_config: &'static ServerConfig) -> Self {
        Self {}
    }

    fn shutdown(&self) -> Result<(), CrustyError> {
        info!("TODO: txn manager shutdown is a stub");
        Ok(())
    }

    fn reset(&self) -> Result<(), CrustyError> {
        info!("TODO: txn manager reset is a stub");
        Ok(())
    }

    fn set_isolation_level(&self, _lvl: IsolationLevel) -> Result<(), CrustyError> {
        Ok(())
    }

    fn start_transaction(&self, _tid: TransactionId) -> Result<(), CrustyError> {
        Ok(())
    }

    fn read_record(
        &self,
        _tuple: &Tuple,
        _value_id: &ValueId,
        _tid: &TransactionId,
    ) -> Result<(), CrustyError> {
        Ok(())
    }

    fn pre_update_record(
        &self,
        _tuple: &mut Tuple,
        _value_id: &ValueId,
        _tid: &TransactionId,
        _changes: &TupleAssignments,
    ) -> Result<(), CrustyError> {
        Ok(())
    }

    fn post_update_record(
        &self,
        _tuple: &mut Tuple,
        _value_id: &ValueId,
        _old_value_id: &ValueId,
        _tid: &TransactionId,
        _changes: &TupleAssignments,
    ) -> Result<(), CrustyError> {
        Ok(())
    }

    fn pre_insert_record(
        &self,
        _tuple: &mut Tuple,
        _tid: TransactionId,
    ) -> Result<(), CrustyError> {
        Ok(())
    }

    fn post_insert_record(
        &self,
        _tuple: &mut Tuple,
        _value_id: ValueId,
        _tid: TransactionId,
    ) -> Result<(), CrustyError> {
        Ok(())
    }

    fn read_predicate(
        &self,
        _predicate: Expression<LogicalRelExpr>,
        _tid: TransactionId,
    ) -> Result<(), CrustyError> {
        Ok(())
    }

    fn validate_txn(&self, _tid: TransactionId) -> Result<(), CrustyError> {
        Ok(())
    }

    fn rollback_txn(&self, _tid: TransactionId) -> Result<(), CrustyError> {
        Ok(())
    }

    fn commit_txn(&self, _tid: TransactionId) -> Result<(), CrustyError> {
        Ok(())
    }
}
