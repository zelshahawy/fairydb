use common::ids::TransactionId;
use common::CrustyError;

#[derive(Debug, PartialEq, Eq)]
pub enum TxnState {
    Active,
    PartiallyCommitted,
    Committed,
    Failed,
    Aborted,
    Terminated,
}

/// Transaction implementation.
pub struct Transaction {
    tid: TransactionId,
    state: TxnState,
}

impl Default for Transaction {
    fn default() -> Self {
        Self::new()
    }
}

impl Transaction {
    /// Creates a new transaction.
    pub fn new() -> Self {
        Self {
            tid: TransactionId::new(),
            state: TxnState::Active,
        }
    }

    pub fn new_from_tid(tid: TransactionId) -> Self {
        Self {
            tid,
            state: TxnState::Active,
        }
    }

    /// Returns the transaction id.
    pub fn tid(&self) -> Result<TransactionId, CrustyError> {
        if self.state != TxnState::Active {
            Err(CrustyError::TransactionNotActive)
        } else {
            Ok(self.tid)
        }
    }

    /// Commits the transaction.
    pub fn commit(&mut self) -> Result<(), CrustyError> {
        self.state = TxnState::PartiallyCommitted;
        self.complete()
    }

    /// Aborts the transaction.
    pub fn abort(&mut self) -> Result<(), CrustyError> {
        self.state = TxnState::Failed;
        self.complete()
    }

    /// Completes the transaction.
    ///
    /// # Arguments
    ///
    /// * `commit` - True if the transaction should commit.
    pub fn complete(&mut self) -> Result<(), CrustyError> {
        if self.state != TxnState::PartiallyCommitted || self.state != TxnState::Failed {
            error!(
                "Error: FIXME, need to notify on txn complete {:?}",
                self.state
            );
            //FIXME DBSERVER.transaction_complete(self.tid, commit)?;
            self.state = TxnState::Terminated;
        }
        Ok(())
    }
}
