#[macro_use]
extern crate log;

pub mod mutator;
pub mod opiterator;
pub mod query;
pub mod stats;
pub mod testutil;

use std::path::PathBuf;

use crate::stats::reservoir_stat_manager::ReservoirStatManager;
use common::physical::{config::ServerConfig, small_string::StringManager};
use common::traits::stat_manager_trait::StatManagerTrait;
use common::traits::storage_trait::StorageTrait;
use common::traits::transaction_manager_trait::TransactionManagerTrait;
use common::{prelude::*, MANAGERS_DIR_NAME};
pub use index::IndexManager;
pub use storage::{StorageManager, STORAGE_DIR};
pub use txn_manager::mock_tm::MockTransactionManager as TransactionManager;

/// This is a wrapper for the managers, which are components responsible
/// for various parts of the system (e.g. storage, indices, etc).
/// This is used to pass around the managers easily.
/// A manager may have more than one implementation, such as having different
/// storage manager implementations for different storage backends.
pub struct Managers {
    pub config: &'static ServerConfig,
    pub sm: &'static StorageManager,
    pub tm: &'static TransactionManager,
    pub im: &'static IndexManager,
    pub stats: &'static ReservoirStatManager,
    pub strm: &'static StringManager,
    pub path: PathBuf,
}

impl Managers {
    pub fn new(
        config: &'static ServerConfig,
        sm: &'static StorageManager,
        tm: &'static TransactionManager,
        im: &'static IndexManager,
        stats: &'static ReservoirStatManager,
        strm: &'static StringManager,
    ) -> Self {
        let mut path = config.db_path.clone();
        path.push(MANAGERS_DIR_NAME);
        Self {
            config,
            sm,
            tm,
            im,
            stats,
            strm,
            path,
        }
    }

    pub fn shutdown(&self) {
        // reverse order of boot-up just to be safe
        self.strm.shutdown().unwrap();
        self.stats.shutdown().unwrap(); // refer to this to see how and to where things are being serialized
        self.im.shutdown().unwrap();
        self.tm.shutdown().unwrap();
        self.sm.shutdown();
    }

    pub fn reset(&self) -> Result<(), CrustyError> {
        // not responsible for clearing serialized data. see caller for that.
        self.strm.reset()?;
        self.stats.reset()?;
        self.im.reset()?;
        self.tm.reset()?;
        self.sm.reset()?;
        Ok(())
    }
}
