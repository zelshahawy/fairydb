use common::{physical::config::ServerConfig, CrustyError};
use log::info;

use crate::{StorageManager, TransactionManager};

#[allow(dead_code)] //TODO: remove this
pub struct IndexManager {
    config: &'static ServerConfig,
    sm: &'static StorageManager,
    tm: &'static TransactionManager,
}

impl IndexManager {
    pub fn new(
        config: &'static ServerConfig,
        sm: &'static StorageManager,
        tm: &'static TransactionManager,
    ) -> Self {
        Self { config, sm, tm }
    }

    pub fn shutdown(&self) -> Result<(), CrustyError> {
        info!("TODO: index manager shutdown is a stub");
        // DO NOT TOUCH sm OR tm, THEY COULD BE SHUT DOWN ALREADY
        // TODO: implement shutdown after index manager is working since there's nothing to shutdown rn
        Ok(())
    }

    pub fn reset(&self) -> Result<(), CrustyError> {
        info!("TODO: index manager reset is a stub");
        // DO NOT TOUCH sm OR tm, THEY COULD BE SHUT DOWN ALREADY
        // TODO: implement reset after index manager is working since there's nothing to reset rn
        Ok(())
    }
}
