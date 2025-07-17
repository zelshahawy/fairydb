use crate::ids::TransactionId;
use std::error::Error;
use std::fmt;
use std::io;

pub fn c_err(s: &str) -> FairyError {
    FairyError::FairyError(s.to_string())
}

/// Custom error type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FairyError {
    /// IO Errors.
    IOError(String),
    /// Serialization errors.
    SerializationError(String),
    /// Custom errors.
    FairyError(String),
    /// Validation errors.
    ValidationError(String),
    /// Execution errors.
    ExecutionError(String),
    /// Transaction aborted or committed.
    TransactionNotActive,
    /// Invalid insert or update
    InvalidMutationError(String),
    /// Transaction Rollback
    TransactionRollback(TransactionId),
    /// Storage Error
    StorageError,
    /// Missing / invalid container
    ContainerDoesNotExist,
    /// Invalid Operation
    InvalidOperation,
}

impl fmt::Display for FairyError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                FairyError::ValidationError(s) => format!("Validation Error: {}", s),
                FairyError::ExecutionError(s) => format!("Execution Error: {}", s),
                FairyError::FairyError(s) => format!("Fairy Error: {}", s),
                FairyError::IOError(s) => s.to_string(),
                FairyError::SerializationError(s) => s.to_string(),
                FairyError::TransactionNotActive => String::from("Transaction Not Active Error"),
                FairyError::InvalidMutationError(s) => format!("InvalidMutationError {}", s),
                FairyError::TransactionRollback(tid) => format!("Transaction Rolledback {:?}", tid),
                FairyError::StorageError => "Storage Error".to_string(),
                FairyError::ContainerDoesNotExist => "Container Does Not Exist".to_string(),
                FairyError::InvalidOperation => "Invalid Operation".to_string(),
            }
        )
    }
}

// Implement std::convert::From for AppError; from io::Error
impl From<io::Error> for FairyError {
    fn from(error: io::Error) -> Self {
        FairyError::IOError(error.to_string())
    }
}

// Implement std::convert::From for std::sync::PoisonError
impl<T> From<std::sync::PoisonError<T>> for FairyError {
    fn from(error: std::sync::PoisonError<T>) -> Self {
        FairyError::ExecutionError(error.to_string())
    }
}

impl Error for FairyError {}

/// Specify an issue when ingesting/converting a record
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConversionError {
    FieldConstraintError(usize, String),
    PrimaryKeyViolation,
    UniqueViolation,
    TransactionViolation(TransactionId, String),
    ParseError,
    UnsupportedType,
    NullFieldNotAllowed(usize),
    WrongType,
}
