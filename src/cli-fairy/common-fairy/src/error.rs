use crate::ids::TransactionId;
use std::error::Error;
use std::fmt;
use std::io;

pub fn c_err(s: &str) -> CrustyError {
    CrustyError::CrustyError(s.to_string())
}

/// Custom error type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CrustyError {
    /// IO Errors.
    IOError(String),
    /// Serialization errors.
    SerializationError(String),
    /// Custom errors.
    CrustyError(String),
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

impl fmt::Display for CrustyError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                CrustyError::ValidationError(s) => format!("Validation Error: {}", s),
                CrustyError::ExecutionError(s) => format!("Execution Error: {}", s),
                CrustyError::CrustyError(s) => format!("Crusty Error: {}", s),
                CrustyError::IOError(s) => s.to_string(),
                CrustyError::SerializationError(s) => s.to_string(),
                CrustyError::TransactionNotActive => String::from("Transaction Not Active Error"),
                CrustyError::InvalidMutationError(s) => format!("InvalidMutationError {}", s),
                CrustyError::TransactionRollback(tid) =>
                    format!("Transaction Rolledback {:?}", tid),
                CrustyError::StorageError => "Storage Error".to_string(),
                CrustyError::ContainerDoesNotExist => "Container Does Not Exist".to_string(),
                CrustyError::InvalidOperation => "Invalid Operation".to_string(),
            }
        )
    }
}

// Implement std::convert::From for AppError; from io::Error
impl From<io::Error> for CrustyError {
    fn from(error: io::Error) -> Self {
        CrustyError::IOError(error.to_string())
    }
}

// Implement std::convert::From for std::sync::PoisonError
impl<T> From<std::sync::PoisonError<T>> for CrustyError {
    fn from(error: std::sync::PoisonError<T>) -> Self {
        CrustyError::ExecutionError(error.to_string())
    }
}

impl Error for CrustyError {}

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
