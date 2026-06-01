use std::result;
use thiserror::Error;

/// MiniOS unified error type
#[derive(Error, Debug)]
pub enum MiniOsError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Shared memory error: {0}")]
    Shm(String),

    #[error("IPC error: {0}")]
    Ipc(String),

    #[error("Cache error: {0}")]
    #[allow(dead_code)]
    Cache(String),

    #[error("Server error: {0}")]
    Server(String),

    #[error("Client error: {0}")]
    Client(String),

    #[error("Object not found: {0}")]
    NotFound(String),

    #[error("Object already exists: {0}")]
    AlreadyExists(String),

    #[error("No space left on device")]
    NoSpace,

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("UTF-8 conversion error: {0}")]
    Utf8(#[from] std::str::Utf8Error),

    #[error("String UTF-8 error: {0}")]
    FromUtf8(#[from] std::string::FromUtf8Error),
}

pub type Result<T> = result::Result<T, MiniOsError>;
