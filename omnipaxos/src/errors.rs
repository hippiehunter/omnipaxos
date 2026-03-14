#[cfg(feature = "toml_config")]
use std::io;
use std::{error, fmt};
#[cfg(feature = "toml_config")]
use toml;

/// A type-erased error that preserves the full error chain.
///
/// Wraps `Box<dyn Error + Send + Sync + 'static>` with convenience methods
/// for downcasting. Unlike a raw boxed error, `AnyError` implements `Error`
/// itself, so it can be used as a `source()` in error chains.
#[derive(Debug)]
pub struct AnyError(Box<dyn error::Error + Send + Sync + 'static>);

impl AnyError {
    /// Wrap any error.
    pub fn new<E: error::Error + Send + Sync + 'static>(e: E) -> Self {
        AnyError(Box::new(e))
    }

    /// Wrap an already-boxed error.
    pub fn from_boxed(b: Box<dyn error::Error + Send + Sync + 'static>) -> Self {
        AnyError(b)
    }

    /// Attempt to downcast the inner error to a concrete type.
    pub fn downcast<T: error::Error + 'static>(self) -> Result<T, Self> {
        match self.0.downcast::<T>() {
            Ok(t) => Ok(*t),
            Err(b) => Err(AnyError(b)),
        }
    }

    /// Attempt to get a reference to the inner error as a concrete type.
    pub fn downcast_ref<T: error::Error + 'static>(&self) -> Option<&T> {
        self.0.downcast_ref::<T>()
    }
}

impl fmt::Display for AnyError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl error::Error for AnyError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        self.0.source()
    }
}

// Note: We can't do a blanket From<E: Error> impl because it would conflict
// with From<T> for T from core. Instead, AnyError::new() or AnyError::from_boxed()
// must be called explicitly, or specific From impls can be added for common types.

/// Identifies which storage operation failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageOperation {
    /// Loading persisted state on startup.
    LoadState,
    /// An atomic write batch.
    WriteAtomic,
    /// Reading entries from the log.
    ReadEntries,
    /// Reading a log suffix.
    ReadSuffix,
    /// Reading a snapshot.
    ReadSnapshot,
    /// Reading the log length.
    ReadLogLen,
    /// Creating a snapshot from entries.
    CreateSnapshot,
}

impl fmt::Display for StorageOperation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StorageOperation::LoadState => write!(f, "load_state"),
            StorageOperation::WriteAtomic => write!(f, "write_atomically"),
            StorageOperation::ReadEntries => write!(f, "get_entries"),
            StorageOperation::ReadSuffix => write!(f, "get_suffix"),
            StorageOperation::ReadSnapshot => write!(f, "get_snapshot"),
            StorageOperation::ReadLogLen => write!(f, "get_log_len"),
            StorageOperation::CreateSnapshot => write!(f, "create_snapshot"),
        }
    }
}

/// Describes what data was being operated on when a storage error occurred.
///
/// Provides context for debugging — e.g., which log index or snapshot was
/// involved in the failed operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorSubject {
    /// General storage state (e.g., loading persisted state on startup).
    State,
    /// A range of log entries.
    LogEntries {
        /// Start index (inclusive).
        from: usize,
        /// End index (exclusive).
        to: usize,
    },
    /// Snapshot data.
    Snapshot,
    /// Multiple operations in an atomic batch.
    Batch,
}

impl fmt::Display for ErrorSubject {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ErrorSubject::State => write!(f, "state"),
            ErrorSubject::LogEntries { from, to } => write!(f, "log[{}..{}]", from, to),
            ErrorSubject::Snapshot => write!(f, "snapshot"),
            ErrorSubject::Batch => write!(f, "batch"),
        }
    }
}

/// Error from the storage backend, enriched with the operation that failed.
///
/// Always fatal by convention — callers should stop processing and surface
/// the error to the application.
#[derive(Debug)]
pub struct StorageError {
    /// Which storage operation failed.
    pub operation: StorageOperation,
    /// What data was being operated on.
    pub subject: ErrorSubject,
    /// The underlying error from the storage backend.
    pub source: AnyError,
    /// Optional human-readable context (e.g., batch contents).
    pub details: Option<String>,
}

impl StorageError {
    /// Create a new `StorageError` with context.
    pub fn new(operation: StorageOperation, subject: ErrorSubject, source: AnyError) -> Self {
        StorageError { operation, subject, source, details: None }
    }

    /// Attach human-readable details to this error (e.g., batch operation summary).
    pub fn with_details(mut self, details: impl Into<String>) -> Self {
        self.details = Some(details.into());
        self
    }

    /// Create a storage error for a read_entries failure.
    pub fn read_entries(from: usize, to: usize, source: AnyError) -> Self {
        Self::new(StorageOperation::ReadEntries, ErrorSubject::LogEntries { from, to }, source)
    }

    /// Create a storage error for a read_snapshot failure.
    pub fn read_snapshot(source: AnyError) -> Self {
        Self::new(StorageOperation::ReadSnapshot, ErrorSubject::Snapshot, source)
    }

    /// Create a storage error for a write_atomically failure.
    pub fn write_batch(source: AnyError) -> Self {
        Self::new(StorageOperation::WriteAtomic, ErrorSubject::Batch, source)
    }

    /// Create a storage error for load_state failure.
    pub fn load_state(source: AnyError) -> Self {
        Self::new(StorageOperation::LoadState, ErrorSubject::State, source)
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "storage error during {} on {}", self.operation, self.subject)?;
        if let Some(ref details) = self.details {
            write!(f, " ({})", details)?;
        }
        write!(f, ": {}", self.source)
    }
}

impl error::Error for StorageError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        Some(&self.source)
    }
}

/// Top-level OmniPaxos error
#[derive(Debug)]
pub enum OmniPaxosError {
    /// Fatal storage error — the node should stop processing.
    Fatal(StorageError),
    /// Invalid configuration
    InvalidConfig(ConfigError),
    /// Corrupted or inconsistent persisted state detected at startup.
    CorruptedState(String),
}

impl OmniPaxosError {
    /// Returns `true` if this is a fatal error (storage failure or corrupted state).
    pub fn is_fatal(&self) -> bool {
        matches!(self, OmniPaxosError::Fatal(_) | OmniPaxosError::CorruptedState(_))
    }

    /// Consume self and return the `StorageError` if fatal, or `None`.
    pub fn into_fatal(self) -> Option<StorageError> {
        match self {
            OmniPaxosError::Fatal(e) => Some(e),
            _ => None,
        }
    }
}

impl fmt::Display for OmniPaxosError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            OmniPaxosError::Fatal(e) => write!(f, "{}", e),
            OmniPaxosError::InvalidConfig(e) => write!(f, "{}", e),
            OmniPaxosError::CorruptedState(msg) => write!(f, "corrupted persisted state: {}", msg),
        }
    }
}

impl error::Error for OmniPaxosError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            OmniPaxosError::Fatal(e) => Some(e),
            OmniPaxosError::InvalidConfig(e) => Some(e),
            OmniPaxosError::CorruptedState(_) => None,
        }
    }
}

impl From<StorageError> for OmniPaxosError {
    fn from(e: StorageError) -> Self {
        OmniPaxosError::Fatal(e)
    }
}

impl From<ConfigError> for OmniPaxosError {
    fn from(e: ConfigError) -> Self {
        OmniPaxosError::InvalidConfig(e)
    }
}

/// Error type for the reading and parsing of OmniPaxosConfig TOML files
#[derive(Debug)]
pub enum ConfigError {
    #[cfg(feature = "toml_config")]
    /// Could not read TOML file
    ReadFile(io::Error),
    #[cfg(feature = "toml_config")]
    /// Could not parse TOML file
    Parse(toml::de::Error),
    /// Invalid config fields
    InvalidConfig(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            #[cfg(feature = "toml_config")]
            ConfigError::ReadFile(ref err) => write!(f, "{}", err),
            #[cfg(feature = "toml_config")]
            ConfigError::Parse(ref err) => write!(f, "{}", err),
            ConfigError::InvalidConfig(ref str) => write!(f, "Invalid config: {}", str),
        }
    }
}

impl error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            #[cfg(feature = "toml_config")]
            ConfigError::ReadFile(ref err) => Some(err),
            #[cfg(feature = "toml_config")]
            ConfigError::Parse(ref err) => Some(err),
            ConfigError::InvalidConfig(_) => None,
        }
    }
}

#[cfg(feature = "toml_config")]
impl From<io::Error> for ConfigError {
    fn from(err: io::Error) -> ConfigError {
        ConfigError::ReadFile(err)
    }
}

#[cfg(feature = "toml_config")]
impl From<toml::de::Error> for ConfigError {
    fn from(err: toml::de::Error) -> ConfigError {
        ConfigError::Parse(err)
    }
}

#[allow(missing_docs)]
macro_rules! valid_config {
    ($pred:expr,$err_str:expr) => {
        if !$pred {
            return Err(ConfigError::InvalidConfig($err_str.to_owned()));
        }
    };
}
pub(crate) use valid_config;
