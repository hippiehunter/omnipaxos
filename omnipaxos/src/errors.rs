#[cfg(feature = "toml_config")]
use std::io;
use std::{error, fmt};
#[cfg(feature = "toml_config")]
use toml;

/// Error from the storage backend
#[derive(Debug)]
pub struct StorageError(pub Box<dyn std::error::Error + Send + Sync>);

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "storage error: {}", self.0)
    }
}

impl error::Error for StorageError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

/// Top-level OmniPaxos error
#[derive(Debug)]
pub enum OmniPaxosError {
    /// Error from the storage backend
    Storage(StorageError),
    /// Compaction error
    Compaction(super::CompactionErr),
    /// Invalid configuration
    InvalidConfig(ConfigError),
}

impl fmt::Display for OmniPaxosError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            OmniPaxosError::Storage(e) => write!(f, "{}", e),
            OmniPaxosError::Compaction(e) => write!(f, "{}", e),
            OmniPaxosError::InvalidConfig(e) => write!(f, "{}", e),
        }
    }
}

impl error::Error for OmniPaxosError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            OmniPaxosError::Storage(e) => Some(e),
            OmniPaxosError::Compaction(e) => Some(e),
            OmniPaxosError::InvalidConfig(e) => Some(e),
        }
    }
}

impl From<StorageError> for OmniPaxosError {
    fn from(e: StorageError) -> Self {
        OmniPaxosError::Storage(e)
    }
}

impl From<ConfigError> for OmniPaxosError {
    fn from(e: ConfigError) -> Self {
        OmniPaxosError::InvalidConfig(e)
    }
}

impl From<Box<dyn std::error::Error>> for StorageError {
    fn from(e: Box<dyn std::error::Error>) -> Self {
        StorageError(e.to_string().into())
    }
}

impl From<super::CompactionErr> for StorageError {
    fn from(e: super::CompactionErr) -> Self {
        StorageError(Box::new(e))
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
