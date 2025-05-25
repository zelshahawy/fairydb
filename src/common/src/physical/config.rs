use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use clap::Parser;
use serde::Deserialize;
use serde_json;

#[derive(Clone, Deserialize, Debug, Parser)]
#[serde(default)]
pub struct ServerConfig {
    /// Server IP address
    #[clap(short = 'o', long = "host", default_value = "127.0.0.1")]
    pub host: String,
    /// Server port number
    #[clap(short = 'p', long = "port", default_value = "3333")]
    pub port: String,
    /// Path where server info (all DB, managers, etc. info) is stored
    #[clap(
        short = 'd',
        long = "server_path",
        default_value = "crusty_data/persist/default/"
    )]
    pub db_path: PathBuf,
    /// Log file
    #[clap(short = 'l', long = "log_file", default_value = "")]
    pub log_file: String,
    /// Log level
    #[clap(short = 'v', long = "log_level", default_value = "warn")]
    pub log_level: String,
    /// Query subsumption detection flag (include for val = true)
    #[clap(short = 'q', long = "query-subplan-detection")]
    pub subsumption_detection: bool,
    /// Path to configuration file (if provided, it will override command-line args)
    #[clap(short = 'c', long = "config_file")]
    pub config_file: Option<PathBuf>,
    /// Purge entire db_state on shutdown (include for val = true, otherwise will attempt to persist)
    #[clap(long = "shutdown-purge")]
    pub shutdown_purge: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            host: "127.0.0.1".to_owned(),
            port: "3333".to_owned(),
            db_path: "crusty_data/persist/default/".into(),
            log_file: "".to_owned(),
            log_level: "warning".to_owned(),
            subsumption_detection: false,
            config_file: None,
            shutdown_purge: false,
        }
    }
}

impl ServerConfig {
    pub fn new() -> Self {
        ServerConfig::default()
    }

    pub fn temporary() -> Self {
        let base_dir = tempfile::tempdir().unwrap().into_path();
        ServerConfig {
            db_path: base_dir,
            ..ServerConfig::default()
        }
    }

    /// Loads configuration from a JSON file, using default values for any unspecified options.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the JSON configuration file
    ///
    /// # Returns
    ///
    /// A `Result` containing either the loaded `ServerConfig` or an error
    ///
    /// # Example
    ///
    /// ```
    /// use common::physical::config::ServerConfig;
    ///
    /// let config = ServerConfig::from_file("config.json").unwrap();
    /// ```
    pub fn from_file<P: AsRef<std::path::Path>>(path: P) -> std::io::Result<Self> {
        // Read the file content
        let mut file = File::open(&path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        // Parse the JSON file directly into ServerConfig
        // Missing fields will use default values automatically
        match serde_json::from_str(&contents) {
            Ok(config) => {
                println!(
                    "Parsed server config from path: {}",
                    path.as_ref().display()
                );
                Ok(config)
            }
            Err(e) => {
                println!(
                    "Failed to parse server config from path: {}",
                    path.as_ref().display()
                );
                Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
            }
        }
    }

    /// Creates a ServerConfig by parsing command-line arguments and potentially loading from a config file.
    /// If a config file is specified, it takes full precedence over command-line arguments, with unspecified fields defaulting to the default values.
    ///
    /// # Returns
    ///
    /// A `ServerConfig` with values from either the config file or command-line args
    pub fn from_command_line() -> Self {
        // Parse command-line arguments first
        let args_config = ServerConfig::parse();

        // If a config file was specified, use it exclusively
        if let Some(config_path) = &args_config.config_file {
            if let Ok(file_config) = Self::from_file(config_path) {
                // Use the file config, but keep the config_file path
                let mut config = file_config;
                config.config_file = args_config.config_file.clone();
                println!("Using configuration from file: {}", config_path.display());
                return config;
            } else {
                println!(
                    "Warning: Could not load config file: {}",
                    config_path.display()
                );
                println!("Falling back to command-line arguments");
            }
        }

        // If no config file was specified or it couldn't be loaded, use command-line args
        args_config
    }
}

#[derive(Parser, Deserialize, Debug)]
pub struct ClientConfig {
    /// Server IP address
    #[clap(short = 'o', long = "host", default_value = "")]
    pub host: String,
    /// Server port number
    #[clap(short = 'p', long = "port", default_value = "")]
    pub port: String,
    /// Optional script to run
    #[clap(short = 's', long = "script", default_value = "")]
    pub script: String,
}

impl Default for ClientConfig {
    fn default() -> Self {
        ClientConfig {
            host: "127.0.0.1".to_owned(),
            port: "3333".to_owned(),
            script: "".to_owned(),
        }
    }
}

impl ClientConfig {
    pub fn resolved() -> Self {
        let cli_config = ClientConfig::parse();

        if !cli_config.host.is_empty() && !cli_config.port.is_empty() {
            return cli_config;
        }

        // Load from ServerConfig
        // let server_config = ServerConfig::from_command_line();

        ClientConfig {
            host: if !cli_config.host.is_empty() {
                cli_config.host
            } else {
                "127.0.0.1".to_owned()
            },
            port: if !cli_config.port.is_empty() {
                cli_config.port
            } else {
                "3333".to_owned()
            },
            script: cli_config.script,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_server_config_default() {
        let config = ServerConfig::default();
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.port, "3333");
        assert_eq!(
            config.db_path,
            PathBuf::from("crusty_data/persist/default/")
        );
        assert_eq!(config.log_file, "");
        assert_eq!(config.log_level, "warning");
        assert!(!config.subsumption_detection);
    }

    #[test]
    fn test_server_config_from_file_non_existant() {
        // Tests that from_file returns an error when the file doesn't exist
        let result = ServerConfig::from_file("/nonexistent/path/config.json");
        assert!(result.is_err());
    }

    #[test]
    fn test_server_config_from_file_partial() {
        // Tests that from_file fills missing fields with default values

        // Create a temporary file with some configuration
        let mut file = NamedTempFile::new().unwrap();
        write!(file, "{{\"host\": \"0.0.0.0\", \"port\": \"9999\"}}").unwrap();

        // Read the configuration from the file
        let result = ServerConfig::from_file(file.path());
        assert!(result.is_ok());

        // Check that the configuration has the expected values
        let config = result.unwrap();
        assert_eq!(config.host, "0.0.0.0");
        assert_eq!(config.port, "9999");
        assert_eq!(
            config.db_path,
            PathBuf::from("crusty_data/persist/default/")
        );
        assert_eq!(config.log_file, "");
        assert_eq!(config.log_level, "warning");
        assert!(!config.subsumption_detection);
    }

    #[test]
    fn test_server_config_from_file_not_json() {
        // Tests that from_file fails when provided with a non-JSON file

        // Create a temporary file with some configuration
        let mut file = NamedTempFile::new().unwrap();
        write!(file, "host=0.0.0.0\nport=9999\ndb_path=different_path\nlog_file=different_log_file\nlog_level=different_level\nsubsumption_detection=true").unwrap();

        // Read the configuration from the file
        let result = ServerConfig::from_file(file.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_server_config_from_file_success() {
        // Tests that from_file returns the correct configuration

        // Create a temporary file with some configuration
        let mut file = NamedTempFile::new().unwrap();
        write!(file, "{{\"host\": \"0.0.0.0\", \"port\": \"9999\", \"db_path\": \"different/path\", \"log_file\": \"different_log_file\", \"log_level\": \"different_level\", \"subsumption_detection\": true}}").unwrap();

        // Read the configuration from the file
        let result = ServerConfig::from_file(file.path());
        assert!(result.is_ok());
        let config = result.unwrap();

        // Test that the config values are correct
        assert_eq!(config.host, "0.0.0.0");
        assert_eq!(config.port, "9999");
        assert_eq!(config.db_path, PathBuf::from("different/path"));
        assert_eq!(config.log_file, "different_log_file");
        assert_eq!(config.log_level, "different_level");
        assert!(config.subsumption_detection);
    }

    #[test]
    fn test_server_config_from_file_extra_fields() {
        // Tests that from_file ignores extra fields

        // Create a temporary file with some configuration
        let mut file = NamedTempFile::new().unwrap();
        write!(file, "{{\"host\": \"0.0.0.0\", \"extra\": \"extra\"}}").unwrap();

        // Read the configuration from the file
        let result = ServerConfig::from_file(file.path());
        assert!(result.is_ok());
        let config = result.unwrap();

        // Test that the config values are correct
        assert_eq!(config.host, "0.0.0.0");
        assert_eq!(config.port, "3333");
        assert_eq!(
            config.db_path,
            PathBuf::from("crusty_data/persist/default/")
        );
        assert_eq!(config.log_file, "");
        assert_eq!(config.log_level, "warning");
        assert!(!config.subsumption_detection);
    }
}
