use std::fmt::{self};
use std::sync::LazyLock;
use std::{
    fmt::{Display, Formatter},
    str::FromStr,
};

use serde::{Deserialize, Serialize};

pub static CONFIG: LazyLock<Config> = LazyLock::new(|| Config::load().unwrap());

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub enum DataSourceType {
    #[default]
    AwsAthenaALBLog,
}

impl FromStr for DataSourceType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "aws_athena" => Ok(DataSourceType::AwsAthenaALBLog),
            _ => Err(format!("Unknown data source type: {}", s)),
        }
    }
}
impl DataSourceType {
    pub fn all() -> [DataSourceType; 1] {
        [DataSourceType::AwsAthenaALBLog]
    }
}

impl Display for DataSourceType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DataSourceType::AwsAthenaALBLog => write!(f, "AwsAthenaALBLog"),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DataSource {
    pub name: String,
    pub id: String,
    pub source_type: DataSourceType,
    pub details: DataSourceDetails,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AwsAthenaALBLog {
    pub region: String,
    pub catalog: String,
    pub workgroup: String,
    pub database: String,
    pub table: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataSourceDetails {
    AwsAthenaALBLog(AwsAthenaALBLog),
}

impl Default for DataSourceDetails {
    fn default() -> Self {
        DataSourceDetails::AwsAthenaALBLog(AwsAthenaALBLog {
            region: "us-east-1".to_string(),
            catalog: "AwsDataCatalog".to_string(),
            workgroup: "primary".to_string(),
            database: "default".to_string(),
            table: "<table>".to_string(),
        })
    }
}

#[derive(Debug)]
pub enum ConfigError {
    FileNotFound,
    NoDatasourceFound,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::FileNotFound => write!(
                f,
                "No configuration file found. Please use fivexx configure"
            ),
            ConfigError::NoDatasourceFound => write!(
                f,
                "No data sources found in the configuration. Please use fivexx configure"
            ),
        }
    }
}

impl std::error::Error for ConfigError {}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct Config {
    pub data_sources: Vec<DataSource>,
    pub default_domain: Option<String>,
}

impl Config {
    pub fn load() -> Result<Self, ConfigError> {
        let config =
            confy::load::<Config>("fivexx", "config").map_err(|_| ConfigError::FileNotFound)?;

        if config.data_sources.is_empty() {
            return Err(ConfigError::NoDatasourceFound);
        }

        Ok(config)
    }

    pub fn file_path() -> Result<String, ConfigError> {
        confy::get_configuration_file_path("fivexx", "config")
            .map(|s| s.display().to_string())
            .map_err(|_| ConfigError::FileNotFound)
    }

    pub fn default_domain(&self) -> Option<&String> {
        self.default_domain.as_ref()
    }

    pub fn data_sources(&self) -> &Vec<DataSource> {
        &self.data_sources
    }
}
