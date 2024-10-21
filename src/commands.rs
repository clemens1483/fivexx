use chrono::NaiveDateTime;
use clap::{Parser, Subcommand};

mod configure;
mod query;

pub use crate::commands::configure::configure;
pub use crate::commands::query::query;

use crate::{
    config::DataSource,
    parsers::{DatasetParser, DateTimeParser, DomainParser, Parser as _},
};

#[derive(Subcommand, Debug)]
pub enum Commands {
    Query(QueryArgs),
    Configure(ConfigureArgs),
}

impl Default for Commands {
    fn default() -> Self {
        Self::Query(QueryArgs::default())
    }
}

#[derive(Parser, Debug, Default)]
pub struct ConfigureArgs {
    /// Get the config file path
    #[arg(long)]
    pub get_path: bool,

    /// Add to the config file
    #[arg(long)]
    pub update: bool,
}

#[derive(Parser, Debug, Default)]
pub struct QueryArgs {
    /// List of ELB Status Codes, e.g. -c=200,300 or -c=5xx
    #[arg(short, long, value_delimiter = ',')]
    code: Vec<String>,

    /// List of domain names, e.g. -d=example.com OR subdomain names -d=sub1,sub2 (must have configured a default domain)
    #[arg(short, long, value_delimiter = ',', value_parser = |s: &str| DomainParser::from_str(s))]
    domain: Vec<String>,

    /// List of HTTP Methods, e.g. -m=GET,POST
    #[arg(short, long, value_delimiter = ',')]
    method: Vec<String>,

    /// Request URL using LIKE syntax - e.g. -r="https://example.com:443/users" or -r="%user%"
    #[arg(short, long)]
    request_url: Option<String>,

    // TODO: map to ip
    /// List of Server Names, e.g. -t=server1,server2
    // #[arg(short, long)]
    // target: Option<String>,

    /// DataSource IDs
    #[arg(short = 'i', long = "data-source-ids", required_unless_present = "raw", value_delimiter = ',', value_parser = |s: &str| DatasetParser::from_id(s).ok_or("DataSource not found"))]
    data_sources: Vec<DataSource>,

    /// Since - can be a Date -s=2022-01-01 or (partial) DateTime -s="2022-01-01 00:00" or Duration -s="1 HOUR AGO"
    #[arg(long, short = 's', value_parser = |s: &str| DateTimeParser::from_str(s))]
    since: Option<NaiveDateTime>,

    /// Until - can be a Date -u=2022-01-01 or (partial) DateTime -u="2022-01-01 00:00" or Duration -u="1 HOUR AGO"
    #[arg(long, short = 'u', value_parser = |s: &str| DateTimeParser::from_str(s))]
    until: Option<NaiveDateTime>,

    /// List of Count By Columns - e.g. --count-by=path,method --count-by=day
    #[arg(long, value_delimiter = ',')]
    count_by: Vec<String>,

    /// Raw query string - e.g. --raw="SELECT elb_status_code, COUNT(*) FROM data1, data2 SINCE 2 days ago GROUP BY elb_status_code"
    #[arg(long, required_unless_present = "data_sources")]
    raw: Option<String>,
}
