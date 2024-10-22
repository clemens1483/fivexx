use crate::{
    config::{DataSource, DataSourceType},
    parsers::QueryInput,
    query::{QueryError, QueryExecutionError, QueryResult},
};

use aws_athena_adapter::AwsAthenaAdapter;
use new_relic_log_adapter::NewRelicLogAdapter;

use async_trait::async_trait;

mod aws_athena_adapter;
mod new_relic_log_adapter;

#[async_trait]
pub trait QueryAdapter<'a> {
    fn build_query(&self, input: &'a QueryInput) -> Result<String, QueryError>;

    async fn execute_query(&self, query: &str) -> Result<QueryResult, QueryExecutionError>;
}

pub struct AdapterFactory;

impl AdapterFactory {
    pub fn new() -> Self {
        Self {}
    }

    pub fn create_adapter<'a>(
        &self,
        data_source: &'a DataSource,
    ) -> Box<dyn QueryAdapter<'a> + 'a> {
        match data_source.source_type {
            DataSourceType::AwsAthenaALBLog => Box::new(AwsAthenaAdapter::new(&data_source)),
            DataSourceType::NewRelicLog => Box::new(NewRelicLogAdapter::new(&data_source)),
        }
    }
}
