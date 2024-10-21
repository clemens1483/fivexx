use crate::{
    config::{DataSource, DataSourceType},
    parsers::QueryInput,
    query::{QueryError, QueryExecutionError, QueryResult},
};

use aws_athena_adapter::AwsAthenaAdapter;

mod aws_athena_adapter;

pub trait QueryAdapter<'a> {
    fn new(data_source: &'a DataSource) -> Self;

    fn build_query(&self, input: &'a QueryInput) -> Result<String, QueryError>;

    async fn execute_query(&self, query: &str) -> Result<QueryResult, QueryExecutionError>;
}

pub struct AdapterFactory;

impl AdapterFactory {
    pub fn new() -> Self {
        Self {}
    }

    pub fn create_adapter<'a>(&self, data_source: &'a DataSource) -> Box<impl QueryAdapter<'a>> {
        match data_source.source_type {
            DataSourceType::AwsAthenaALBLog => Box::new(AwsAthenaAdapter::new(&data_source)),
        }
    }
}
