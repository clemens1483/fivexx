mod client;
mod query_builder;
mod query_executor;
mod query_result_fetcher;
mod query_result_processor;
mod query_status_checker;

use crate::adapters::aws_athena_adapter::{client::Client, query_builder::QueryBuilder};

use crate::config::{AwsAthenaALBLog, DataSourceDetails};
use crate::parsers::QueryInput;
use crate::{
    adapters::QueryAdapter,
    config::DataSource,
    query::{QueryError, QueryExecutionError, QueryResult},
};

use async_trait::async_trait;

pub struct AwsAthenaAdapter<'a> {
    details: &'a AwsAthenaALBLog,
}

impl<'a> AwsAthenaAdapter<'a> {
    pub fn new(data_source: &'a DataSource) -> Self {
        match &data_source.details {
            DataSourceDetails::AwsAthenaALBLog(details) => Self { details },
            _ => panic!("AwsAthenaAdapter requires an AwsAthenaALBLog data source"),
        }
    }
}

#[async_trait]
impl<'a> QueryAdapter<'a> for AwsAthenaAdapter<'a> {
    async fn execute_query(&self, query: &str) -> Result<QueryResult, QueryExecutionError> {
        let client = Client::new(&self.details.region).await;

        let client = client
            .catalog(&self.details.catalog)
            .database(&self.details.database)
            .workgroup(&self.details.workgroup);

        client.execute_query(query).await
    }

    fn build_query(&self, input: &'a QueryInput) -> Result<String, QueryError> {
        let mut query = QueryBuilder::new(self.details.table.as_str());

        let query_string = query
            .select(&input.select)?
            .conditions(&input.conditions)?
            .facet(&input.facet)?
            .since(input.since)?
            .until(input.until)?
            .build_query();

        Ok(query_string)
    }
}
