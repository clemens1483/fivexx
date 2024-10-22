use query_builder::QueryBuilder;
use query_executor::QueryExecutor;

use crate::{
    config::{DataSource, DataSourceDetails, NewRelicLog},
    parsers::QueryInput,
    query::{QueryError, QueryExecutionError, QueryResult},
};

use super::QueryAdapter;

use async_trait::async_trait;

mod query_builder;
mod query_executor;

pub struct NewRelicLogAdapter<'a> {
    details: &'a NewRelicLog,
}

impl<'a> NewRelicLogAdapter<'a> {
    pub fn new(data_source: &'a DataSource) -> Self {
        match &data_source.details {
            DataSourceDetails::NewRelicLog(details) => Self { details },
            _ => panic!("NewRelicLogAdapter requires a NewRelicLog data source"),
        }
    }
}

#[async_trait]
impl<'a> QueryAdapter<'a> for NewRelicLogAdapter<'a> {
    async fn execute_query(&self, query: &str) -> Result<QueryResult, QueryExecutionError> {
        let executor = QueryExecutor::new(&self.details.api_key, &self.details.account_id);
        executor.execute_query(query).await
    }

    fn build_query(&self, input: &'a QueryInput) -> Result<String, QueryError> {
        let mut query = QueryBuilder::new(self.details.table.as_str());

        let query_string = query
            .select(&input.select)?
            .conditions(&input.conditions)?
            .since(input.since)?
            .until(input.until)?
            .build_query();

        Ok(query_string)
    }
}
