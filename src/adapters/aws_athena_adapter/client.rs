use std::ops::Deref;

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_athena::{
    operation::get_query_execution::GetQueryExecutionError, Client as AthenaClient,
};

use crate::query::{QueryExecutionError, QueryResult};

use super::{
    query_executor::QueryExecutor, query_result_fetcher::QueryResultFetcher,
    query_result_processor::QueryResultProcessor, query_status_checker::QueryStatusChecker,
};

// Allow AWS SDK errors to be converted into QueryExecutionErrors
impl
    From<
        aws_smithy_runtime_api::client::result::SdkError<
            GetQueryExecutionError,
            aws_smithy_runtime_api::http::Response,
        >,
    > for QueryExecutionError
{
    fn from(
        e: aws_smithy_runtime_api::client::result::SdkError<
            GetQueryExecutionError,
            aws_smithy_runtime_api::http::Response,
        >,
    ) -> Self {
        QueryExecutionError::ClientError(e.to_string())
    }
}

pub struct Client<'a> {
    pub client: AthenaClient,
    pub catalog: &'a str,
    pub workgroup: &'a str,
    pub database: &'a str,
}

impl Deref for Client<'_> {
    type Target = AthenaClient;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl<'a> Client<'a> {
    pub async fn new(region: &str) -> Self {
        let region = aws_config::Region::new(region.to_owned());

        let region_provider = RegionProviderChain::first_try(region).or_default_provider();

        let shared_config = aws_config::from_env().region(region_provider).load().await;

        let client = AthenaClient::new(&shared_config);

        Self {
            client,
            catalog: "AwsDataCatalog",
            workgroup: "primary",
            database: "default",
        }
    }

    pub fn database(mut self, db_name: &'a str) -> Self {
        self.database = db_name;
        self
    }

    pub fn catalog(mut self, catalog_name: &'a str) -> Self {
        self.catalog = catalog_name;
        self
    }

    pub fn workgroup(mut self, workgroup_name: &'a str) -> Self {
        self.workgroup = workgroup_name;
        self
    }

    pub async fn execute_query(&self, query: &str) -> Result<QueryResult, QueryExecutionError> {
        let executor = QueryExecutor::new(&self);
        let query_execution_id = executor.start_query_execution(query).await?;

        let status_checker = QueryStatusChecker::new(&self);
        status_checker
            .poll_query_status(&query_execution_id)
            .await?;

        let result_fetcher = QueryResultFetcher::new(&self);
        let result = result_fetcher
            .get_query_results(&query_execution_id)
            .await?;

        let processor = QueryResultProcessor::new();
        Ok(processor.process(result))
    }
}
