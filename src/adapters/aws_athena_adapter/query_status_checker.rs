use aws_sdk_athena::{
    operation::get_query_execution::builders::GetQueryExecutionFluentBuilder,
    types::QueryExecutionState,
};

use crate::query::QueryExecutionError;

use super::client::Client;

pub struct QueryStatusChecker<'a> {
    client: &'a Client<'a>,
}

impl<'a> QueryStatusChecker<'a> {
    pub fn new(client: &'a Client) -> Self {
        Self { client }
    }

    pub async fn poll_query_status(
        &self,
        query_execution_id: &str,
    ) -> Result<(), QueryExecutionError> {
        let max_retries: u32 = 10;
        // possibly wait longer if status is queued
        let wait_time_in_secs: u64 = 5;

        let mut i: u32 = 0;

        tokio::time::sleep(std::time::Duration::from_secs(wait_time_in_secs)).await;

        while i < max_retries {
            let response = self.build_request(&query_execution_id).send().await?;

            let Some(state) = response
                .query_execution
                .and_then(|qe| qe.status)
                .and_then(|status| status.state)
            else {
                return Err(QueryExecutionError::BadQueryStatus("Not Found".to_string()));
            };

            if [QueryExecutionState::Cancelled, QueryExecutionState::Failed].contains(&state) {
                return Err(QueryExecutionError::BadQueryStatus(state.to_string()));
            }

            if state == QueryExecutionState::Succeeded {
                break;
            }

            if i == max_retries {
                return Err(QueryExecutionError::QueryTimeout);
            }

            println!(
                "Query state: {:?}. Retrying in {} seconds...",
                state, wait_time_in_secs
            );

            tokio::time::sleep(std::time::Duration::from_secs(wait_time_in_secs)).await;

            i += 1;
        }

        Ok(())
    }

    fn build_request(&self, query_execution_id: &str) -> GetQueryExecutionFluentBuilder {
        self.client
            .get_query_execution()
            .query_execution_id(query_execution_id)
    }
}
