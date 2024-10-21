use aws_sdk_athena::types::QueryExecutionContext;

use crate::query::QueryExecutionError;

use super::client::Client;

pub struct QueryExecutor<'a> {
    client: &'a Client<'a>,
}

impl<'a> QueryExecutor<'a> {
    pub fn new(client: &'a Client) -> Self {
        Self { client }
    }

    pub async fn start_query_execution(&self, query: &str) -> Result<String, QueryExecutionError> {
        let query_execution_context = QueryExecutionContext::builder()
            .catalog(self.client.catalog)
            .database(self.client.database)
            .build();

        let request = self
            .client
            .start_query_execution()
            .query_string(query)
            .query_execution_context(query_execution_context)
            .work_group(self.client.workgroup);

        match request.send().await {
            Ok(output) => {
                let query_execution_id =
                    output
                        .query_execution_id
                        .ok_or(QueryExecutionError::ClientError(String::from(
                            "Query Execution ID not found",
                        )))?;

                return Ok(query_execution_id);
            }
            Err(error) => {
                return Err(QueryExecutionError::ClientError(error.to_string()));
            }
        }
    }
}
