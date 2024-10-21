use aws_sdk_athena::{
    operation::get_query_results::builders::GetQueryResultsFluentBuilder, types::Row,
};

use crate::query::QueryExecutionError;

use super::client::Client;

pub struct QueryResultFetcher<'a> {
    client: &'a Client<'a>,
}

impl<'a> QueryResultFetcher<'a> {
    pub fn new(client: &'a Client) -> Self {
        Self { client }
    }

    pub async fn get_query_results(
        &self,
        query_execution_id: &str,
    ) -> Result<Vec<Row>, QueryExecutionError> {
        let mut rows: Vec<Row> = Vec::new();

        // Helper variable to not lose the next_token value
        let mut temp_token: String;

        let mut next_token: Option<&str> = None;

        let mut page: u16 = 1;

        let mut ran_first_time: bool = false;

        let max_iterations: u16 = 10;

        while page < max_iterations && (!ran_first_time || next_token.is_some()) {
            println!("Fetching page {}...", page);

            let result_request = self.build_request(query_execution_id, next_token);

            match result_request.send().await {
                Ok(output) => {
                    let Some(mut rows_data) =
                        output.result_set.and_then(|result_set| result_set.rows)
                    else {
                        return Err(QueryExecutionError::NoData);
                    };

                    rows.append(&mut rows_data);

                    match output.next_token {
                        Some(t) => {
                            // t is only valid in this scope
                            // clone on heap and assign to next_token
                            temp_token = t.to_string();
                            next_token = Some(&temp_token);
                        }
                        None => {
                            next_token = None;
                        }
                    }
                }
                Err(error) => {
                    return Err(QueryExecutionError::ClientError(error.to_string()));
                }
            }

            ran_first_time = true;
            page += 1;
        }

        Ok(rows)
    }

    fn build_request(
        &self,
        query_execution_id: &str,
        next_token: Option<&str>,
    ) -> GetQueryResultsFluentBuilder {
        let builder = self
            .client
            .get_query_results()
            .query_execution_id(query_execution_id);

        match next_token {
            Some(token) => builder.next_token(token),
            None => builder,
        }
    }
}
