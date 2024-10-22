use std::collections::HashMap;

use chrono::DateTime;
use reqwest::Client;
use serde_json::{json, Value};

use crate::query::{QueryExecutionError, QueryResult};

const URL: &str = "https://api.newrelic.com/graphql";

pub struct QueryExecutor<'a> {
    client: Client,
    api_key: &'a str,
    account_id: &'a str,
}

impl<'a> QueryExecutor<'a> {
    pub fn new(api_key: &'a str, account_id: &'a str) -> QueryExecutor<'a> {
        QueryExecutor {
            client: Client::new(),
            api_key,
            account_id,
        }
    }

    pub async fn execute_query(&self, query: &str) -> Result<QueryResult, QueryExecutionError> {
        let graphql_query = format!(
            r#"
            query {{
                actor {{
                    account(id: {}) {{
                        nrql(query: "{}", timeout: 60) {{
                            results
                        }}
                    }}
                }}
            }}
            "#,
            self.account_id, query
        );

        let response = self
            .client
            .post(URL)
            .header("API-Key", self.api_key)
            .header("Content-Type", "application/json")
            .json(&json!({
                "query": graphql_query
            }))
            .send()
            .await
            .map_err(|e| QueryExecutionError::ClientError(e.to_string()))?;

        let json_response: Value = response
            .json()
            .await
            .map_err(|e| QueryExecutionError::ParseError(e.to_string()))?;

        println!("JSON: {:?}", json_response);

        Self::transform_json_response(json_response)
    }

    fn transform_json_response(json_response: Value) -> Result<QueryResult, QueryExecutionError> {
        let results = json_response["data"]["actor"]["account"]["nrql"]["results"]
            .as_array()
            .ok_or(QueryExecutionError::ParseError(
                "Invalid response".to_string(),
            ))?;

        let query_result: Result<QueryResult, QueryExecutionError> = results
            .iter()
            .map(|result| {
                let mut row: HashMap<String, Value> = HashMap::new();

                if let Some(logtype) = result["logtype"].as_str() {
                    row.insert("logtype".to_string(), Value::String(logtype.to_string()));
                }

                if let Some(message) = result["message"].as_str() {
                    row.insert("message".to_string(), Value::String(message.to_string()));
                }

                if let Some(timestamp) = result["timestamp"].as_f64() {
                    let naive_datetime = DateTime::from_timestamp_millis(timestamp as i64)
                        .ok_or("Invalid timestamp")
                        .map_err(|e| QueryExecutionError::ParseError(e.to_string()))?;

                    row.insert(
                        "time".to_string(),
                        Value::String(naive_datetime.to_string()),
                    );
                }

                Ok(row)
            })
            .collect();

        query_result
    }
}
