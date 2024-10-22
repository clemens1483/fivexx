use std::collections::HashMap;

use aws_sdk_athena::types::Row;
use serde_json::Value;

use crate::query::QueryResult;

pub struct QueryResultProcessor;

impl QueryResultProcessor {
    pub fn new() -> Self {
        Self {}
    }

    pub fn process(&self, data: Vec<Row>) -> QueryResult {
        let mut result: QueryResult = vec![];
        let mut headers: Vec<String> = vec![];

        for (index, row) in data.into_iter().enumerate() {
            let row_data = row.data.unwrap();

            if index == 0 {
                // Headers
                for datum in row_data {
                    let header = datum.var_char_value.unwrap();
                    headers.push(header);
                }
            } else {
                // Data
                let mut row_hash: HashMap<String, Value> = HashMap::new();

                for (index, value) in row_data.iter().enumerate() {
                    // TODO: clone is not great
                    let key = headers.get(index).unwrap().clone();
                    let value = value.var_char_value.clone().unwrap();

                    row_hash.insert(key, Value::String(value));
                }

                result.push(row_hash);
            }
        }

        result
    }
}
