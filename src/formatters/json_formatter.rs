use crate::query::QueryResult;

use super::Formatter;

pub struct JSONFormatter();

impl Formatter for JSONFormatter {
    type Output = String;

    fn format(&self, data: QueryResult) -> String {
        serde_json::to_string(&data).unwrap()
    }
}
