use std::error::Error;

use crate::query::QueryResult;

use super::Formatter;

const DEFAULT_DELIMITER: char = ',';

#[derive(Debug)]
pub struct CSVFormatter {
    delimiter: char,
}

impl Default for CSVFormatter {
    fn default() -> Self {
        CSVFormatter {
            delimiter: DEFAULT_DELIMITER,
        }
    }
}

impl CSVFormatter {
    pub fn new(delimiter: char) -> Self {
        CSVFormatter { delimiter }
    }

    fn escape_field(&self, field: &str) -> String {
        if field.contains(self.delimiter) || field.contains('\n') || field.contains('"') {
            format!("\"{}\"", field.replace('"', "\"\""))
        } else {
            field.to_string()
        }
    }
}

impl Formatter for CSVFormatter {
    type Output = Result<String, Box<dyn Error>>;

    fn format(&self, data: QueryResult) -> Self::Output {
        let mut output = String::new();

        if data.is_empty() {
            return Ok(output);
        }

        let first_row = &data[0];

        let headers: Vec<String> = first_row.keys().map(|key| key.to_string()).collect();

        output.push_str(&headers.join(&self.delimiter.to_string()));
        output.push('\n');

        for row in &data {
            let formatted_row = headers
                .iter()
                .map(|header| {
                    let value = row.get(header).unwrap();
                    self.escape_field(value)
                })
                .collect::<Vec<String>>()
                .join(&self.delimiter.to_string());

            output.push_str(&formatted_row);
            output.push('\n');
        }

        Ok(output)
    }
}
