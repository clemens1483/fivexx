mod csv_formatter;
mod json_formatter;

pub use csv_formatter::CSVFormatter;
pub use json_formatter::JSONFormatter;

use crate::query::QueryResult;

pub trait Formatter {
    type Output;

    fn format(&self, data: QueryResult) -> Self::Output;
}
