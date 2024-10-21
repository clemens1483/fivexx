mod dataset_parser;
mod date_time_parser;
mod domain_parser;
mod query_parser;

pub use crate::parsers::dataset_parser::DatasetParser;
pub use crate::parsers::date_time_parser::DateTimeParser;
pub use crate::parsers::domain_parser::DomainParser;
pub use crate::parsers::query_parser::*;

pub trait Parser {
    type Output;

    fn from_str(input: &str) -> Result<Self::Output, &'static str>;
}
