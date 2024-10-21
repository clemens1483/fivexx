use std::{collections::HashMap, fmt};

#[derive(Debug)]
pub enum QueryError {
    UnknownColumn(String),
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryError::UnknownColumn(msg) => write!(f, "Unknown column: {}", msg),
        }
    }
}

impl std::error::Error for QueryError {}

#[derive(Debug)]
pub enum QueryExecutionError {
    QueryTimeout,
    BadQueryStatus(String),
    NoData,
    ClientError(String),
}

impl fmt::Display for QueryExecutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryExecutionError::QueryTimeout => write!(f, "Query is taking too long. Aborting!"),
            QueryExecutionError::BadQueryStatus(msg) => write!(f, "Bad query status: {}", msg),
            QueryExecutionError::NoData => write!(f, "No data found"),
            QueryExecutionError::ClientError(msg) => write!(f, "Client error: {}", msg),
        }
    }
}

impl std::error::Error for QueryExecutionError {}

pub type QueryResult = Vec<HashMap<String, String>>;
