use chrono::NaiveDateTime;

use crate::parsers::{Select, Where};
use crate::query::QueryError;
use std::str::FromStr;

const TIME_FORMAT: &str = "%Y-%m-%dT%H:%M:%SZ";

const NEW_RELIC_LOG_COLUMNS: [NewRelicLogColumn; 6] = [
    NewRelicLogColumn {
        name: "response",
        col_type: ColumnType::String,
    },
    NewRelicLogColumn {
        name: "logtype",
        col_type: ColumnType::String,
    },
    NewRelicLogColumn {
        name: "message",
        col_type: ColumnType::String,
    },
    NewRelicLogColumn {
        name: "timestamp",
        col_type: ColumnType::Integer,
    },
    NewRelicLogColumn {
        name: "clientip",
        col_type: ColumnType::String,
    },
    NewRelicLogColumn {
        name: "hostname",
        col_type: ColumnType::String,
    },
];

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ColumnType {
    String,
    Integer,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct NewRelicLogColumn {
    pub name: &'static str,
    pub col_type: ColumnType,
}

impl NewRelicLogColumn {
    pub fn parse_value(&self, value: &str) -> String {
        match self.col_type {
            ColumnType::String => format!("'{}'", value),
            ColumnType::Integer => value.to_string(),
        }
    }

    pub fn as_str(&self) -> &str {
        self.name
    }
}

impl FromStr for NewRelicLogColumn {
    type Err = QueryError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        NEW_RELIC_LOG_COLUMNS
            .iter()
            .find(|&col| col.name == s)
            .cloned()
            .ok_or_else(|| QueryError::UnknownColumn(s.to_string()))
    }
}

pub struct QueryBuilder<'a> {
    table: &'a str,
    where_clauses: Vec<String>,
    select_clauses: Vec<String>,
    time_clauses: Vec<String>,
}

impl<'a> QueryBuilder<'a> {
    pub fn new(table: &'a str) -> Self {
        QueryBuilder {
            table,
            where_clauses: vec![],
            select_clauses: vec![],
            time_clauses: vec![],
        }
    }

    pub fn select(&mut self, select_clause: &Vec<Select>) -> Result<&mut Self, QueryError> {
        if select_clause.is_empty() {
            return Ok(self);
        }

        for select in select_clause {
            match select {
                Select::All => self.select_clauses.push("*".to_string()),
                Select::Column(col_str) => {
                    let column = NewRelicLogColumn::from_str(col_str)?;
                    self.select_clauses.push(column.name.to_string());
                }
                Select::Count(col_str_opt) => {
                    if let Some(col_str) = col_str_opt {
                        let column = NewRelicLogColumn::from_str(col_str)?;
                        self.select_clauses.push(format!("count({})", column.name));
                    } else {
                        self.select_clauses.push("count(*)".to_string());
                    }
                }
                Select::Average(col_str) => {
                    let column = NewRelicLogColumn::from_str(col_str)?;
                    self.select_clauses
                        .push(format!("average({})", column.name));
                }
            }
        }

        Ok(self)
    }

    pub fn conditions(&mut self, where_clause: &Vec<Where>) -> Result<&mut Self, QueryError> {
        if where_clause.is_empty() {
            return Ok(self);
        }

        for condition in where_clause {
            match condition {
                Where::Equals(col, value) => {
                    let column = NewRelicLogColumn::from_str(col)?;
                    self.where_clauses.push(format!(
                        "{} = {}",
                        column.name,
                        column.parse_value(value)
                    ));
                }
                Where::NotEquals(col, value) => {
                    let column = NewRelicLogColumn::from_str(col)?;
                    self.where_clauses.push(format!(
                        "{} != {}",
                        column.name,
                        column.parse_value(value)
                    ));
                }
                Where::In(col, values) => {
                    let column = NewRelicLogColumn::from_str(col)?;
                    let formatted_values = values
                        .iter()
                        .map(|v| column.parse_value(v))
                        .collect::<Vec<_>>()
                        .join(", ");
                    self.where_clauses
                        .push(format!("{} IN ({})", column.name, formatted_values));
                }
                Where::Like(col, pattern) => {
                    let column = NewRelicLogColumn::from_str(col)?;
                    self.where_clauses
                        .push(format!("{} LIKE '{}'", column.name, pattern));
                }
                _ => panic!("Unsupported where clause: {:?}", condition),
            }
        }

        Ok(self)
    }

    pub fn since(&mut self, since: Option<NaiveDateTime>) -> Result<&mut Self, QueryError> {
        if let Some(since) = since {
            let col = NewRelicLogColumn::from_str("timestamp")?;

            self.where_clauses.push(format!(
                "{} >= {}",
                col.as_str(),
                since.and_utc().timestamp_millis()
            ));

            self.time_clauses
                .push(format!("SINCE '{}'", since.format(TIME_FORMAT)));
        }

        Ok(self)
    }

    pub fn until(&mut self, until: Option<NaiveDateTime>) -> Result<&mut Self, QueryError> {
        if let Some(until) = until {
            let col = NewRelicLogColumn::from_str("timestamp")?;

            self.where_clauses.push(format!(
                "{} <= {}",
                col.as_str(),
                until.and_utc().timestamp_millis()
            ));

            self.time_clauses
                .push(format!("UNTIL '{}'", until.format(TIME_FORMAT)));
        }

        Ok(self)
    }

    pub fn build_query(&self) -> String {
        let mut query = String::with_capacity(512);

        if self.select_clauses.is_empty() {
            query.push_str("SELECT *");
        } else {
            query.push_str(&format!("SELECT {}", self.select_clauses.join(", ")));
        }

        query.push_str(&format!(" FROM {}", self.table));

        if !self.where_clauses.is_empty() {
            query.push_str(&format!(" WHERE {}", self.where_clauses.join(" AND ")));
        }

        if !self.time_clauses.is_empty() {
            query.push_str(&format!(" {}", self.time_clauses.join(" ")));
        }

        query
    }
}
