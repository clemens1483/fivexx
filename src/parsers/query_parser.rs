use std::collections::HashMap;

use chrono::NaiveDateTime;

use crate::config::DataSource;

use super::{DatasetParser, DateTimeParser, Parser};

use std::fmt;

#[derive(Debug)]
pub enum QueryParserError {
    InvalidSelect(String),
    InvalidFrom(String),
    InvalidWhere(String),
    InvalidTime(String),
}

impl fmt::Display for QueryParserError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryParserError::InvalidSelect(msg) => write!(f, "Invalid SELECT: {}", msg),
            QueryParserError::InvalidFrom(msg) => write!(f, "Invalid FROM: {}", msg),
            QueryParserError::InvalidWhere(msg) => write!(f, "Invalid WHERE: {}", msg),
            QueryParserError::InvalidTime(msg) => write!(f, "Invalid time: {}", msg),
        }
    }
}

impl std::error::Error for QueryParserError {}

type Column = String;

#[derive(Debug, PartialEq)]
pub enum Select {
    All,
    Column(Column),
    Count(Option<Column>),
    Average(Column),
}

#[derive(Debug, PartialEq)]
pub enum Where {
    Equals(Column, String),
    NotEquals(Column, String),
    In(Column, Vec<String>),
    GreaterThan(Column, String),
    LessThan(Column, String),
    GreaterThanOrEqual(Column, String),
    LessThanOrEqual(Column, String),
    Like(Column, String),
}

#[derive(Debug, PartialEq)]
pub struct Facet(pub String);

#[derive(Debug, PartialEq)]
pub struct QueryInput {
    pub select: Vec<Select>,
    pub conditions: Vec<Where>,
    pub facet: Vec<Facet>,
    pub since: Option<NaiveDateTime>,
    pub until: Option<NaiveDateTime>,
}

pub struct QueryParser;

type QueryParserOutput = (QueryInput, Vec<DataSource>);

impl QueryParser {
    pub fn parse(query: &str) -> Result<QueryParserOutput, QueryParserError> {
        let mut parts = query.split_whitespace();

        let mut query_by_keyword: HashMap<&str, String> = HashMap::new();

        let mut input = QueryInput {
            select: Vec::new(),
            conditions: Vec::new(),
            facet: Vec::new(),
            since: None,
            until: None,
        };

        let mut data_sources: Vec<DataSource> = Vec::new();

        let mut key: &str = "BAD";

        while let Some(token) = parts.next() {
            match token.to_uppercase().as_str() {
                "SELECT" => {
                    key = "SELECT";
                }
                "FROM" => {
                    key = "FROM";
                }
                "WHERE" => {
                    key = "WHERE";
                }
                "FACET" => {
                    key = "FACET";
                }
                "SINCE" => {
                    key = "SINCE";
                }
                "UNTIL" => {
                    key = "UNTIL";
                }
                _ => {
                    query_by_keyword.entry(key).or_insert(String::new());

                    query_by_keyword
                        .entry(key)
                        .and_modify(|v| v.push_str(format!(" {}", token).as_str()));
                }
            }
        }

        Self::handle_select(
            query_by_keyword
                .get("SELECT")
                .ok_or(QueryParserError::InvalidSelect(
                    "SELECT clause missing".to_string(),
                ))?,
            &mut input,
        )?;

        Self::handle_from(
            query_by_keyword
                .get("FROM")
                .ok_or(QueryParserError::InvalidFrom(
                    "FROM clause missing".to_string(),
                ))?,
            &mut data_sources,
        )?;

        if let Some(where_clause) = query_by_keyword.get("WHERE") {
            Self::handle_conditions(where_clause, &mut input)?;
        }

        if let Some(facet_clause) = query_by_keyword.get("FACET") {
            Self::handle_facet(facet_clause, &mut input)?;
        }

        if let Some(since_clause) = query_by_keyword.get("SINCE") {
            Self::handle_time(since_clause, &mut input)?;
        }

        if let Some(until_clause) = query_by_keyword.get("UNTIL") {
            Self::handle_time(until_clause, &mut input)?;
        }

        Ok((input, data_sources))
    }

    fn handle_select(select_clause: &str, input: &mut QueryInput) -> Result<(), QueryParserError> {
        let select = select_clause
            .split(',')
            .map(|item| {
                let item = item.trim();
                if item == "*" {
                    Select::All
                } else if item.to_uppercase().starts_with("COUNT(") {
                    let inner = item[6..item.len() - 1].trim();
                    if inner == "*" || inner.is_empty() {
                        Select::Count(None)
                    } else {
                        Select::Count(Some(inner.to_string()))
                    }
                } else if item.to_uppercase().starts_with("AVG(") {
                    let inner = item[4..item.len() - 1].trim();
                    Select::Average(inner.to_string())
                } else {
                    Select::Column(item.to_string())
                }
            })
            .collect::<Vec<Select>>();

        if select.is_empty() {
            return Err(QueryParserError::InvalidSelect(
                "Invalid select".to_string(),
            ));
        }

        input.select = select;

        Ok(())
    }

    fn handle_from(
        from_clause: &str,
        data_sources: &mut Vec<DataSource>,
    ) -> Result<(), QueryParserError> {
        let parsed = DatasetParser::from_str(from_clause)
            .map_err(|err| QueryParserError::InvalidFrom(err.to_string()))?;

        if parsed.is_empty() {
            return Err(QueryParserError::InvalidFrom(
                "No valid data source ids".to_string(),
            ));
        }

        data_sources.extend(parsed);

        Ok(())
    }

    fn handle_conditions(
        conditions_str: &str,
        input: &mut QueryInput,
    ) -> Result<(), QueryParserError> {
        let mut conditions: Vec<Where> = Vec::new();

        for condition_str in conditions_str.split("AND").map(str::trim) {
            let parts: Vec<&str> = condition_str
                .splitn(3, |c: char| c.is_whitespace())
                .collect();
            if parts.len() != 3 {
                return Err(QueryParserError::InvalidWhere(condition_str.to_string()));
            }

            let field: String = parts[0].into();
            let operator = parts[1];
            let value_str: String = parts[2].into();

            match operator {
                "IN" => {
                    let values = value_str
                        .trim_start_matches('(')
                        .trim_end_matches(')')
                        .split(',')
                        .map(|s| s.trim().to_string())
                        .collect();
                    conditions.push(Where::In(field, values));
                }
                "=" => {
                    conditions.push(Where::Equals(field, value_str));
                }
                "!=" => {
                    conditions.push(Where::NotEquals(field, value_str));
                }
                ">" => {
                    conditions.push(Where::GreaterThan(field, value_str));
                }
                "<" => {
                    conditions.push(Where::LessThan(field, value_str));
                }
                ">=" => {
                    conditions.push(Where::GreaterThanOrEqual(field, value_str));
                }
                "<=" => {
                    conditions.push(Where::LessThanOrEqual(field, value_str));
                }
                "LIKE" => {
                    conditions.push(Where::Like(field, value_str));
                }
                _ => {
                    return Err(QueryParserError::InvalidWhere(format!(
                        "Unsupported operator in condition: {}",
                        condition_str
                    )));
                }
            }
        }

        input.conditions = conditions;

        Ok(())
    }

    fn handle_facet(facet_str: &str, input: &mut QueryInput) -> Result<(), QueryParserError> {
        let facet_str = facet_str.trim();

        let result = facet_str
            .split(',')
            .filter(|s| !s.is_empty())
            .map(|s| Facet(s.trim().to_string()))
            .collect::<Vec<Facet>>();

        input.facet = result;

        Ok(())
    }

    fn handle_time(since_str: &str, input: &mut QueryInput) -> Result<(), QueryParserError> {
        match DateTimeParser::from_str(since_str.trim()) {
            Ok(dt) => {
                input.since = Some(dt);
                Ok(())
            }
            Err(e) => Err(QueryParserError::InvalidTime(e.to_string())),
        }
    }
}
