use std::collections::HashMap;

use chrono::{NaiveDateTime, TimeDelta};

use crate::config::DataSource;

use super::{DatasetParser, DateTimeParser, DurationParser, Parser};

use std::fmt;

#[derive(Debug)]
pub enum QueryParserError {
    InvalidSelect(String),
    InvalidFrom(String),
    InvalidWhere(String),
    InvalidTime(String),
    InvalidCorrelate(String),
}

impl fmt::Display for QueryParserError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryParserError::InvalidSelect(msg) => write!(f, "Invalid SELECT: {}", msg),
            QueryParserError::InvalidFrom(msg) => write!(f, "Invalid FROM: {}", msg),
            QueryParserError::InvalidWhere(msg) => write!(f, "Invalid WHERE: {}", msg),
            QueryParserError::InvalidTime(msg) => write!(f, "Invalid time: {}", msg),
            QueryParserError::InvalidCorrelate(msg) => write!(f, "Invalid correlate: {}", msg),
        }
    }
}

impl std::error::Error for QueryParserError {}

type Column = String;

#[derive(Debug, PartialEq, Clone)]
pub enum Select {
    All,
    Column(Column),
    Count(Option<Column>),
    Average(Column),
}

#[derive(Debug, PartialEq, Clone)]
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

#[derive(Debug, PartialEq, Clone)]
pub struct Facet(pub String);

#[derive(Debug, PartialEq, Clone)]
pub struct QueryInput {
    pub select: Vec<Select>,
    pub conditions: Vec<Where>,
    pub facet: Vec<Facet>,
    pub since: Option<NaiveDateTime>,
    pub until: Option<NaiveDateTime>,
    pub correlate: Option<Correlate>,
}

impl Default for QueryInput {
    fn default() -> Self {
        Self {
            select: Vec::new(),
            conditions: Vec::new(),
            facet: Vec::new(),
            since: None,
            until: None,
            correlate: None,
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Correlate {
    pub data_source: DataSource,
    pub query_input: Box<QueryInput>,
    pub dependent_conditions: Vec<CorrelateCondition>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum CorrelateCondition {
    Is {
        parent: Column,
        child: Column,
    },
    Within {
        parent: Column,
        child: Column,
        delta: TimeDelta,
    },
}

pub struct QueryParser;

type QueryParserOutput = (QueryInput, Vec<DataSource>);

impl QueryParser {
    pub fn parse(query: &str) -> Result<QueryParserOutput, QueryParserError> {
        let keywords = [
            "SELECT",
            "FROM",
            "WHERE",
            "FACET",
            "SINCE",
            "UNTIL",
            "CORRELATE",
        ];
        let mut query_by_keyword: HashMap<&str, String> = HashMap::new();
        let mut current_key = "";

        for token in query.split_whitespace() {
            if keywords.contains(&token.to_uppercase().as_str()) {
                current_key = token;
            } else {
                query_by_keyword
                    .entry(current_key)
                    .or_default()
                    .push_str(&format!(" {}", token));
            }
        }

        let mut input = QueryInput::default();
        let mut data_sources = Vec::new();

        let handlers: Vec<(
            &str,
            fn(&str, &mut QueryInput) -> Result<(), QueryParserError>,
        )> = vec![
            ("SELECT", Self::handle_select),
            ("WHERE", Self::handle_conditions),
            ("FACET", Self::handle_facet),
            ("SINCE", Self::handle_time),
            ("UNTIL", Self::handle_time),
            ("CORRELATE", Self::handle_correlate),
        ];

        for (key, handler) in handlers {
            if let Some(clause) = query_by_keyword.get(key) {
                handler(clause.trim(), &mut input)?;
            }
        }

        Self::handle_from(
            query_by_keyword
                .get("FROM")
                .ok_or(QueryParserError::InvalidFrom(
                    "FROM clause missing".to_string(),
                ))?,
            &mut data_sources,
        )?;

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

        input.select.extend(select);

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

        input.conditions.extend(conditions);

        Ok(())
    }

    fn handle_facet(facet_str: &str, input: &mut QueryInput) -> Result<(), QueryParserError> {
        let facet_str = facet_str.trim();

        let facet = facet_str
            .split(',')
            .filter(|s| !s.is_empty())
            .map(|s| Facet(s.trim().to_string()))
            .collect::<Vec<Facet>>();

        input.facet.extend(facet);

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

    fn handle_correlate(
        correlate_str: &str,
        input: &mut QueryInput,
    ) -> Result<(), QueryParserError> {
        let parts: Vec<&str> = correlate_str.splitn(2, "ON").collect();
        if parts.len() != 2 {
            return Err(QueryParserError::InvalidCorrelate(
                "Missing ON clause".to_string(),
            ));
        }

        let data_source_id = parts[0].trim().trim_start_matches("WITH ").trim();
        let conditions_str = parts[1].trim();

        let data_source = DatasetParser::from_id(data_source_id)
            .ok_or(QueryParserError::InvalidCorrelate(
                "Invalid data source id".to_string(),
            ))
            .unwrap();

        let mut query_input = QueryInput::default();
        let mut dependent_conditions: Vec<CorrelateCondition> = Vec::new();

        for condition in conditions_str.split("AND").map(str::trim) {
            if condition.contains("WITHIN") {
                let parts: Vec<&str> = condition.split("WITHIN").collect();
                if parts.len() != 2 {
                    return Err(QueryParserError::InvalidCorrelate(
                        "Missing WITHIN clause".to_string(),
                    ));
                }

                let field0_str = parts[0].trim();
                let within_str = parts[1].trim();
                let within_parts: Vec<&str> = within_str.splitn(2, "OF").collect();
                if within_parts.len() != 2 {
                    return Err(QueryParserError::InvalidCorrelate(
                        "Missing OF clause".to_string(),
                    ));
                }

                let time_str = within_parts[0].trim();
                let field1_str = within_parts[1].trim();

                let duration = DurationParser::from_str(time_str).map_err(|e| {
                    QueryParserError::InvalidCorrelate(format!("Invalid duration: {}", e))
                })?;

                let [f0_id, f0_col] = Self::handle_correlate_field(field0_str)?;
                let [_f1_id, f1_col] = Self::handle_correlate_field(field1_str)?;

                dependent_conditions.push(if f0_id == data_source.id {
                    CorrelateCondition::Within {
                        parent: f1_col.into(),
                        child: f0_col.into(),
                        delta: duration,
                    }
                } else {
                    CorrelateCondition::Within {
                        parent: f0_col.into(),
                        child: f1_col.into(),
                        delta: duration,
                    }
                });
            } else if condition.contains("IS") {
                let parts = condition.split("IS").map(str::trim).collect::<Vec<&str>>();

                if parts.len() != 2 {
                    return Err(QueryParserError::InvalidCorrelate(
                        "Missing IS clause".to_string(),
                    ));
                }

                let [f0_id, f0_col] = Self::handle_correlate_field(parts[0])?;
                let [_f1_id, f1_col] = Self::handle_correlate_field(parts[1])?;

                dependent_conditions.push(if f0_id == data_source.id {
                    CorrelateCondition::Is {
                        parent: f1_col.into(),
                        child: f0_col.into(),
                    }
                } else {
                    CorrelateCondition::Is {
                        parent: f0_col.into(),
                        child: f1_col.into(),
                    }
                });
            } else {
                Self::handle_conditions(condition, &mut query_input)?;
            }
        }

        let correlate = Correlate {
            data_source,
            query_input: Box::new(query_input),
            dependent_conditions,
        };

        input.correlate = Some(correlate);
        Ok(())
    }

    fn handle_correlate_field(field_str: &str) -> Result<[&str; 2], QueryParserError> {
        let field_str = field_str.trim();

        let id_and_column = field_str.split('.').collect::<Vec<&str>>();

        if id_and_column.len() != 2 {
            return Err(QueryParserError::InvalidCorrelate(
                "Invalid field".to_string(),
            ));
        }

        let data_source_id = id_and_column[0];
        let column = id_and_column[1];

        DatasetParser::from_id(data_source_id)
            .ok_or(QueryParserError::InvalidCorrelate(
                "Invalid data source id".to_string(),
            ))
            .unwrap();

        Ok([data_source_id, column])
    }
}
