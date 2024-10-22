use std::str::FromStr;

use chrono::NaiveDateTime;

use crate::{
    parsers::{Facet, Select, Where},
    query::QueryError,
};

const DAY_FORMAT: &str = "%Y/%m/%d";
const TIME_FORMAT: &str = "%Y-%m-%d-%H:%M:%S";

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ColumnType {
    String,
    Integer,
    DateTime,
}

const ATHENA_ALB_COLUMNS: [AthenaAlbColumn; 9] = [
    AthenaAlbColumn {
        name: "day",
        col_type: ColumnType::String,
    },
    AthenaAlbColumn {
        name: "time",
        col_type: ColumnType::DateTime,
    },
    AthenaAlbColumn {
        name: "domain_name",
        col_type: ColumnType::String,
    },
    AthenaAlbColumn {
        name: "elb_status_code",
        col_type: ColumnType::Integer,
    },
    AthenaAlbColumn {
        name: "target_status_code",
        col_type: ColumnType::Integer,
    },
    AthenaAlbColumn {
        name: "request_url",
        col_type: ColumnType::String,
    },
    AthenaAlbColumn {
        name: "request_method",
        col_type: ColumnType::String,
    },
    AthenaAlbColumn {
        name: "client_ip",
        col_type: ColumnType::String,
    },
    AthenaAlbColumn {
        name: "target_ip",
        col_type: ColumnType::String,
    },
    // AthenaAlbColumn::TargetStatusCode,
];

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct AthenaAlbColumn {
    pub name: &'static str,
    pub col_type: ColumnType,
}

#[derive(Debug)]
pub enum ParsedValue {
    Integer(i64),
    String(String),
    DateTime(NaiveDateTime),
}

impl AthenaAlbColumn {
    pub fn prepare_value(&self, value: &str) -> String {
        match self.col_type {
            ColumnType::String | ColumnType::DateTime => format!("'{}'", value),
            ColumnType::Integer => value.to_string(),
        }
    }

    pub fn parse_value(&self, value: &str) -> Result<ParsedValue, String> {
        match self.col_type {
            ColumnType::Integer => value
                .parse::<i64>()
                .map(ParsedValue::Integer)
                .map_err(|e| format!("Failed to parse integer: {}", e)),
            ColumnType::String => Ok(ParsedValue::String(value.to_string())),
            ColumnType::DateTime => NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.fZ")
                .map(ParsedValue::DateTime)
                .map_err(|e| format!("Failed to parse datetime: {}", e)),
        }
    }

    pub fn as_str(&self) -> &str {
        self.name
    }
}

impl FromStr for AthenaAlbColumn {
    type Err = QueryError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        ATHENA_ALB_COLUMNS
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
    group_by_clauses: Vec<String>,
    order_by_clauses: Vec<String>,
}

impl QueryBuilder<'_> {
    pub fn new<'a>(table: &'a str) -> QueryBuilder<'a> {
        QueryBuilder {
            table,
            where_clauses: vec![],
            select_clauses: vec![],
            group_by_clauses: vec![],
            order_by_clauses: vec![],
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
                    let column = AthenaAlbColumn::from_str(col_str)?;

                    self.select_clauses.push(column.as_str().to_string());
                }
                Select::Count(col_str_opt) => {
                    if let Some(col_str) = col_str_opt {
                        let column = AthenaAlbColumn::from_str(col_str)?;

                        self.select_clauses
                            .push(format!("count({}) AS count", column.as_str()));
                    } else {
                        self.select_clauses.push("count(*) AS count".to_string());
                    }
                }
                Select::Average(col_str) => {
                    let column = AthenaAlbColumn::from_str(col_str)?;

                    self.select_clauses
                        .push(format!("avg({}) AS avg", column.as_str()));
                }
            }
        }

        Ok(self)
    }

    pub fn conditions(&mut self, where_clause: &Vec<Where>) -> Result<&mut Self, QueryError> {
        if where_clause.is_empty() {
            return Ok(self);
        }

        for where_clause in where_clause {
            let (col_str, op, value) = match where_clause {
                Where::Equals(col, val) => (col, "=", val),
                Where::NotEquals(col, val) => (col, "!=", val),
                Where::GreaterThan(col, val) => (col, ">", val),
                Where::LessThan(col, val) => (col, "<", val),
                Where::GreaterThanOrEqual(col, val) => (col, ">=", val),
                Where::LessThanOrEqual(col, val) => (col, "<=", val),
                Where::Like(col, val) => (col, "LIKE", val),
                Where::In(col, values) => {
                    let column = AthenaAlbColumn::from_str(col)?;

                    let formatted_values = values
                        .iter()
                        .map(|v| column.prepare_value(v))
                        .collect::<Vec<String>>()
                        .join(",");
                    self.where_clauses.push(format!(
                        "{} IN ({})",
                        column.as_str(),
                        formatted_values
                    ));
                    continue;
                }
            };

            let column = AthenaAlbColumn::from_str(col_str)?;

            self.where_clauses.push(format!(
                "{} {} {}",
                column.as_str(),
                op,
                column.prepare_value(value)
            ));
        }

        Ok(self)
    }

    pub fn since(&mut self, since: Option<NaiveDateTime>) -> Result<&mut Self, QueryError> {
        if let Some(since) = since {
            let col = AthenaAlbColumn::from_str("day")?;

            self.where_clauses.push(format!(
                "{} >= '{}'",
                col.as_str(),
                since.format(DAY_FORMAT)
            ));

            self.where_clauses
            .push(format!("parse_datetime(time,'yyyy-MM-dd''T''HH:mm:ss.SSSSSS''Z') >= parse_datetime('{}','yyyy-MM-dd-HH:mm:ss')", since.format(TIME_FORMAT)));
        }

        Ok(self)
    }

    pub fn until(&mut self, until: Option<NaiveDateTime>) -> Result<&mut Self, QueryError> {
        if let Some(until) = until {
            let col = AthenaAlbColumn::from_str("day")?;

            self.where_clauses.push(format!(
                "{} <= '{}'",
                col.as_str(),
                until.format(DAY_FORMAT)
            ));

            self.where_clauses
            .push(format!("parse_datetime(time,'yyyy-MM-dd''T''HH:mm:ss.SSSSSS''Z') <= parse_datetime('{}','yyyy-MM-dd-HH:mm:ss')", until.format(TIME_FORMAT)));
        }

        Ok(self)
    }

    pub fn facet(&mut self, facet_input: &Vec<Facet>) -> Result<&mut Self, QueryError> {
        if facet_input.is_empty() {
            return Ok(self);
        }

        for facet in facet_input {
            let col = AthenaAlbColumn::from_str(facet.0.as_str())?;

            let col_name = col.as_str().to_string();

            match col.name {
                "path" => {
                    let regex = r"regexp_replace(regexp_replace(regexp_replace(regexp_replace(lower(request_url), '[0-9a-fA-F]{4,12}(?:-[0-9a-fA-F]{4,12}){0,4}', '<GUID>'),'https\:.*:443\/', ''),'\d+', '<ID>'),'\?.+','')";

                    self.group_by_clauses.push(regex.to_string());

                    self.select_clauses.push(col_name);
                }
                _ => {
                    self.group_by_clauses.push(col_name.clone());

                    self.select_clauses.push(col_name.clone());
                }
            }
        }

        Ok(self)
    }

    pub fn build_query(&self) -> String {
        let capacity = 512;
        let mut query_string = String::with_capacity(capacity);

        if self.select_clauses.is_empty() {
            // TODO SELECT *
            query_string.push_str(format!("SELECT time, request_url, elb_status_code, target_status_code, target_processing_time, response_processing_time, client_ip, target_ip, request_creation_time from {}", self.table).as_str());
        } else {
            query_string.push_str(
                format!(
                    "SELECT {} FROM {}",
                    self.select_clauses.join(", "),
                    self.table
                )
                .as_str(),
            );
        }

        if !self.where_clauses.is_empty() {
            query_string.push_str(format!(" WHERE {}", self.where_clauses.join(" AND ")).as_str());
        }

        if !self.group_by_clauses.is_empty() {
            query_string
                .push_str(format!(" GROUP BY {}", self.group_by_clauses.join(", ")).as_str());
        }

        if !self.order_by_clauses.is_empty() {
            query_string
                .push_str(format!(" ORDER BY {}", self.order_by_clauses.join(", ")).as_str());
        }

        query_string
    }
}
