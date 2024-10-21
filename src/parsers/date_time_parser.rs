use chrono::{Duration, NaiveDate, NaiveDateTime, Utc};
use regex::Regex;
use std::ops::Deref;

use super::Parser;

#[derive(Debug, Clone)]
pub struct DateTimeParser(NaiveDateTime);

impl Deref for DateTimeParser {
    type Target = NaiveDateTime;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Parser for DateTimeParser {
    type Output = NaiveDateTime;

    fn from_str(input: &str) -> Result<NaiveDateTime, &'static str> {
        // Try parsing as "YYYY-MM-DD"
        if let Ok(date) = NaiveDate::parse_from_str(input, "%Y-%m-%d") {
            let datetime = date.and_hms_opt(0, 0, 0).unwrap();

            return Ok(datetime);
        }

        // Try parsing as "YYYY-MM-DD HH:MM:SS"
        let formats = ["%Y-%m-%d %H", "%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"];

        for format in formats.iter() {
            if let Ok(datetime) = NaiveDateTime::parse_from_str(input, format) {
                return Ok(datetime);
            }
        }

        let re = Regex::new(r"(?i)^(\d+)\s*(day|week|hour|minute)s?\s*(ago)?$").unwrap();

        if let Some(caps) = re.captures(input) {
            let amount: i64 = caps[1].parse().unwrap();

            let unit = &caps[2];

            let duration = match unit {
                "day" => Duration::days(amount),
                "week" => Duration::weeks(amount),
                "hour" => Duration::hours(amount),
                "minute" => Duration::minutes(amount),
                _ => unreachable!(),
            };

            let datetime = Utc::now().naive_utc() - duration;

            return Ok(datetime);
        }

        Err("Could not parse date time")
    }
}
