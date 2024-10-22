use chrono::Duration;
use regex::Regex;
use std::ops::Deref;

use super::Parser;

#[derive(Debug, Clone)]
pub struct DurationParser(Duration);

impl Deref for DurationParser {
    type Target = Duration;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Parser for DurationParser {
    type Output = Duration;

    fn from_str(input: &str) -> Result<Duration, &'static str> {
        println!("input: {}", input);
        let re = Regex::new(r"(?i)^(\d+)\s*(day|week|hour|minute|second)s?\s*(ago)?$").unwrap();

        if let Some(caps) = re.captures(input) {
            let amount: i64 = caps[1].parse().unwrap();

            let unit = &caps[2];

            let duration = match unit {
                "day" => Duration::days(amount),
                "week" => Duration::weeks(amount),
                "hour" => Duration::hours(amount),
                "minute" => Duration::minutes(amount),
                "second" => Duration::seconds(amount),
                _ => unreachable!(),
            };

            return Ok(duration);
        }

        Err("Could not parse duration")
    }
}
