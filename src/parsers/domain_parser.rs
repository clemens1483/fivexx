use std::ops::Deref;

use crate::config::CONFIG;

use super::Parser;

pub struct DomainParser(String);

impl Deref for DomainParser {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Parser for DomainParser {
    type Output = String;

    fn from_str(input: &str) -> Result<Self::Output, &'static str> {
        let domain = if let Some(default_domain) = CONFIG.default_domain() {
            if !input.contains('.') {
                format!("{}.{}", input, default_domain)
            } else {
                input.to_string()
            }
        } else {
            input.to_string()
        };

        Ok(domain)

        // let domains = input
        //     .split(',')
        //     .map(|domain| {
        //         if let Some(default_domain) = CONFIG.default_domain() {
        //             if !domain.contains('.') {
        //                 format!("{}.{}", domain, default_domain)
        //             } else {
        //                 domain.to_string()
        //             }
        //         } else {
        //             domain.to_string()
        //         }
        //     })
        //     .collect::<Vec<String>>();

        // println!("domains: {:?}", domains);

        // Ok(domains)
    }
}
