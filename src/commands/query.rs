use std::{collections::HashMap, fs::File, io::Write};

use crate::{
    adapters::{AdapterFactory, QueryAdapter},
    config::DataSource,
    formatters::{CSVFormatter, Formatter},
    parsers::{QueryInput, QueryParser, Select, Where},
};

use super::QueryArgs;

pub async fn query(args: QueryArgs) {
    let data_sources: Vec<DataSource>;
    let query_input: QueryInput;

    if let Some(query_string) = args.raw {
        let parsed_query = QueryParser::parse(&query_string).unwrap();

        query_input = parsed_query.0;
        data_sources = parsed_query.1;
    } else {
        let mut select: Vec<Select> = vec![];
        let mut conditions: Vec<Where> = vec![];

        select.push(Select::Column("elb_status_code".to_string()));
        select.push(Select::Column("domain_name".to_string()));
        select.push(Select::Column("request_method".to_string()));
        select.push(Select::Column("request_url".to_string()));

        if !args.code.is_empty() {
            conditions.push(Where::In("elb_status_code".to_string(), args.code));
        }

        if !args.domain.is_empty() {
            conditions.push(Where::In("domain_name".to_string(), args.domain));
        }

        if !args.method.is_empty() {
            conditions.push(Where::In("request_method".to_string(), args.method));
        }

        if let Some(url) = args.request_url {
            if url.contains("%") {
                conditions.push(Where::Like("request_url".to_string(), url));
            } else {
                conditions.push(Where::Equals("request_url".to_string(), url));
            }
        }

        query_input = QueryInput {
            select,
            conditions,
            facet: vec![],
            since: args.since,
            until: args.until,
        };

        data_sources = args.data_sources;
    }

    // let query_input = QueryInput {x
    //     status_codes: args.code,
    //     domains: args.domain,
    //     methods: args.method,
    //     url: args.request_url,
    //     since: args.since,
    //     until: args.until,
    //     count_by: args.count_by,
    // };

    let mut results: Vec<HashMap<String, String>> = Vec::new();

    for data_source in data_sources.iter() {
        let adapter_factory = AdapterFactory::new();
        let adapter = adapter_factory.create_adapter(&data_source);

        let query = adapter.build_query(&query_input).unwrap();

        println!("\n{}\n", query);

        match adapter.execute_query(&query).await {
            Ok(result) => {
                results.extend(result);
            }
            Err(e) => println!("{:?}", e),
        }
    }

    let formatter = CSVFormatter::default();
    let formatted = formatter.format(results).unwrap();

    // write to file
    let mut file = File::create("result.csv").unwrap();
    file.write_all(formatted.as_bytes()).unwrap();

    // let formatter = JSONFormatter {};
    // let formatted = formatter.format(results);

    // // write to file
    // let mut file = File::create("result.json").unwrap();
    // file.write_all(formatted.as_bytes()).unwrap();

    // println!("{:?}", formatted);
}
