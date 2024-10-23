use std::{collections::HashMap, fs::File, io::Write};

use serde_json::{json, Value};

use crate::{
    adapters::{AdapterFactory, QueryAdapter},
    column_mappings::get_mapping,
    config::DataSource,
    formatters::{CSVFormatter, Formatter, JSONFormatter},
    parsers::{CorrelateCondition, QueryInput, QueryParser, Select, Where},
    query::QueryResult,
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

        select.push(Select::Column("time".to_string()));
        select.push(Select::Column("client_ip".to_string()));
        select.push(Select::Column("elb_status_code".to_string()));
        select.push(Select::Column("domain_name".to_string()));
        // select.push(Select::Column("request_method".to_string()));
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
            correlate: None,
        };

        data_sources = args.data_sources;
    }

    let mut results: QueryResult = vec![];

    for data_source in data_sources.iter() {
        let adapter_factory = AdapterFactory::new();
        let adapter = adapter_factory.create_adapter(&data_source);

        let query = adapter.build_query(&query_input).unwrap();

        println!("\n{}\n", query);

        match adapter.execute_query(&query).await {
            Ok(mut result) => {
                for row in &mut result {
                    row.insert(
                        "data_source_id".to_string(),
                        Value::String(data_source.id.clone()),
                    );
                }

                results.extend(result);
            }
            Err(e) => println!("{:?}", e),
        }
    }

    if let Some(correlate) = &query_input.correlate {
        let adapter_factory = AdapterFactory::new();

        for row in &mut results {
            let mut correlated_query_input = *correlate.query_input.clone();

            for dependent_condition in &correlate.dependent_conditions {
                match dependent_condition {
                    CorrelateCondition::Is { parent, child } => {
                        let default: HashMap<String, String> = HashMap::new();

                        let mapping = get_mapping(
                            row["data_source_id"].as_str().unwrap(),
                            &correlate.data_source.id,
                            parent,
                            child,
                        )
                        .unwrap_or(&default);

                        if let Some(parent_value) = row.get(parent) {
                            match parent_value {
                                Value::String(str_value) => {
                                    let value = match mapping.get(str_value) {
                                        Some(val) => val,
                                        None => str_value,
                                    };

                                    correlated_query_input
                                        .conditions
                                        .push(Where::Equals(child.clone(), value.clone()));
                                }
                                _ => continue,
                            }
                        } else {
                            continue; // Skip this condition if parent value is not found
                        }
                    }
                    CorrelateCondition::Within {
                        parent,
                        child: _,
                        delta,
                    } => {
                        if let Some(parent_value) = row.get(parent) {
                            match parent_value {
                                Value::String(str_value) => {
                                    let parent_datetime = chrono::NaiveDateTime::parse_from_str(
                                        str_value,
                                        "%Y-%m-%dT%H:%M:%S%.fZ",
                                    )
                                    .unwrap();

                                    correlated_query_input.since = Some(parent_datetime - *delta);
                                    correlated_query_input.until = Some(parent_datetime + *delta);
                                }
                                _ => continue,
                            }
                        } else {
                            continue; // Skip this condition if parent value is not found
                        }
                    }
                };
            }
            {
                let adapter = adapter_factory.create_adapter(&correlate.data_source);

                let query = adapter.build_query(&correlated_query_input).unwrap();

                println!("\nCorrelated Query: {}\n", query);

                match adapter.execute_query(&query).await {
                    Ok(correlated_result) => {
                        let correlated_result = json!(correlated_result);

                        // TODO: use id of dataset instead
                        row.insert("correlated".to_string(), correlated_result);
                    }
                    Err(e) => println!("Error executing correlated query: {:?}", e),
                }
            }
        }
    }

    // let formatter = CSVFormatter::default();
    // let formatted = formatter.format(results).unwrap();

    // // write to file
    // let mut file = File::create("result.csv").unwrap();
    // file.write_all(formatted.as_bytes()).unwrap();

    let formatter = JSONFormatter {};
    let formatted = formatter.format(results);

    // write to file
    let mut file = File::create("result.json").unwrap();
    file.write_all(formatted.as_bytes()).unwrap();

    // println!("{:?}", formatted);
}
