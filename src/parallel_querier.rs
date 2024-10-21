// use futures::future::join_all;
// use std::{collections::HashMap, fs::File, io::Write};
// use tokio::task;

// use crate::{
//     adapters::{AdapterFactory, QueryAdapter},
//     config::DataSource,
//     formatters::{CSVFormatter, Formatter},
//     query::{QueryError, QueryInput, QueryResult},
// };

// pub struct ParallelQuerier {
//     datasets: Vec<DataSource>,
//     adapter_factory: AdapterFactory,
// }

// impl ParallelQuerier {
//     pub fn new(datasets: Vec<DataSource>) -> Self {
//         ParallelQuerier {
//             datasets,
//             adapter_factory: AdapterFactory::new(),
//         }
//     }

//     pub async fn execute(&self, query_input: &QueryInput) -> QueryResult {
//         let results = self.execute_parallel_queries(query_input).await;
//         self.combine_results(results)
//     }

//     async fn execute_parallel_queries(
//         &self,
//         query_input: &QueryInput,
//     ) -> Vec<Result<QueryResult, QueryError>> {
//         let mut handles = Vec::new();

//         for data_source in &self.datasets {
//             let query_input = query_input.clone();
//             let adapter = self.adapter_factory.create_adapter(data_source);
//             let handle = task::spawn(async move {
//                 let query = adapter.build_query(&query_input);
//                 println!("Executing query for dataset {}: {}", data_source.id, query);

//                 adapter.execute_query(&query).await
//             });
//             handles.push(handle);
//         }

//         join_all(handles)
//             .await
//             .into_iter()
//             .map(|res| {
//                 res.unwrap_or_else(|e| {
//                     Err(QueryError {
//                         message: format!("Task join error: {}", e),
//                     })
//                 })
//             })
//             .collect()
//     }

//     fn combine_results(&self, results: Vec<Result<QueryResult, QueryError>>) -> QueryResult {
//         let mut combined_results = Vec::new();

//         for result in results {
//             match result {
//                 Ok(mut query_result) => combined_results.append(&mut query_result),
//                 Err(e) => eprintln!("Error executing query: {}", e),
//             }
//         }

//         combined_results
//     }
// }
