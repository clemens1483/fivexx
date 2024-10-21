use clap::Parser;
use commands::{configure, query, Commands};

mod adapters;
mod commands;
mod config;
mod formatters;
mod parallel_querier;
mod parsers;
mod query;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    cmd: Commands,
}

// TODO:
//   - implement 5xx
//   - change NaiveDateTime to UTC
//   - implement --output format
//   - improve QueryError
//   - implement --debug mode (improve query logging)
//   - implement query mode
//         QUERY (
//          SELECT * FROM data_source.table SINCE  AGO COUNT BY controller,action
//          ) FROM datasets 123, 456 // FROM *
//        COMBINE RESULTS BY SUM
//   - implement count by hour, week
//   - implement parallel execution
//   - add dataset columns, etc. to output
//   - domain configure
//   implement average by (potentially should have "aggregators", also want percentiles, etc.)
// --aggregate-by=day, --aggreggation=  - aggregat/select e.g. --select="COUNT(*)" --group-by=day

#[tokio::main]
async fn main() {
    let args = Cli::parse();

    match args.cmd {
        Commands::Configure(args) => {
            let _ = configure(args);
        }
        Commands::Query(args) => {
            let _ = query(args).await;
        }
    }
}
