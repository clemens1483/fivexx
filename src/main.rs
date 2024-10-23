use clap::Parser;
use commands::{configure, query, Commands};

mod adapters;
mod commands;
mod config;
mod formatters;
mod column_mappings;
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
//   - implement count by hour, week
//   - implement parallel execution
//   - add dataset columns, etc. to output
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
