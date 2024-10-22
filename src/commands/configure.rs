use std::collections::HashMap;

use inquire::{Confirm, Select, Text};

use crate::config::{
    AwsAthenaALBLog, Config, DataSource, DataSourceDetails, DataSourceType, NewRelicLog,
};

use super::ConfigureArgs;

pub fn configure(args: ConfigureArgs) -> Result<(), Box<dyn std::error::Error>> {
    if args.get_path {
        let _ = Config::file_path().inspect(|path| println!("{}", path));
        return Ok(());
    }

    let mut config = if args.update {
        Config::load()?
    } else {
        Config::default()
    };

    if config.default_domain().is_none()
        || prompt_yes_no("Do you want to update the default domain?")?
    {
        config.default_domain = Some(prompt_string(
            "Enter the default domain (e.g., example.com)",
            config.default_domain(),
        )?);
    }

    if prompt_yes_no("Do you want to add a data source?")? {
        let mut defaults: HashMap<&str, String> = HashMap::new();

        loop {
            let data_source = add_data_source(&defaults)?;

            match data_source.details {
                DataSourceDetails::AwsAthenaALBLog(ref details) => {
                    defaults.insert("region", details.region.clone());
                    defaults.insert("catalog", details.catalog.clone());
                    defaults.insert("workgroup", details.workgroup.clone());
                    defaults.insert("database", details.database.clone());
                }
                DataSourceDetails::NewRelicLog(ref details) => {
                    defaults.insert("api_key", details.api_key.clone());
                    defaults.insert("account_id", details.account_id.clone());
                }
            }

            config.data_sources.push(data_source);

            if !prompt_yes_no(
                "Do you want to use these values as defaults for the next data source?",
            )? {
                defaults.clear();
            }

            if !prompt_yes_no("Do you want to add another data source?")? {
                break;
            }

            println!("\n");
        }
    }

    save_configuration(config);

    Ok(())
}

fn add_data_source(
    defaults: &HashMap<&str, String>,
) -> Result<DataSource, Box<dyn std::error::Error>> {
    let name = prompt_string("Enter a name for the data source", None).unwrap_or_default();
    let id = prompt_string("Enter an ID for the data source", None).unwrap_or_default();
    let source_type = prompt_options("Choose a source type", &DataSourceType::all())?;

    let details = match source_type {
        DataSourceType::AwsAthenaALBLog => {
            let region = prompt_string("Enter the AWS region", defaults.get("region"))?;
            let catalog = prompt_string("Enter the catalog name", defaults.get("catalog"))?;
            let workgroup = prompt_string("Enter the workgroup name", defaults.get("workgroup"))?;
            let database = prompt_string("Enter the database name", defaults.get("database"))?;
            let table = prompt_string("Enter the table name", None).unwrap_or_default();

            DataSourceDetails::AwsAthenaALBLog(AwsAthenaALBLog {
                region,
                catalog,
                workgroup,
                database,
                table,
            })
        }
        DataSourceType::NewRelicLog => {
            let api_key = prompt_string("Enter the New Relic API key", None)?;
            let account_id = prompt_string("Enter the New Relic account ID", None)?;
            let table = prompt_string("Enter the table name", None)?;

            DataSourceDetails::NewRelicLog(NewRelicLog {
                api_key,
                account_id,
                table,
            })
        }
    };

    Ok(DataSource {
        name,
        id,
        source_type,
        details,
    })
}

fn prompt_yes_no(question: &str) -> Result<bool, Box<dyn std::error::Error>> {
    let answer = Confirm::new(question).with_default(false).prompt()?;

    Ok(answer)
}

fn prompt_string(
    prompt: &str,
    default: Option<&String>,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut text = Text::new(prompt);

    if let Some(default) = default {
        text = text.with_default(default);
    }

    Ok(text.prompt()?)
}

fn prompt_options<T>(prompt: &str, options: &[T]) -> Result<T, Box<dyn std::error::Error>>
where
    T: Clone + std::fmt::Display + ToString,
{
    let selection = Select::new(prompt, options.to_vec())
        .with_vim_mode(true)
        .prompt()?;

    Ok(selection)
}

fn save_configuration(config: Config) -> () {
    confy::store("fivexx", "config", config).expect("Failed to save configuration");

    match Config::file_path() {
        Ok(path) => println!("Saved config to {:?}", path),
        Err(_) => println!("Failed to save configuration"),
    }
}
