use std::ops::Deref;

use crate::config::{DataSource, CONFIG};

use super::Parser;

pub struct DatasetParser(Vec<DataSource>);

impl Deref for DatasetParser {
    type Target = Vec<DataSource>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Parser for DatasetParser {
    type Output = Vec<DataSource>;

    fn from_str(input: &str) -> Result<Self::Output, &'static str> {
        let dataset_ids = input
            .split(',')
            .map(|id| id.trim().to_string())
            .collect::<Vec<String>>();

        // TODO: Config::data_sources().where_id_in(&dataset_ids)

        println!("dataset_ids: {:?}", dataset_ids);

        // TODO: not great that we are clongin
        let data_sources = CONFIG
            .data_sources()
            .into_iter()
            .filter(|ds| dataset_ids.contains(&ds.id))
            .map(|ds| ds.clone())
            .collect::<Vec<DataSource>>();

        println!("data_sources: {:?}", data_sources);

        // let data_sources = DataSource.where_id_in(&dataset_ids);
        Ok(data_sources)
    }
}

impl DatasetParser {
    pub fn from_id(id: &str) -> Option<DataSource> {
        CONFIG
            .data_sources()
            .into_iter()
            .find(|ds| ds.id == id)
            .cloned()
    }
}
