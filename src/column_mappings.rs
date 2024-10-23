use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::LazyLock;

pub static COLUMN_MAPPINGS: LazyLock<Vec<Mapping>> = LazyLock::new(|| load_mappings().unwrap());

#[derive(Serialize, Deserialize, Debug)]
pub struct Mapping {
    from: Source,
    to: Source,
    mapping: HashMap<String, String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Source {
    data_source_ids: Vec<String>,
    column: String,
}

fn load_mappings() -> Result<Vec<Mapping>, Box<dyn std::error::Error>> {
    let file = File::open(Path::new("column_mappings.json"))?;
    let reader = BufReader::new(file);

    let mappings: Vec<Mapping> = serde_json::from_reader(reader)?;

    Ok(mappings)
}

pub fn get_mapping<'a>(
    from_id: &str,
    to_id: &str,
    from_col: &str,
    to_col: &str,
) -> Option<&'a HashMap<String, String>> {
    for mapping in COLUMN_MAPPINGS.iter() {
        if mapping.from.data_source_ids.contains(&from_id.to_string())
            && mapping.to.data_source_ids.contains(&to_id.to_string())
            && mapping.from.column == from_col.to_string()
            && mapping.to.column == to_col.to_string()
        {
            return Some(&mapping.mapping);
        }
    }

    None
}
