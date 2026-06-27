use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use serde::Serialize;

use crate::config::Table;
use crate::error::Result;
use crate::paths::LakePaths;
use crate::schema;

pub fn lake_contract_version() -> &'static str {
    "1.0.0"
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ContractColumn {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct LakeContract {
    pub contract_version: String,
    pub schema_version: String,
    pub lake_layout_version: String,
    pub oddsfox_version: String,
    pub bronze_tables: BTreeMap<String, Vec<ContractColumn>>,
    pub metadata_tables: BTreeMap<String, Vec<ContractColumn>>,
}

pub fn build_contract() -> LakeContract {
    let mut bronze_tables = BTreeMap::new();
    for table in Table::all() {
        bronze_tables.insert(table.as_str().to_string(), schema_columns(*table));
    }

    let mut metadata_tables = BTreeMap::new();
    metadata_tables.insert(
        "sync_state".into(),
        vec![
            column("source", "Utf8", false),
            column("cursor_key", "Utf8", false),
            column("cursor_value", "Utf8", false),
        ],
    );

    LakeContract {
        contract_version: lake_contract_version().into(),
        schema_version: schema::schema_version().into(),
        lake_layout_version: schema::lake_layout_version().into(),
        oddsfox_version: env!("CARGO_PKG_VERSION").into(),
        bronze_tables,
        metadata_tables,
    }
}

fn column(name: &str, data_type: &str, nullable: bool) -> ContractColumn {
    ContractColumn {
        name: name.into(),
        data_type: data_type.into(),
        nullable,
    }
}

fn schema_columns(table: Table) -> Vec<ContractColumn> {
    schema::arrow_schema(table)
        .fields()
        .iter()
        .map(|field| ContractColumn {
            name: field.name().clone(),
            data_type: format!("{:?}", field.data_type()),
            nullable: field.is_nullable(),
        })
        .collect()
}

pub fn contract_json() -> Result<String> {
    Ok(serde_json::to_string_pretty(&build_contract())?)
}

pub fn write_contract_file(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, contract_json()?)?;
    Ok(())
}

pub fn refresh_contract(paths: &LakePaths) -> Result<()> {
    write_contract_file(&paths.contract_manifest())
}

pub fn print_contract() -> Result<()> {
    println!("{}", contract_json()?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contract_includes_bronze_tables() {
        let contract = build_contract();
        assert!(contract.bronze_tables.contains_key("markets"));
        assert_eq!(contract.contract_version, "1.0.0");
    }
}
