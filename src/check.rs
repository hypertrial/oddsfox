use std::path::Path;

use crate::config::Table;
use crate::error::{OddsfoxError, Result};
use crate::manifest::ManifestStore;
use crate::paths::LakePaths;

#[derive(Debug, Clone, serde::Serialize)]
pub struct CheckReport {
    pub issues: Vec<String>,
    pub schema_version: String,
    pub lake_layout_version: String,
}

pub fn check_lake(out: &Path) -> Result<CheckReport> {
    let paths = LakePaths::new(out);
    if !paths.metadata_dir().exists() {
        return Err(OddsfoxError::LakeNotFound(out.to_path_buf()));
    }
    let _store = ManifestStore::open_read_only(out)?;
    let mut issues = Vec::new();

    for table in Table::all() {
        let dir = paths.bronze_table_dir(*table);
        if !dir.is_dir() {
            issues.push(format!("missing bronze table directory: {}", table.as_str()));
        }
    }

    if !paths.contract_manifest().exists() {
        issues.push("missing _metadata/contract.json".into());
    }

    Ok(CheckReport {
        issues,
        schema_version: crate::schema::schema_version().into(),
        lake_layout_version: crate::schema::lake_layout_version().into(),
    })
}

pub fn run(out: &Path) -> Result<CheckReport> {
    let report = check_lake(out)?;
    if report.issues.is_empty() {
        println!("check: ok");
    } else {
        for issue in &report.issues {
            println!("check issue: {issue}");
        }
        return Err(OddsfoxError::CheckFailed {
            count: report.issues.len(),
        });
    }
    Ok(report)
}
