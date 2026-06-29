use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

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
    let store = ManifestStore::open_read_only(out)?;
    let mut issues = Vec::new();

    for table in Table::all() {
        let dir = paths.bronze_table_dir(*table);
        if !dir.is_dir() {
            issues.push(format!(
                "missing bronze table directory: {}",
                table.as_str()
            ));
        }
    }

    if !paths.contract_manifest().exists() {
        issues.push("missing _metadata/contract.json".into());
    }

    for path in temp_files(&paths.root) {
        issues.push(format!("temporary file left behind: {}", path.display()));
    }

    for run_id in store.incomplete_run_ids() {
        issues.push(format!("incomplete run in manifest: {run_id}"));
    }

    let completed = store.completed_run_ids();
    for (table, run_id, path) in orphan_run_partitions(&paths, &completed) {
        issues.push(format!(
            "orphan {} run partition not marked complete: {} ({})",
            table.as_str(),
            run_id,
            path.display()
        ));
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

pub fn temp_files(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    collect_temp_files(root, &mut out);
    out
}

fn collect_temp_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_temp_files(&path, out);
        } else if path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().ends_with(".tmp"))
        {
            out.push(path);
        }
    }
}

pub fn orphan_run_partitions(
    paths: &LakePaths,
    completed: &BTreeSet<String>,
) -> Vec<(Table, String, PathBuf)> {
    let mut out = Vec::new();
    for table in Table::all()
        .iter()
        .copied()
        .filter(|table| table.is_run_partitioned())
    {
        let Ok(entries) = std::fs::read_dir(paths.bronze_table_dir(table)) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let Some(run_id) = name.strip_prefix("run=") else {
                continue;
            };
            if !completed.contains(run_id) {
                out.push((table, run_id.to_string(), path));
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_reports_temp_files_and_orphan_runs() {
        let dir = tempfile::tempdir().unwrap();
        let paths = LakePaths::new(dir.path());
        paths.scaffold_dirs().unwrap();
        std::fs::write(paths.root.join("_raw").join("left.json.tmp"), b"partial").unwrap();
        std::fs::create_dir_all(paths.bronze_table_dir(Table::Events).join("run=orphan")).unwrap();

        let report = check_lake(dir.path()).unwrap();
        assert!(report
            .issues
            .iter()
            .any(|issue| issue.contains("temporary file")));
        assert!(report.issues.iter().any(|issue| issue.contains("orphan")));
    }
}
