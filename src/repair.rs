use std::path::{Path, PathBuf};

use crate::check::{orphan_run_partitions, temp_files};
use crate::error::Result;
use crate::manifest::ManifestStore;
use crate::paths::LakePaths;

pub async fn run(out: &Path) -> Result<()> {
    let paths = LakePaths::new(out);
    let store = ManifestStore::open(out)?;

    let mut removed = 0;
    for path in temp_files(out) {
        std::fs::remove_file(&path)?;
        removed += 1;
    }

    let mut quarantined = 0;
    let completed = store.completed_run_ids();
    for (table, run_id, path) in orphan_run_partitions(&paths, &completed) {
        let dest = unique_dest(
            paths
                .root
                .join("_quarantine")
                .join("orphan_runs")
                .join(table.as_str())
                .join(format!("run={run_id}")),
        );
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::rename(&path, &dest)?;
        quarantined += 1;
    }

    println!("repair complete: removed {removed} temp file(s), quarantined {quarantined} run(s)");
    Ok(())
}

fn unique_dest(path: PathBuf) -> PathBuf {
    if !path.exists() {
        return path;
    }
    for idx in 1.. {
        let candidate = path.with_file_name(format!(
            "{}-{idx}",
            path.file_name().unwrap_or_default().to_string_lossy()
        ));
        if !candidate.exists() {
            return candidate;
        }
    }
    unreachable!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Table;

    #[tokio::test]
    async fn repair_removes_temps_and_quarantines_orphan_runs() {
        let dir = tempfile::tempdir().unwrap();
        let paths = LakePaths::new(dir.path());
        paths.scaffold_dirs().unwrap();
        let temp = paths.root.join("_raw").join("partial.json.tmp");
        std::fs::write(&temp, b"partial").unwrap();
        let orphan = paths.bronze_table_dir(Table::Events).join("run=orphan");
        std::fs::create_dir_all(&orphan).unwrap();

        run(dir.path()).await.unwrap();

        assert!(!temp.exists());
        assert!(!orphan.exists());
        assert!(paths
            .root
            .join("_quarantine/orphan_runs/events/run=orphan")
            .exists());
    }
}
