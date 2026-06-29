use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;

use crate::config::Table;
use crate::error::Result;
use crate::paths::LakePaths;

pub fn quarantine_bad_row(
    lake: &LakePaths,
    table: Table,
    run_id: &str,
    message: &str,
    raw: &str,
) -> Result<()> {
    let path = lake.quarantine_bad_rows(table, run_id);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    let line = serde_json::json!({
        "message": message,
        "raw": raw,
    });
    writeln!(file, "{line}")?;
    Ok(())
}

pub fn quarantine_bad_file(
    lake: &LakePaths,
    source: &str,
    filename: &str,
    message: &str,
) -> Result<()> {
    let path = lake.quarantine_bad_file(source, filename);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    let line = serde_json::json!({
        "message": message,
        "filename": filename,
    });
    writeln!(file, "{line}")?;
    Ok(())
}

pub fn write_raw_json(
    lake: &LakePaths,
    source: &str,
    filename: &str,
    body: &[u8],
) -> Result<PathBuf> {
    let path = lake.raw_file(source, filename);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let temp = path.with_extension("json.tmp");
    std::fs::write(&temp, body)?;
    std::fs::rename(&temp, &path)?;
    Ok(path)
}

use sha2::{Digest, Sha256};

pub fn sha256_hex(body: &[u8]) -> String {
    format!("{:x}", Sha256::digest(body))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_raw_json_publishes_final_file_without_temp() {
        let dir = tempfile::tempdir().unwrap();
        let lake = LakePaths::new(dir.path());
        let path = write_raw_json(&lake, "gamma", "events.json", br#"{"ok":true}"#).unwrap();

        assert_eq!(std::fs::read(&path).unwrap(), br#"{"ok":true}"#);
        assert!(!path.with_extension("json.tmp").exists());
    }
}
