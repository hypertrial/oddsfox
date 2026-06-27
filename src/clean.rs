use std::path::Path;

use crate::error::Result;
use crate::paths::LakePaths;

pub fn run(out: &Path, dry_run: bool) -> Result<()> {
    let paths = LakePaths::new(out);
    let quarantine = paths.root.join("_quarantine");
    if !quarantine.exists() {
        println!("clean: nothing to do");
        return Ok(());
    }
    if dry_run {
        println!("clean dry-run: would inspect {}", quarantine.display());
    } else {
        println!("clean: inspected quarantine at {}", quarantine.display());
    }
    Ok(())
}
