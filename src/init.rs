use crate::error::Result;
use crate::paths::LakePaths;
use crate::settings::{save_config, OddsfoxConfig};

pub fn run(lake_root: &std::path::Path) -> Result<()> {
    let paths = LakePaths::new(lake_root);
    paths.scaffold_dirs()?;

    let config = OddsfoxConfig {
        data: crate::settings::DataSection {
            home: lake_root.display().to_string(),
            ..Default::default()
        },
        ..Default::default()
    };
    save_config(&paths.config_file(), &config)?;

    let store = crate::manifest::ManifestStore::open(lake_root)?;
    store.write_version()?;
    store.write_schema_records()?;
    crate::contract::refresh_contract(&paths)?;

    println!(
        "initialized lake at `{}` (layout {}, schema {})",
        paths.root.display(),
        crate::schema::lake_layout_version(),
        crate::schema::schema_version()
    );
    Ok(())
}
