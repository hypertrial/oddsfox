use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};

use crate::config::{LakeOptions, TopBy};
use crate::error::Result;
use crate::settings::{resolve_config, OddsfoxConfig};

#[derive(Parser, Debug)]
#[command(name = "oddsfox", version, about = "Local-first Polymarket research kit")]
pub struct Cli {
    #[arg(long, global = true)]
    pub config: Option<PathBuf>,
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    Init {
        #[arg(long)]
        out: Option<PathBuf>,
    },
    Quickstart {
        #[arg(long)]
        out: Option<PathBuf>,
        #[arg(long)]
        db: Option<PathBuf>,
        #[arg(long, default_value_t = 8787)]
        port: u16,
        #[arg(long, default_value_t = 50)]
        top_volume: usize,
    },
    Sync {
        #[command(subcommand)]
        target: SyncCommands,
    },
    Snapshot {
        #[command(subcommand)]
        target: SnapshotCommands,
    },
    Watch {
        #[arg(long)]
        out: Option<PathBuf>,
        #[arg(long)]
        market: Option<String>,
        #[arg(long, default_value_t = false)]
        active: bool,
        #[arg(long)]
        tag: Option<String>,
        #[arg(long)]
        top_volume: Option<usize>,
    },
    Compute {
        #[command(subcommand)]
        target: ComputeCommands,
    },
    Search {
        query: String,
        #[arg(long)]
        out: Option<PathBuf>,
    },
    Market {
        market_id: String,
        #[arg(long)]
        out: Option<PathBuf>,
    },
    Event {
        event_id: String,
        #[arg(long)]
        out: Option<PathBuf>,
    },
    Resolved {
        #[arg(long)]
        out: Option<PathBuf>,
        #[arg(long)]
        since: Option<String>,
    },
    Top {
        #[arg(long, value_enum, default_value_t = TopByArg::Volume24h)]
        by: TopByArg,
        #[arg(long)]
        out: Option<PathBuf>,
        #[arg(long, default_value_t = 25)]
        limit: usize,
    },
    Metrics {
        #[command(subcommand)]
        target: MetricsCommands,
    },
    Check {
        #[arg(long)]
        out: Option<PathBuf>,
    },
    Repair {
        #[arg(long)]
        out: Option<PathBuf>,
    },
    Clean {
        #[arg(long)]
        out: Option<PathBuf>,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
    Schema {
        table: String,
    },
    Contract {
        #[arg(long)]
        out: Option<PathBuf>,
    },
    Duckdb {
        #[arg(long)]
        out: Option<PathBuf>,
        #[arg(long)]
        db: Option<PathBuf>,
    },
    Sql {
        query: String,
        #[arg(long)]
        out: Option<PathBuf>,
        #[arg(long)]
        db: Option<PathBuf>,
    },
    Serve {
        #[arg(long, default_value_t = 8787)]
        port: u16,
        #[arg(long)]
        out: Option<PathBuf>,
        #[arg(long)]
        db: Option<PathBuf>,
    },
    Stats {
        #[arg(long)]
        out: Option<PathBuf>,
    },
}

#[derive(Subcommand, Debug)]
pub enum SyncCommands {
    Markets {
        #[arg(long, default_value_t = false)]
        active: bool,
        #[arg(long, default_value_t = false)]
        closed: bool,
        #[arg(long, default_value_t = false)]
        all: bool,
        #[arg(long)]
        tag: Option<String>,
        #[arg(long)]
        since: Option<String>,
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long)]
        out: Option<PathBuf>,
    },
    Prices {
        #[arg(long)]
        market: Option<String>,
        #[arg(long, default_value_t = false)]
        active: bool,
        #[arg(long)]
        interval: Option<String>,
        #[arg(long)]
        fidelity: Option<u32>,
        #[arg(long)]
        since: Option<String>,
        #[arg(long)]
        until: Option<String>,
        #[arg(long)]
        out: Option<PathBuf>,
    },
}

#[derive(Subcommand, Debug)]
pub enum SnapshotCommands {
    Books {
        #[arg(long)]
        market: Option<String>,
        #[arg(long, default_value_t = false)]
        active: bool,
        #[arg(long)]
        top_volume: Option<usize>,
        #[arg(long)]
        tokens: Option<PathBuf>,
        #[arg(long)]
        out: Option<PathBuf>,
    },
}

#[derive(Subcommand, Debug)]
pub enum ComputeCommands {
    Liquidity {
        #[arg(long, default_value_t = true)]
        active: bool,
        #[arg(long)]
        out: Option<PathBuf>,
    },
    Accuracy {
        #[arg(long, default_value_t = true)]
        resolved: bool,
        #[arg(long)]
        since: Option<String>,
        #[arg(long)]
        out: Option<PathBuf>,
    },
    Calibration {
        #[arg(long, default_value_t = true)]
        resolved: bool,
        #[arg(long, default_value_t = 0.05)]
        bucket_width: f64,
        #[arg(long)]
        since: Option<String>,
        #[arg(long)]
        out: Option<PathBuf>,
    },
    All {
        #[arg(long)]
        since: Option<String>,
        #[arg(long)]
        out: Option<PathBuf>,
    },
}

#[derive(Subcommand, Debug)]
pub enum MetricsCommands {
    Market {
        market_id: String,
        #[arg(long)]
        out: Option<PathBuf>,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum TopByArg {
    Volume24h,
    Spread,
    Liquidity,
    Volume,
}

impl From<TopByArg> for TopBy {
    fn from(value: TopByArg) -> Self {
        match value {
            TopByArg::Volume24h => TopBy::Volume24h,
            TopByArg::Spread => TopBy::Spread,
            TopByArg::Liquidity => TopBy::Liquidity,
            TopByArg::Volume => TopBy::Volume,
        }
    }
}

impl Cli {
    pub fn lake_root(&self) -> Result<PathBuf> {
        lake_root_from_config(self.config.as_deref(), None)
    }

    pub fn lake_options(&self, out: Option<PathBuf>) -> Result<LakeOptions> {
        Ok(LakeOptions {
            out: lake_root_from_config(self.config.as_deref(), out)?,
        })
    }
}

pub fn lake_root_from_config(config: Option<&std::path::Path>, out: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(out) = out {
        return Ok(out);
    }
    let cfg = resolve_config(config, None)?;
    Ok(PathBuf::from(cfg.data.home))
}

pub fn default_db_for(out: &std::path::Path, config: &OddsfoxConfig) -> PathBuf {
    let db = PathBuf::from(&config.duckdb.database);
    if db.is_absolute() {
        db
    } else {
        out.join(db)
    }
}
