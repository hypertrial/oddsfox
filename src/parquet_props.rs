pub const DEFAULT_COMPACTION_ZSTD_LEVEL: i32 = 6;

use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::config::Table;

pub fn data_writer_properties(_table: Table) -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .set_max_row_group_row_count(Some(65_536))
        .build()
}
