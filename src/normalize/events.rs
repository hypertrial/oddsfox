use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, RecordBatch, StringBuilder, TimestampMillisecondBuilder,
};
use chrono::Utc;

use crate::error::Result;
use crate::gamma::GammaEvent;
use crate::schema;

#[allow(clippy::too_many_arguments)]
pub fn events_batch(
    events: &[GammaEvent],
    source: &str,
    raw_url: &str,
    raw_sha256: &str,
    run_id: &str,
) -> Result<RecordBatch> {
    let schema = schema::events::schema();
    let mut event_id = StringBuilder::new();
    let mut slug = StringBuilder::new();
    let mut title = StringBuilder::new();
    let mut description = StringBuilder::new();
    let mut category = StringBuilder::new();
    let mut tags = StringBuilder::new();
    let mut active = BooleanBuilder::new();
    let mut closed = BooleanBuilder::new();
    let mut start_time = TimestampMillisecondBuilder::new();
    let mut end_time = TimestampMillisecondBuilder::new();
    let mut created_at = TimestampMillisecondBuilder::new();
    let mut updated_at = TimestampMillisecondBuilder::new();
    let mut raw_json = StringBuilder::new();
    let mut source_col = StringBuilder::new();
    let mut raw_url_col = StringBuilder::new();
    let mut raw_sha_col = StringBuilder::new();
    let mut ingested_at = TimestampMillisecondBuilder::new();
    let mut run_id_col = StringBuilder::new();
    let now = Utc::now().timestamp_millis();

    for event in events {
        event_id.append_value(&event.id);
        slug.append_option(event.slug.as_deref());
        title.append_option(event.title.as_deref());
        description.append_option(event.description.as_deref());
        category.append_option(event.category.as_deref());
        let tag_json = serde_json::to_string(
            &event
                .tags
                .iter()
                .filter_map(|t| t.slug.clone().or_else(|| t.label.clone()))
                .collect::<Vec<_>>(),
        )
        .unwrap_or_else(|_| "[]".into());
        tags.append_value(tag_json);
        active.append_option(event.active);
        closed.append_option(event.closed);
        append_ts(&mut start_time, super::parse_ts(event.startDate.as_deref()));
        append_ts(&mut end_time, super::parse_ts(event.endDate.as_deref()));
        append_ts(&mut created_at, super::parse_ts(event.createdAt.as_deref()));
        append_ts(&mut updated_at, super::parse_ts(event.updatedAt.as_deref()));
        raw_json.append_value(serde_json::to_string(event).unwrap_or_else(|_| "{}".into()));
        source_col.append_value(source);
        raw_url_col.append_value(raw_url);
        raw_sha_col.append_value(raw_sha256);
        ingested_at.append_value(now);
        run_id_col.append_value(run_id);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(event_id.finish()),
        Arc::new(slug.finish()),
        Arc::new(title.finish()),
        Arc::new(description.finish()),
        Arc::new(category.finish()),
        Arc::new(tags.finish()),
        Arc::new(active.finish()),
        Arc::new(closed.finish()),
        Arc::new(start_time.finish()),
        Arc::new(end_time.finish()),
        Arc::new(created_at.finish()),
        Arc::new(updated_at.finish()),
        Arc::new(raw_json.finish()),
        Arc::new(source_col.finish()),
        Arc::new(raw_url_col.finish()),
        Arc::new(raw_sha_col.finish()),
        Arc::new(ingested_at.finish()),
        Arc::new(run_id_col.finish()),
    ];
    Ok(RecordBatch::try_new(schema, columns)?)
}

fn append_ts(builder: &mut TimestampMillisecondBuilder, value: Option<chrono::DateTime<Utc>>) {
    if let Some(ts) = value {
        builder.append_value(ts.timestamp_millis());
    } else {
        builder.append_null();
    }
}
