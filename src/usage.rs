use std::collections::BTreeMap;
use std::path::Path;

use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::check::check_lake;
use crate::error::Result;
use crate::manifest::{ManifestStore, RunRecord};

#[derive(Debug, Clone, Serialize, Default, PartialEq, Eq)]
pub struct UsageSummary {
    pub runs_total: usize,
    pub completed: usize,
    pub failed: usize,
    pub incomplete: usize,
    pub rows_written: i64,
    pub last_success_at: Option<DateTime<Utc>>,
    pub last_failure_at: Option<DateTime<Utc>>,
    pub issue_count: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct UsageRun {
    pub run_id: String,
    pub command: String,
    pub status: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub duration_seconds: Option<i64>,
    pub rows_written: i64,
    pub oddsfox_version: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct UsageReport {
    pub summary: UsageSummary,
    pub runs: Vec<UsageRun>,
    pub issues: Vec<String>,
    pub suggestions: Vec<String>,
}

pub fn usage_report(out: &Path) -> Result<UsageReport> {
    let records = ManifestStore::open_read_only(out)?.run_records();
    let issues = check_lake(out)?.issues;
    Ok(build_usage_report(records, issues, out))
}

fn build_usage_report(records: Vec<RunRecord>, issues: Vec<String>, out: &Path) -> UsageReport {
    let mut latest = BTreeMap::new();
    for record in records {
        latest.insert(record.run_id.clone(), record);
    }

    let mut runs: Vec<_> = latest.into_values().map(UsageRun::from).collect();
    runs.sort_by(|a, b| b.started_at.cmp(&a.started_at));

    let mut summary = UsageSummary {
        runs_total: runs.len(),
        issue_count: issues.len(),
        ..UsageSummary::default()
    };
    for run in &runs {
        match run.status.as_str() {
            "complete" => {
                summary.completed += 1;
                summary.rows_written += run.rows_written;
                summary.last_success_at = max_time(summary.last_success_at, run.finished_at);
            }
            "failed" => {
                summary.failed += 1;
                summary.last_failure_at = max_time(
                    summary.last_failure_at,
                    run.finished_at.or(Some(run.started_at)),
                );
            }
            _ => summary.incomplete += 1,
        }
    }

    let suggestions = suggestions(&summary, out);
    runs.truncate(50);
    UsageReport {
        summary,
        runs,
        issues,
        suggestions,
    }
}

fn max_time(a: Option<DateTime<Utc>>, b: Option<DateTime<Utc>>) -> Option<DateTime<Utc>> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

fn suggestions(summary: &UsageSummary, out: &Path) -> Vec<String> {
    let out = out.display();
    let mut suggestions = Vec::new();
    if summary.issue_count > 0 || summary.failed > 0 || summary.incomplete > 0 {
        suggestions.push(format!("oddsfox check --out {out}"));
    }
    if summary.completed == 0 {
        suggestions.push(format!("oddsfox quickstart --out {out}"));
    } else {
        suggestions.push(format!(
            "oddsfox collect hourly --source all --once --out {out}"
        ));
        suggestions.push(format!("oddsfox compute liquidity --active --out {out}"));
    }
    suggestions
}

impl From<RunRecord> for UsageRun {
    fn from(record: RunRecord) -> Self {
        Self {
            duration_seconds: record
                .finished_at
                .map(|finished| (finished - record.started_at).num_seconds()),
            run_id: record.run_id,
            command: record.command,
            status: record.status,
            started_at: record.started_at,
            finished_at: record.finished_at,
            rows_written: record.rows_written,
            oddsfox_version: record.oddsfox_version,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(run_id: &str, status: &str, rows_written: i64) -> RunRecord {
        let started_at = DateTime::parse_from_rfc3339("2026-06-29T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        RunRecord {
            run_id: run_id.into(),
            command: "sync prices".into(),
            started_at,
            finished_at: (status != "started").then_some(started_at + chrono::Duration::seconds(5)),
            status: status.into(),
            rows_written,
            oddsfox_version: "0.2.0".into(),
        }
    }

    #[test]
    fn usage_collapses_run_statuses_and_sums_completed_rows() {
        let report = build_usage_report(
            vec![run("run-1", "started", 0), run("run-1", "complete", 7)],
            Vec::new(),
            Path::new("/tmp/lake"),
        );

        assert_eq!(report.summary.runs_total, 1);
        assert_eq!(report.summary.completed, 1);
        assert_eq!(report.summary.rows_written, 7);
        assert_eq!(report.runs[0].duration_seconds, Some(5));
    }

    #[test]
    fn usage_handles_empty_runs() {
        let report = build_usage_report(Vec::new(), Vec::new(), Path::new("/tmp/lake"));
        assert_eq!(report.summary, UsageSummary::default());
        assert!(report.runs.is_empty());
        assert_eq!(
            report.suggestions,
            vec!["oddsfox quickstart --out /tmp/lake"]
        );
    }

    #[test]
    fn usage_tracks_failed_and_incomplete_runs() {
        let report = build_usage_report(
            vec![run("run-1", "failed", 0), run("run-2", "started", 0)],
            vec!["incomplete run in manifest: run-2".into()],
            Path::new("/tmp/lake"),
        );

        assert_eq!(report.summary.failed, 1);
        assert_eq!(report.summary.incomplete, 1);
        assert_eq!(report.summary.issue_count, 1);
        assert!(report
            .suggestions
            .contains(&"oddsfox check --out /tmp/lake".to_string()));
    }

    #[test]
    fn usage_report_reads_lake_manifest() {
        let dir = tempfile::tempdir().unwrap();
        crate::paths::LakePaths::new(dir.path())
            .scaffold_dirs()
            .unwrap();
        {
            let store = ManifestStore::open(dir.path()).unwrap();
            let started = Utc::now();
            store
                .append_started_run("sync markets", "run-1", started)
                .unwrap();
            store
                .append_completed_run("sync markets", "run-1", started, 4)
                .unwrap();
            store
                .append_failed_run("sync prices", "run-2", started, "boom")
                .unwrap();
        }

        let report = usage_report(dir.path()).unwrap();

        assert_eq!(report.summary.runs_total, 2);
        assert_eq!(report.summary.completed, 1);
        assert_eq!(report.summary.failed, 1);
        assert_eq!(report.summary.rows_written, 4);
        assert!(report.issues.iter().any(|issue| issue.contains("contract")));
    }
}
