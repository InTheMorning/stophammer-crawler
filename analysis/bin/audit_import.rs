#![allow(
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    reason = "offline replay tool keeps resume and batch logic in one executable flow"
)]

use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use clap::Parser;
use rusqlite::Connection;
use serde::Deserialize;

#[allow(dead_code)]
#[path = "../../src/crawl.rs"]
mod crawl;
#[path = "../../src/pool.rs"]
mod pool;

use crawl::{CrawlConfig, CrawlOutcome, ingest_cached_feed};
use pool::run_pool;

const AUDIT_INGEST_ATTEMPTS: u32 = 3;

#[derive(Parser, Debug)]
#[command(
    name = "audit_import",
    about = "Import cached feed_audit.ndjson rows into stophammer without refetching feeds"
)]
struct Cli {
    /// Path to `feed_audit` NDJSON.
    #[arg(long, default_value = "./analysis/data/feed_audit.ndjson")]
    input: String,

    /// Path to import state database (resume cursor by NDJSON row number).
    #[arg(long, default_value = "./analysis/data/audit_import_state.db")]
    state: String,

    /// Rows per batch.
    #[arg(long, default_value_t = 100)]
    batch: usize,

    /// Maximum number of NDJSON rows to process this run.
    #[arg(long)]
    limit: Option<usize>,

    /// Parallel parse+ingest workers.
    #[arg(long, env = "CONCURRENCY", default_value_t = 5)]
    concurrency: usize,

    /// Log candidates without posting to stophammer.
    #[arg(long)]
    dry_run: bool,

    /// Clear resume cursor and start from the first NDJSON row.
    #[arg(long)]
    reset: bool,
}

#[derive(Debug)]
struct AuditCandidate {
    row_num: usize,
    source_db: SourceDbRow,
    fetch: FetchRow,
    raw_xml: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct SourceDbRow {
    feed_guid: String,
    feed_url: String,
    title: String,
}

#[derive(Debug, Clone, Deserialize)]
struct FetchRow {
    final_url: Option<String>,
    http_status: Option<u16>,
    content_sha256: Option<String>,
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AuditRow {
    source_db: SourceDbRow,
    fetch: FetchRow,
    raw_xml: Option<String>,
    #[serde(rename = "parse_error")]
    _parse_error: Option<String>,
}

/// Resume cursor stored in a separate state DB.
struct ProgressStore {
    conn: Connection,
}

impl ProgressStore {
    fn open(path: &str) -> Self {
        if let Some(parent) = Path::new(path).parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent).unwrap_or_else(|e| {
                panic!(
                    "failed to create audit import state directory {}: {e}",
                    parent.display()
                )
            });
        }

        let conn = Connection::open(path).expect("failed to open audit import state DB");
        conn.pragma_update(None, "journal_mode", "MEMORY")
            .expect("failed to set audit import state journal mode");
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS import_progress (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )",
        )
        .expect("failed to create progress table");
        Self { conn }
    }

    fn get_last_row(&self) -> usize {
        self.conn
            .query_row(
                "SELECT value FROM import_progress WHERE key = 'last_processed_row'",
                [],
                |row| row.get::<_, String>(0),
            )
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    }

    fn set_last_row(&self, row_num: usize) {
        if let Err(e) = self.conn.execute(
            "INSERT INTO import_progress (key, value) VALUES ('last_processed_row', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            [row_num.to_string()],
        ) {
            eprintln!("audit_import: WARNING: failed to persist cursor at row={row_num}: {e}");
        }
    }

    fn reset(&self) {
        self.conn
            .execute(
                "DELETE FROM import_progress WHERE key = 'last_processed_row'",
                [],
            )
            .expect("failed to reset audit import cursor");
    }
}

fn query_batch(path: &str, start_row: usize, batch_size: usize) -> Vec<AuditCandidate> {
    let file = File::open(path).unwrap_or_else(|e| {
        eprintln!("audit_import: failed to open NDJSON at {path}: {e}");
        std::process::exit(1);
    });
    let reader = BufReader::new(file);
    let mut batch = Vec::with_capacity(batch_size);

    for (index, line) in reader.lines().enumerate() {
        let row_num = index + 1;
        if row_num <= start_row {
            continue;
        }
        if batch.len() >= batch_size {
            break;
        }

        let line = match line {
            Ok(line) => line,
            Err(e) => {
                eprintln!("audit_import: failed reading row {row_num}: {e}");
                continue;
            }
        };

        let row: AuditRow = match serde_json::from_str(&line) {
            Ok(row) => row,
            Err(e) => {
                eprintln!("audit_import: failed parsing row {row_num} as NDJSON: {e}");
                continue;
            }
        };

        batch.push(AuditCandidate {
            row_num,
            source_db: row.source_db,
            fetch: row.fetch,
            raw_xml: row.raw_xml,
        });
    }

    batch
}

#[allow(
    clippy::too_many_arguments,
    reason = "audit import retry wrapper threads through cached row details plus retry context"
)]
async fn ingest_cached_feed_with_retries(
    client: &reqwest::Client,
    source_url: &str,
    canonical_url: &str,
    http_status: u16,
    raw_xml: &str,
    content_hash: Option<&str>,
    fallback_guid: Option<&str>,
    config: &CrawlConfig,
    row_num: usize,
) -> CrawlOutcome {
    let mut attempt = 1;

    loop {
        let outcome = ingest_cached_feed(
            client,
            source_url,
            canonical_url,
            http_status,
            raw_xml,
            content_hash,
            fallback_guid,
            config,
        )
        .await;

        if outcome.is_retryable() && attempt < AUDIT_INGEST_ATTEMPTS {
            let backoff = outcome
                .retry_delay(attempt)
                .unwrap_or_else(|| std::time::Duration::from_secs(1_u64 << (attempt - 1)));
            eprintln!(
                "  audit_import: retrying ingest after attempt {attempt}/{AUDIT_INGEST_ATTEMPTS} for row={row_num} {source_url}: {outcome}"
            );
            tokio::time::sleep(backoff).await;
            attempt += 1;
            continue;
        }

        return outcome;
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let progress = ProgressStore::open(&cli.state);

    if cli.reset {
        progress.reset();
        eprintln!("audit_import: cursor reset to row 0");
    }

    let mut cursor = progress.get_last_row();
    eprintln!(
        "audit_import: starting from row={cursor}, batch={}, concurrency={}",
        cli.batch, cli.concurrency
    );

    let config = Arc::new(if cli.dry_run {
        CrawlConfig {
            crawl_token: String::new(),
            ingest_url: String::new(),
            user_agent: "stophammer-crawler/0.1 (audit-import dry-run)".to_string(),
            fetch_timeout: std::time::Duration::from_secs(1),
            ingest_timeout: std::time::Duration::from_secs(10),
        }
    } else {
        CrawlConfig::from_env()
    });

    let client = Arc::new(reqwest::Client::new());
    let mut total_processed: u64 = 0;

    loop {
        let effective_batch_size = match cli.limit {
            Some(limit) => {
                let remaining = limit.saturating_sub(total_processed as usize);
                remaining.min(cli.batch)
            }
            None => cli.batch,
        };
        if effective_batch_size == 0 {
            eprintln!("audit_import: reached --limit after row={cursor}");
            break;
        }

        let batch = query_batch(&cli.input, cursor, effective_batch_size);
        if batch.is_empty() {
            eprintln!("audit_import: no more candidates after row={cursor}");
            break;
        }

        let batch_last_row = batch.last().map_or(cursor, |row| row.row_num);
        let batch_len = batch.len();

        if cli.dry_run {
            for row in &batch {
                let has_raw_xml = row.raw_xml.is_some();
                let status_display = row
                    .fetch
                    .http_status
                    .map_or_else(|| "(none)".to_string(), |status| status.to_string());
                eprintln!(
                    "  [dry-run] row={} guid={} status={} raw_xml={} {}",
                    row.row_num,
                    row.source_db.feed_guid,
                    status_display,
                    if has_raw_xml { "yes" } else { "no" },
                    row.source_db.feed_url,
                );
            }
        } else {
            let accepted = Arc::new(AtomicU64::new(0));
            let rejected = Arc::new(AtomicU64::new(0));
            let no_change = Arc::new(AtomicU64::new(0));
            let parse_errors = Arc::new(AtomicU64::new(0));
            let ingest_errors = Arc::new(AtomicU64::new(0));
            let skipped = Arc::new(AtomicU64::new(0));

            let tasks: Vec<_> = batch
                .into_iter()
                .map(|row| {
                    let client = Arc::clone(&client);
                    let config = Arc::clone(&config);
                    let accepted = Arc::clone(&accepted);
                    let rejected = Arc::clone(&rejected);
                    let no_change = Arc::clone(&no_change);
                    let parse_errors = Arc::clone(&parse_errors);
                    let ingest_errors = Arc::clone(&ingest_errors);
                    let skipped = Arc::clone(&skipped);

                    move || async move {
                        let Some(raw_xml) = row.raw_xml.as_deref() else {
                            skipped.fetch_add(1, Ordering::Relaxed);
                            let reason = row.fetch.error.as_deref().unwrap_or("missing raw_xml");
                            eprintln!(
                                "  skipped: row={} guid={} {} ({reason})",
                                row.row_num, row.source_db.feed_guid, row.source_db.feed_url
                            );
                            return;
                        };

                        let source_url = row.source_db.feed_url.as_str();
                        let canonical_url = row.fetch.final_url.as_deref().unwrap_or(source_url);
                        let http_status = row.fetch.http_status.unwrap_or(200);
                        let fallback_guid = Some(row.source_db.feed_guid.as_str());
                        let outcome = ingest_cached_feed_with_retries(
                            &client,
                            source_url,
                            canonical_url,
                            http_status,
                            raw_xml,
                            row.fetch.content_sha256.as_deref(),
                            fallback_guid,
                            &config,
                            row.row_num,
                        )
                        .await;

                        match &outcome {
                            CrawlOutcome::Accepted { .. } => {
                                accepted.fetch_add(1, Ordering::Relaxed);
                            }
                            CrawlOutcome::Rejected { .. } => {
                                rejected.fetch_add(1, Ordering::Relaxed);
                            }
                            CrawlOutcome::NoChange => {
                                no_change.fetch_add(1, Ordering::Relaxed);
                            }
                            CrawlOutcome::ParseError(_) => {
                                parse_errors.fetch_add(1, Ordering::Relaxed);
                            }
                            CrawlOutcome::IngestError { .. } | CrawlOutcome::FetchError { .. } => {
                                ingest_errors.fetch_add(1, Ordering::Relaxed);
                            }
                        }

                        eprintln!(
                            "  {outcome}: row={} guid={} title={} {}",
                            row.row_num,
                            row.source_db.feed_guid,
                            row.source_db.title,
                            row.source_db.feed_url,
                        );
                    }
                })
                .collect();

            run_pool(tasks, cli.concurrency).await;

            eprintln!(
                "audit_import: batch done — accepted={} rejected={} no_change={} parse_errors={} ingest_errors={} skipped={}",
                accepted.load(Ordering::Relaxed),
                rejected.load(Ordering::Relaxed),
                no_change.load(Ordering::Relaxed),
                parse_errors.load(Ordering::Relaxed),
                ingest_errors.load(Ordering::Relaxed),
                skipped.load(Ordering::Relaxed),
            );
        }

        cursor = batch_last_row;
        progress.set_last_row(cursor);
        total_processed += batch_len as u64;
        eprintln!("audit_import: cursor={cursor} total_processed={total_processed}");
    }

    eprintln!("audit_import: complete — {total_processed} rows processed");
}
