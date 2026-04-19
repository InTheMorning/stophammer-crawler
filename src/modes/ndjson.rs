use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use rusqlite::Connection;
use serde::Deserialize;

use crate::crawl::{CrawlConfig, CrawlOutcome, ingest_cached_feed};
use crate::pool::run_pool;

const INGEST_ATTEMPTS: u32 = 3;

#[derive(Debug, Deserialize)]
struct SourceDb {
    feed_guid: String,
    feed_url: String,
    title: String,
}

#[derive(Debug, Deserialize)]
struct Fetch {
    final_url: Option<String>,
    http_status: Option<u16>,
    content_sha256: Option<String>,
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct NdjsonRow {
    source_db: SourceDb,
    fetch: Fetch,
    raw_xml: Option<String>,
}

#[derive(Debug)]
struct Candidate {
    row_num: usize,
    source_db: SourceDb,
    fetch: Fetch,
    raw_xml: Option<String>,
}

/// Resume cursor stored in a `SQLite` state DB.
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
                    "ndjson: failed to create state directory {}: {e}",
                    parent.display()
                )
            });
        }

        let conn = Connection::open(path).expect("failed to open ndjson state DB");
        conn.pragma_update(None, "journal_mode", "MEMORY")
            .expect("failed to set ndjson state journal mode");
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
            eprintln!("ndjson: WARNING: failed to persist cursor at row={row_num}: {e}");
        }
    }

    fn reset(&self) {
        self.conn
            .execute(
                "DELETE FROM import_progress WHERE key = 'last_processed_row'",
                [],
            )
            .expect("failed to reset ndjson cursor");
    }
}

fn read_batch(path: &str, start_row: usize, batch_size: usize) -> Vec<Candidate> {
    let file = File::open(path).unwrap_or_else(|e| {
        eprintln!("ndjson: failed to open {path}: {e}");
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
                eprintln!("ndjson: failed reading row {row_num}: {e}");
                continue;
            }
        };

        let row: NdjsonRow = match serde_json::from_str(&line) {
            Ok(row) => row,
            Err(e) => {
                eprintln!("ndjson: failed parsing row {row_num}: {e}");
                continue;
            }
        };

        batch.push(Candidate {
            row_num,
            source_db: row.source_db,
            fetch: row.fetch,
            raw_xml: row.raw_xml,
        });
    }

    batch
}

#[expect(
    clippy::too_many_arguments,
    reason = "cached ingest replay carries source row details plus retry bookkeeping together"
)]
async fn ingest_with_retries(
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
            source_url,
            canonical_url,
            http_status,
            raw_xml,
            content_hash,
            fallback_guid,
            config,
        )
        .await;

        if outcome.is_retryable() && attempt < INGEST_ATTEMPTS {
            let backoff = outcome
                .retry_delay(attempt)
                .unwrap_or_else(|| std::time::Duration::from_secs(1_u64 << (attempt - 1)));
            eprintln!(
                "  ndjson: retrying ingest after attempt {attempt}/{INGEST_ATTEMPTS} for row={row_num} {source_url}: {outcome}"
            );
            tokio::time::sleep(backoff).await;
            attempt += 1;
            continue;
        }

        return outcome;
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "subcommand runner receives all CLI fields destructured from the clap match arm"
)]
#[expect(
    clippy::too_many_lines,
    reason = "linear batch loop with dry-run and live branches kept together for readability"
)]
pub async fn run(
    input: String,
    state: String,
    batch: usize,
    limit: Option<usize>,
    concurrency: usize,
    dry_run: bool,
    reset: bool,
    force: bool,
) {
    let progress = ProgressStore::open(&state);

    if reset {
        progress.reset();
        eprintln!("ndjson: cursor reset to row 0");
    }

    let mut cursor = progress.get_last_row();
    eprintln!("ndjson: input={input} start_row={cursor} batch={batch} concurrency={concurrency}");

    let config = Arc::new(if dry_run {
        CrawlConfig::dry_run(
            "stophammer-crawler/0.1 (ndjson dry-run)",
            std::time::Duration::from_secs(1),
        )
    } else {
        CrawlConfig::from_env_with_force(force)
    });

    let mut total_processed: u64 = 0;

    loop {
        let effective_batch_size = match limit {
            Some(max) => {
                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "total_processed never exceeds NDJSON row count which fits in usize"
                )]
                let remaining = max.saturating_sub(total_processed as usize);
                remaining.min(batch)
            }
            None => batch,
        };
        if effective_batch_size == 0 {
            eprintln!("ndjson: reached --limit after row={cursor}");
            break;
        }

        let rows = read_batch(&input, cursor, effective_batch_size);
        if rows.is_empty() {
            eprintln!("ndjson: no more rows after row={cursor}");
            break;
        }

        let batch_last_row = rows.last().map_or(cursor, |row| row.row_num);
        let batch_len = rows.len();

        if dry_run {
            for row in &rows {
                let has_xml = row.raw_xml.is_some();
                let status = row
                    .fetch
                    .http_status
                    .map_or_else(|| "(none)".to_string(), |s| s.to_string());
                eprintln!(
                    "  [dry-run] row={} guid={} status={} raw_xml={} {}",
                    row.row_num,
                    row.source_db.feed_guid,
                    status,
                    if has_xml { "yes" } else { "no" },
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

            let tasks: Vec<_> = rows
                .into_iter()
                .map(|row| {
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
                        let outcome = ingest_with_retries(
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

            run_pool(tasks, concurrency).await;

            eprintln!(
                "ndjson: batch done — accepted={} rejected={} no_change={} parse_errors={} ingest_errors={} skipped={}",
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
        eprintln!("ndjson: cursor={cursor} total_processed={total_processed}");
    }

    eprintln!("ndjson: complete — {total_processed} rows processed");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn sample_ndjson_line(guid: &str, url: &str, title: &str) -> String {
        serde_json::json!({
            "source_db": {
                "feed_guid": guid,
                "feed_url": url,
                "title": title,
            },
            "fetched_at": 1_700_000_000,
            "fetch": {
                "final_url": url,
                "http_status": 200,
                "content_sha256": "abc123",
                "error": null,
            },
            "raw_xml": "<rss><channel><title>Test</title></channel></rss>",
            "parsed_feed": null,
            "podcast_namespace": null,
            "parse_error": null,
        })
        .to_string()
    }

    #[test]
    fn read_batch_parses_ndjson_rows() {
        let mut tmp = tempfile::NamedTempFile::new().expect("create temp file");
        writeln!(
            tmp,
            "{}",
            sample_ndjson_line("guid-1", "https://a.com/feed.xml", "Feed A")
        )
        .unwrap();
        writeln!(
            tmp,
            "{}",
            sample_ndjson_line("guid-2", "https://b.com/feed.xml", "Feed B")
        )
        .unwrap();
        writeln!(
            tmp,
            "{}",
            sample_ndjson_line("guid-3", "https://c.com/feed.xml", "Feed C")
        )
        .unwrap();

        let path = tmp.path().to_str().unwrap();

        let batch = read_batch(path, 0, 10);
        assert_eq!(batch.len(), 3, "should read all 3 rows");
        assert_eq!(batch[0].row_num, 1);
        assert_eq!(batch[0].source_db.feed_guid, "guid-1");
        assert_eq!(batch[2].row_num, 3);
        assert_eq!(batch[2].source_db.feed_guid, "guid-3");
    }

    #[test]
    fn read_batch_respects_start_row() {
        let mut tmp = tempfile::NamedTempFile::new().expect("create temp file");
        writeln!(
            tmp,
            "{}",
            sample_ndjson_line("guid-1", "https://a.com/feed.xml", "Feed A")
        )
        .unwrap();
        writeln!(
            tmp,
            "{}",
            sample_ndjson_line("guid-2", "https://b.com/feed.xml", "Feed B")
        )
        .unwrap();

        let path = tmp.path().to_str().unwrap();

        let batch = read_batch(path, 1, 10);
        assert_eq!(batch.len(), 1, "should skip row 1");
        assert_eq!(batch[0].row_num, 2);
        assert_eq!(batch[0].source_db.feed_guid, "guid-2");
    }

    #[test]
    fn read_batch_respects_batch_size() {
        let mut tmp = tempfile::NamedTempFile::new().expect("create temp file");
        for i in 1..=5 {
            writeln!(
                tmp,
                "{}",
                sample_ndjson_line(
                    &format!("guid-{i}"),
                    &format!("https://example.com/feed{i}.xml"),
                    &format!("Feed {i}")
                )
            )
            .unwrap();
        }

        let path = tmp.path().to_str().unwrap();

        let batch = read_batch(path, 0, 2);
        assert_eq!(batch.len(), 2, "should cap at batch size");
        assert_eq!(batch[1].row_num, 2);
    }

    #[test]
    fn read_batch_skips_malformed_rows() {
        let mut tmp = tempfile::NamedTempFile::new().expect("create temp file");
        writeln!(
            tmp,
            "{}",
            sample_ndjson_line("guid-1", "https://a.com/feed.xml", "Feed A")
        )
        .unwrap();
        writeln!(tmp, "{{not valid json}}").unwrap();
        writeln!(
            tmp,
            "{}",
            sample_ndjson_line("guid-3", "https://c.com/feed.xml", "Feed C")
        )
        .unwrap();

        let path = tmp.path().to_str().unwrap();

        let batch = read_batch(path, 0, 10);
        assert_eq!(batch.len(), 2, "should skip malformed row");
        assert_eq!(batch[0].source_db.feed_guid, "guid-1");
        assert_eq!(batch[1].source_db.feed_guid, "guid-3");
    }

    #[test]
    fn progress_store_round_trips() {
        let tmp = tempfile::NamedTempFile::new().expect("create temp file");
        let path = tmp.path().to_str().unwrap();
        let store = ProgressStore::open(path);

        assert_eq!(store.get_last_row(), 0, "fresh store starts at 0");

        store.set_last_row(42);
        assert_eq!(store.get_last_row(), 42);

        store.set_last_row(100);
        assert_eq!(store.get_last_row(), 100);

        store.reset();
        assert_eq!(store.get_last_row(), 0, "reset returns to 0");
    }
}
