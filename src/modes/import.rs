use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use rusqlite::Connection;

use crate::crawl::{crawl_feed, CrawlConfig, CrawlOutcome};
use crate::pool::run_pool;

struct CandidateRow {
    id: i64,
    url: String,
    podcast_guid: Option<String>,
}

/// Resume cursor stored in a separate state DB.
struct ProgressStore {
    conn: Connection,
}

impl ProgressStore {
    fn open(path: &str) -> Self {
        if let Some(parent) = Path::new(path).parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).unwrap_or_else(|e| {
                    panic!("failed to create import state directory {}: {e}", parent.display())
                });
            }
        }

        let conn = Connection::open(path).expect("failed to open import state DB");
        conn.pragma_update(None, "journal_mode", "MEMORY")
            .expect("failed to set import state journal mode");
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS import_progress (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )",
        )
        .expect("failed to create progress table");
        Self { conn }
    }

    fn get_last_id(&self) -> i64 {
        self.conn
            .query_row(
                "SELECT value FROM import_progress WHERE key = 'last_processed_id'",
                [],
                |row| row.get::<_, String>(0),
            )
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    }

    fn set_last_id(&self, id: i64) {
        if let Err(e) = self.conn.execute(
            "INSERT INTO import_progress (key, value) VALUES ('last_processed_id', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            [id.to_string()],
        ) {
            eprintln!("import: WARNING: failed to persist cursor at id={id}: {e}");
        }
    }

    fn reset(&self) {
        self.conn
            .execute(
                "DELETE FROM import_progress WHERE key = 'last_processed_id'",
                [],
            )
            .expect("failed to reset cursor");
    }
}

fn query_batch(db: &Connection, start_id: i64, batch_size: usize) -> Vec<CandidateRow> {
    let mut stmt = db
        .prepare_cached(
            "SELECT id, url, podcastGuid
             FROM   podcasts
             WHERE  dead = 0
               AND (category1 LIKE '%music%' OR category2 LIKE '%music%' OR itunesType = 'serial')
               AND  id > ?1
             ORDER BY id ASC
             LIMIT ?2",
        )
        .expect("failed to prepare query");

    stmt.query_map(
        rusqlite::params![start_id, batch_size],
        |row| {
            Ok(CandidateRow {
                id: row.get(0)?,
                url: row.get(1)?,
                podcast_guid: row.get(2)?,
            })
        },
    )
    .expect("query failed")
    .filter_map(Result::ok)
    .collect()
}

#[allow(clippy::too_many_arguments)]
pub async fn run(
    db_path: String,
    state_path: String,
    batch_size: usize,
    concurrency: usize,
    dry_run: bool,
    reset: bool,
) {
    let progress = ProgressStore::open(&state_path);

    if reset {
        progress.reset();
        eprintln!("import: cursor reset to 0");
    }

    let mut cursor = progress.get_last_id();
    eprintln!("import: starting from id={cursor}, batch={batch_size}, concurrency={concurrency}");

    let pi_db = Connection::open_with_flags(
        &db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .unwrap_or_else(|e| {
        eprintln!("import: failed to open PodcastIndex DB at {db_path}: {e}");
        std::process::exit(1);
    });

    let config = Arc::new(if dry_run {
        // Dry run still needs a config but won't POST
        CrawlConfig {
            crawl_token: String::new(),
            ingest_url: String::new(),
            user_agent: "stophammer-crawler/0.1 (dry-run)".to_string(),
            fetch_timeout: std::time::Duration::from_secs(20),
            ingest_timeout: std::time::Duration::from_secs(10),
        }
    } else {
        CrawlConfig::from_env()
    });

    let client = Arc::new(reqwest::Client::new());
    let mut total_processed: u64 = 0;

    loop {
        let batch = query_batch(&pi_db, cursor, batch_size);
        if batch.is_empty() {
            eprintln!("import: no more candidates after id={cursor}");
            break;
        }

        let batch_max_id = batch.last().map_or(cursor, |r| r.id);
        let batch_len = batch.len();

        if dry_run {
            for row in &batch {
                let guid_display = row.podcast_guid.as_deref().unwrap_or("(none)");
                eprintln!("  [dry-run] id={} guid={guid_display} {}", row.id, row.url);
            }
        } else {
            let accepted = Arc::new(AtomicU64::new(0));
            let rejected = Arc::new(AtomicU64::new(0));
            let no_change = Arc::new(AtomicU64::new(0));
            let errors = Arc::new(AtomicU64::new(0));

            let tasks: Vec<_> = batch
                .into_iter()
                .map(|row| {
                    let client = Arc::clone(&client);
                    let config = Arc::clone(&config);
                    let accepted = Arc::clone(&accepted);
                    let rejected = Arc::clone(&rejected);
                    let no_change = Arc::clone(&no_change);
                    let errors = Arc::clone(&errors);
                    move || async move {
                        let fallback = row.podcast_guid.as_deref();
                        let outcome = crawl_feed(&client, &row.url, fallback, &config).await;

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
                            CrawlOutcome::FetchError(_)
                            | CrawlOutcome::ParseError(_)
                            | CrawlOutcome::IngestError(_) => {
                                errors.fetch_add(1, Ordering::Relaxed);
                            }
                        }

                        eprintln!("  {outcome}: id={} {}", row.id, row.url);
                    }
                })
                .collect();

            run_pool(tasks, concurrency).await;

            eprintln!(
                "import: batch done — accepted={} rejected={} no_change={} errors={}",
                accepted.load(Ordering::Relaxed),
                rejected.load(Ordering::Relaxed),
                no_change.load(Ordering::Relaxed),
                errors.load(Ordering::Relaxed),
            );
        }

        cursor = batch_max_id;
        progress.set_last_id(cursor);
        total_processed += batch_len as u64;
        eprintln!("import: cursor={cursor} total_processed={total_processed}");
    }

    eprintln!("import: complete — {total_processed} feeds processed");
}
