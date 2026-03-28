use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use reqwest::Client;
use rusqlite::{Connection, params};
use tokio::sync::{Mutex, Semaphore};

use crate::crawl::{CrawlConfig, CrawlReport, crawl_feed_report};
use crate::dedup::Dedup;

fn create_async_client() -> reqwest::Client {
    reqwest::Client::builder()
        .use_rustls_tls()
        .connect_timeout(Duration::from_secs(10))
        .build()
        .expect("failed to create async HTTP client")
}

const GOSSIP_LISTENER_SSE_URL: &str = "http://localhost:8089/events";
const ARCHIVE_REPLAY_BATCH_SIZE: usize = 500;
const RECONCILIATION_INITIAL_DELAY_SECS: u64 = 10;
const RECONCILIATION_STEADY_STATE_SECS: u64 = 60;
const RECONCILIATION_MAX_INTERVAL_SECS: u64 = 300;

#[derive(Debug, serde::Deserialize)]
struct GossipNotification {
    #[allow(dead_code)]
    version: String,
    #[allow(dead_code)]
    sender: String,
    #[allow(dead_code)]
    medium: Option<String>,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    iris: Option<Vec<String>>,
    #[allow(dead_code)]
    signature: Option<String>,
    #[allow(dead_code)]
    sig_status: Option<String>,
    #[serde(default)]
    timestamp: Option<u64>,
}

impl GossipNotification {
    fn all_urls(&self) -> Vec<&str> {
        self.iris
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(String::as_str)
            .collect()
    }

    fn effective_timestamp(&self) -> u64 {
        self.timestamp.unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
        })
    }
}

fn should_accept(msg: &GossipNotification) -> bool {
    // Accept all notifications - the parser will verify podcast:medium
    // Only reject newValueBlock (payment-only events)
    msg.reason.as_deref() != Some("newValueBlock")
}

/// Archive cursor stored as a `(created_at, hash)` pair for stable replay
/// ordering even when multiple messages share the same `created_at` value.
#[derive(Debug, Clone)]
struct ArchiveCursor {
    created_at: i64,
    hash: String,
}

struct ProgressStore {
    conn: Connection,
}

impl ProgressStore {
    fn open(path: &str) -> Self {
        if let Some(parent) = std::path::Path::new(path).parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).unwrap_or_else(|e| {
                panic!(
                    "failed to create gossip state directory {}: {e}",
                    parent.display()
                )
            });
        }

        let conn = Connection::open(path).expect("failed to open gossip state DB");
        conn.pragma_update(None, "journal_mode", "MEMORY")
            .expect("failed to set gossip state journal mode");
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS gossip_progress (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS gossip_feed_memory (
                feed_url           TEXT PRIMARY KEY,
                fetch_http_status  INTEGER,
                fetch_outcome      TEXT NOT NULL,
                outcome_reason     TEXT,
                raw_medium         TEXT,
                parsed_feed_guid   TEXT,
                attempt_duration_ms INTEGER NOT NULL DEFAULT 0,
                first_attempted_at INTEGER NOT NULL,
                last_attempted_at  INTEGER NOT NULL,
                attempt_count      INTEGER NOT NULL DEFAULT 1
            )",
        )
        .expect("failed to create gossip state tables");

        ProgressStore { conn }
    }

    fn get_last_timestamp(&self) -> Option<u64> {
        self.conn
            .query_row(
                "SELECT value FROM gossip_progress WHERE key = 'last_seen_timestamp'",
                [],
                |row| row.get::<_, String>(0),
            )
            .ok()
            .and_then(|v| v.parse().ok())
    }

    fn set_last_timestamp(&self, timestamp: u64) {
        let result = self.conn.execute(
            "INSERT OR REPLACE INTO gossip_progress (key, value) VALUES ('last_seen_timestamp', ?1)",
            params![timestamp.to_string()],
        );
        if let Err(e) = result {
            eprintln!("gossip: WARNING: failed to persist cursor at timestamp={timestamp}: {e}");
        }
    }

    fn get_archive_cursor(&self) -> Option<ArchiveCursor> {
        let created_at: i64 = self
            .conn
            .query_row(
                "SELECT value FROM gossip_progress WHERE key = 'archive_cursor_created_at'",
                [],
                |row| row.get::<_, String>(0),
            )
            .ok()
            .and_then(|v| v.parse().ok())?;
        let hash: String = self
            .conn
            .query_row(
                "SELECT value FROM gossip_progress WHERE key = 'archive_cursor_hash'",
                [],
                |row| row.get::<_, String>(0),
            )
            .ok()?;
        Some(ArchiveCursor { created_at, hash })
    }

    fn set_archive_cursor(&self, cursor: &ArchiveCursor) {
        let result = self
            .conn
            .execute(
                "INSERT OR REPLACE INTO gossip_progress (key, value) VALUES ('archive_cursor_created_at', ?1)",
                rusqlite::params![cursor.created_at],
            )
            .and_then(|_| {
                self.conn.execute(
                    "INSERT OR REPLACE INTO gossip_progress (key, value) VALUES ('archive_cursor_hash', ?1)",
                    rusqlite::params![cursor.hash],
                )
            });
        if let Err(e) = result {
            eprintln!(
                "gossip: WARNING: failed to persist archive cursor at ({}, {}): {e}",
                cursor.created_at, cursor.hash
            );
        }
    }

    /// Migrate legacy `last_seen_timestamp` to an archive cursor by looking up
    /// the first matching row in `archive.db`. Returns the migrated cursor if
    /// successful, or `None` if migration is not applicable.
    fn migrate_legacy_cursor(&self, archive_conn: &Connection) -> Option<ArchiveCursor> {
        let timestamp = self.get_last_timestamp()?;
        if self.get_archive_cursor().is_some() {
            return None;
        }

        let ts_i64 = i64::try_from(timestamp).unwrap_or(i64::MAX);
        let cursor = archive_conn
            .query_row(
                "SELECT created_at, hash FROM messages WHERE created_at >= ?1 ORDER BY created_at ASC, hash ASC LIMIT 1",
                params![ts_i64],
                |row| {
                    Ok(ArchiveCursor {
                        created_at: row.get(0)?,
                        hash: row.get(1)?,
                    })
                },
            )
            .ok()?;

        self.set_archive_cursor(&cursor);
        let _ = self.conn.execute(
            "DELETE FROM gossip_progress WHERE key = 'last_seen_timestamp'",
            [],
        );
        eprintln!(
            "gossip: migrated legacy cursor timestamp={timestamp} to archive cursor ({}, {})",
            cursor.created_at, cursor.hash
        );
        Some(cursor)
    }

    fn upsert_feed_memory(&self, url: &str, report: &CrawlReport, duration_ms: i64) {
        let now = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        )
        .unwrap_or(i64::MAX);
        let result = self.conn.execute(
            "INSERT INTO gossip_feed_memory (
                feed_url, fetch_http_status, fetch_outcome, outcome_reason,
                raw_medium, parsed_feed_guid, attempt_duration_ms,
                first_attempted_at, last_attempted_at, attempt_count
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8, 1)
            ON CONFLICT(feed_url) DO UPDATE SET
                fetch_http_status = excluded.fetch_http_status,
                fetch_outcome = excluded.fetch_outcome,
                outcome_reason = excluded.outcome_reason,
                raw_medium = excluded.raw_medium,
                parsed_feed_guid = excluded.parsed_feed_guid,
                attempt_duration_ms = excluded.attempt_duration_ms,
                last_attempted_at = excluded.last_attempted_at,
                attempt_count = attempt_count + 1",
            params![
                url,
                report.fetch_http_status.map(i64::from),
                report.outcome.label(),
                report.outcome.reason(),
                report.raw_medium.as_deref(),
                report.parsed_feed_guid.as_deref(),
                duration_ms,
                now,
            ],
        );
        if let Err(e) = result {
            eprintln!("gossip: WARNING: failed to upsert feed memory for {url}: {e}");
        }
    }

    /// Check if a feed should be skipped based on prior crawl results.
    /// Returns the skip reason if the feed is known irrelevant, or `None` to proceed.
    fn should_skip_feed(&self, url: &str, skip_ttl_days: Option<u64>) -> Option<String> {
        let row = self
            .conn
            .query_row(
                "SELECT fetch_http_status, fetch_outcome, outcome_reason, raw_medium, last_attempted_at
                 FROM gossip_feed_memory WHERE feed_url = ?1",
                params![url],
                |row| {
                    Ok((
                        row.get::<_, Option<i64>>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, i64>(4)?,
                    ))
                },
            )
            .ok()?;

        let (http_status, outcome, reason, medium, last_attempted) = row;

        // Check TTL expiry — if set and expired, re-evaluate
        if let Some(ttl_days) = skip_ttl_days {
            let now = i64::try_from(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            )
            .unwrap_or(i64::MAX);
            let ttl_secs = i64::try_from(ttl_days)
                .unwrap_or(i64::MAX)
                .saturating_mul(86400);
            if now - last_attempted > ttl_secs {
                return None;
            }
        }

        // Skip if HTTP 200 and medium is not music/publisher
        if http_status == Some(200)
            && let Some(ref m) = medium
            && m != "music"
            && m != "publisher"
        {
            return Some(format!("known non-music medium: {m}"));
        }

        // Skip if medium-gate rejection (includes absent podcast:medium)
        if outcome == "rejected"
            && let Some(ref r) = reason
            && r.starts_with("[medium_music]")
        {
            return Some(format!("prior medium-gate rejection: {r}"));
        }

        None
    }
}

#[derive(Default)]
struct GossipCounters {
    notifications_seen: u64,
    notifications_accepted: u64,
    notifications_filtered: u64,
    urls_seen: u64,
    urls_launched: u64,
    urls_dedup_skipped: u64,
    urls_feed_memory_skipped: u64,
}

/// Maximum bytes buffered in a single SSE line before it is discarded.
/// Protects against a misbehaving source that never emits newlines.
const SSE_MAX_LINE_BYTES: usize = 1024 * 1024; // 1 MiB

#[derive(Default)]
struct SseParser {
    line_buffer: String,
    event_data: String,
    in_event: bool,
}

impl SseParser {
    fn push_chunk(&mut self, text: &str) -> Vec<String> {
        self.line_buffer.push_str(text);

        // Guard against unbounded growth when the source sends no newlines.
        if self.line_buffer.len() > SSE_MAX_LINE_BYTES {
            eprintln!(
                "gossip: WARNING: SSE line buffer exceeded {SSE_MAX_LINE_BYTES} bytes without a newline; discarding buffer",
            );
            self.line_buffer.clear();
            self.event_data.clear();
            self.in_event = false;
            return Vec::new();
        }

        let mut completed_events = Vec::new();

        while let Some(newline_idx) = self.line_buffer.find('\n') {
            let mut line = self.line_buffer[..newline_idx].to_string();
            if line.ends_with('\r') {
                line.pop();
            }
            self.line_buffer.drain(..=newline_idx);

            if line.starts_with("event:") {
                let event_type = line.trim_start_matches("event:").trim();
                self.in_event = event_type == "podping";
            } else if line.starts_with("data:") && self.in_event {
                let data = line.trim_start_matches("data:").trim();
                if !self.event_data.is_empty() {
                    self.event_data.push('\n');
                }
                self.event_data.push_str(data);
            } else if line.is_empty() {
                if self.in_event && !self.event_data.is_empty() {
                    completed_events.push(std::mem::take(&mut self.event_data));
                } else {
                    self.event_data.clear();
                }
                self.in_event = false;
            }
        }

        completed_events
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "shared handler threads through dedup, crawl infra, progress, and skip policy"
)]
async fn process_notification_urls(
    notification: &GossipNotification,
    counters: &mut GossipCounters,
    dedup: &Arc<Mutex<Dedup>>,
    client: &Arc<Client>,
    config: &Arc<CrawlConfig>,
    sem: &Arc<Semaphore>,
    progress: &Arc<std::sync::Mutex<ProgressStore>>,
    skip_db: &Arc<std::sync::Mutex<crate::feed_skip::FeedSkipDb>>,
    skip_known_non_music: bool,
    skip_ttl_days: Option<u64>,
    quiet: bool,
) {
    if !should_accept(notification) {
        counters.notifications_filtered += 1;
        return;
    }
    counters.notifications_accepted += 1;

    for url in notification.all_urls() {
        counters.urls_seen += 1;
        let should_crawl = dedup.lock().await.should_process(url);
        if !should_crawl {
            counters.urls_dedup_skipped += 1;
            continue;
        }

        if skip_known_non_music {
            // Check shared cross-mode skip DB first
            if let Some(reason) = skip_db.lock().unwrap().should_skip(url, skip_ttl_days) {
                counters.urls_feed_memory_skipped += 1;
                if !quiet {
                    eprintln!("  skipped (shared: {reason}): {url}");
                }
                continue;
            }

            // Then check mode-specific feed memory
            if let Some(reason) = progress
                .lock()
                .unwrap()
                .should_skip_feed(url, skip_ttl_days)
            {
                counters.urls_feed_memory_skipped += 1;
                if !quiet {
                    eprintln!("  skipped ({reason}): {url}");
                }
                continue;
            }
        }

        counters.urls_launched += 1;

        let url = url.to_string();
        let client = Arc::clone(client);
        let config = Arc::clone(config);
        let sem = Arc::clone(sem);
        let progress = Arc::clone(progress);
        let skip_db = Arc::clone(skip_db);

        tokio::spawn(async move {
            let _permit = sem.acquire().await.expect("semaphore closed");
            let start = Instant::now();
            let report = crawl_feed_report(&client, &url, None, &config).await;
            let duration_ms = i64::try_from(start.elapsed().as_millis()).unwrap_or(i64::MAX);

            if !(quiet && report.outcome.is_medium_rejection()) {
                eprintln!("  {}: {url}", report.outcome);
            }

            skip_db
                .lock()
                .unwrap()
                .record_outcome(&url, &report, "gossip");
            progress
                .lock()
                .unwrap()
                .upsert_feed_memory(&url, &report, duration_ms);
        });
    }
}

/// Validate that `archive.db` has the expected `messages(hash, payload, created_at)` schema.
fn validate_archive_schema(conn: &Connection) -> Result<(), String> {
    let mut stmt = conn
        .prepare("PRAGMA table_info(messages)")
        .map_err(|e| format!("failed to query archive schema: {e}"))?;

    let columns: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(|e| format!("failed to read archive schema: {e}"))?
        .filter_map(Result::ok)
        .collect();

    if columns.is_empty() {
        return Err("archive.db has no 'messages' table".to_string());
    }

    for required in ["hash", "payload", "created_at"] {
        if !columns.iter().any(|c| c == required) {
            return Err(format!(
                "archive.db 'messages' table missing required column '{required}'"
            ));
        }
    }

    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM messages", [], |row| row.get(0))
        .map_err(|e| format!("failed to count archive messages: {e}"))?;
    if count == 0 {
        return Err("archive.db 'messages' table is empty".to_string());
    }

    Ok(())
}

/// Return the newest `(created_at, hash)` row in the archive.
fn get_archive_high_water_mark(conn: &Connection) -> Option<ArchiveCursor> {
    conn.query_row(
        "SELECT created_at, hash FROM messages ORDER BY created_at DESC, hash DESC LIMIT 1",
        [],
        |row| {
            Ok(ArchiveCursor {
                created_at: row.get(0)?,
                hash: row.get(1)?,
            })
        },
    )
    .ok()
}

/// Return the oldest `(created_at, hash)` row in the archive.
fn get_archive_oldest_cursor(conn: &Connection) -> Option<ArchiveCursor> {
    conn.query_row(
        "SELECT created_at, hash FROM messages ORDER BY created_at ASC, hash ASC LIMIT 1",
        [],
        |row| {
            Ok(ArchiveCursor {
                created_at: row.get(0)?,
                hash: row.get(1)?,
            })
        },
    )
    .ok()
}

/// Fail-closed check: is the stored archive cursor still within the retained archive window?
fn check_archive_continuity(conn: &Connection, cursor: &ArchiveCursor) -> Result<(), String> {
    let min_created_at: i64 = conn
        .query_row("SELECT MIN(created_at) FROM messages", [], |row| row.get(0))
        .map_err(|e| format!("failed to query archive min created_at: {e}"))?;

    if cursor.created_at < min_created_at {
        return Err(format!(
            "archive gap: cursor created_at={} is older than the oldest retained row \
             created_at={} ({} seconds of notifications may be lost). \
             Remove gossip_state.db to re-bootstrap from the oldest archive row.",
            cursor.created_at,
            min_created_at,
            min_created_at - cursor.created_at,
        ));
    }
    Ok(())
}

/// A single row from the archive replay query, carrying the cursor position.
struct ArchiveMessage {
    created_at: i64,
    hash: String,
    payload: Vec<u8>,
}

#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "archive replay threads through shared clients, counters, progress, and output policy"
)]
async fn replay_from_archive(
    archive_path: &str,
    cursor: Option<&ArchiveCursor>,
    since: u64,
    high_water: &ArchiveCursor,
    dedup: &Arc<Mutex<Dedup>>,
    client: &Arc<Client>,
    config: &Arc<CrawlConfig>,
    sem: &Arc<Semaphore>,
    concurrency: usize,
    progress: &Arc<std::sync::Mutex<ProgressStore>>,
    skip_db: &Arc<std::sync::Mutex<crate::feed_skip::FeedSkipDb>>,
    skip_known_non_music: bool,
    skip_ttl_days: Option<u64>,
    quiet: bool,
) -> GossipCounters {
    let conn = match Connection::open_with_flags(
        archive_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    ) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("gossip: ERROR: failed to open archive db: {e}");
            return GossipCounters::default();
        }
    };

    if let Err(e) = validate_archive_schema(&conn) {
        eprintln!("gossip: ERROR: archive schema validation failed: {e}");
        return GossipCounters::default();
    }

    eprintln!(
        "gossip: replaying archive in batches of {ARCHIVE_REPLAY_BATCH_SIZE} \
         (high-water: ({}, {}))",
        high_water.created_at, high_water.hash,
    );

    let mut total_counters = GossipCounters::default();
    let mut current_cursor: Option<ArchiveCursor> = cursor.cloned();
    let since_i64 = i64::try_from(since).unwrap_or(i64::MAX);
    let limit = i64::try_from(ARCHIVE_REPLAY_BATCH_SIZE).unwrap_or(i64::MAX);
    let mut batch_number = 0u64;

    loop {
        let messages: Vec<ArchiveMessage> = if let Some(ref cur) = current_cursor {
            let mut stmt = match conn.prepare(
                "SELECT hash, payload, created_at FROM messages
                 WHERE (created_at > ?1 OR (created_at = ?1 AND hash > ?2))
                   AND (created_at < ?3 OR (created_at = ?3 AND hash <= ?4))
                 ORDER BY created_at ASC, hash ASC
                 LIMIT ?5",
            ) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("gossip: ERROR: failed to prepare cursor batch query: {e}");
                    break;
                }
            };
            match stmt.query_map(
                params![
                    cur.created_at,
                    cur.hash,
                    high_water.created_at,
                    high_water.hash,
                    limit,
                ],
                |row| {
                    Ok(ArchiveMessage {
                        hash: row.get(0)?,
                        payload: row.get::<_, String>(1)?.into_bytes(),
                        created_at: row.get(2)?,
                    })
                },
            ) {
                Ok(rows) => rows.filter_map(Result::ok).collect(),
                Err(e) => {
                    eprintln!("gossip: ERROR: failed to query archive batch: {e}");
                    break;
                }
            }
        } else {
            let mut stmt = match conn.prepare(
                "SELECT hash, payload, created_at FROM messages
                 WHERE created_at >= ?1
                   AND (created_at < ?2 OR (created_at = ?2 AND hash <= ?3))
                 ORDER BY created_at ASC, hash ASC
                 LIMIT ?4",
            ) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("gossip: ERROR: failed to prepare since batch query: {e}");
                    break;
                }
            };
            match stmt.query_map(
                params![since_i64, high_water.created_at, high_water.hash, limit],
                |row| {
                    Ok(ArchiveMessage {
                        hash: row.get(0)?,
                        payload: row.get::<_, String>(1)?.into_bytes(),
                        created_at: row.get(2)?,
                    })
                },
            ) {
                Ok(rows) => rows.filter_map(Result::ok).collect(),
                Err(e) => {
                    eprintln!("gossip: ERROR: failed to query archive batch: {e}");
                    break;
                }
            }
        };

        if messages.is_empty() {
            break;
        }

        let batch_count = messages.len();
        batch_number += 1;
        let pre_launched = total_counters.urls_launched;
        let pre_skipped =
            total_counters.urls_dedup_skipped + total_counters.urls_feed_memory_skipped;

        for msg in &messages {
            let notif: GossipNotification = match serde_json::from_slice(&msg.payload) {
                Ok(n) => n,
                Err(_) => continue,
            };

            total_counters.notifications_seen += 1;
            current_cursor = Some(ArchiveCursor {
                created_at: msg.created_at,
                hash: msg.hash.clone(),
            });

            process_notification_urls(
                &notif,
                &mut total_counters,
                dedup,
                client,
                config,
                sem,
                progress,
                &skip_db,
                skip_known_non_music,
                skip_ttl_days,
                quiet,
            )
            .await;
        }

        // Drain: wait for all in-flight crawl tasks to complete before next batch
        let drain = sem
            .acquire_many(u32::try_from(concurrency).expect("concurrency fits u32"))
            .await
            .expect("semaphore closed");
        drop(drain);

        // Persist cursor after each batch for resume granularity
        if let Some(ref cur) = current_cursor {
            progress.lock().unwrap().set_archive_cursor(cur);
        }

        let batch_launched = total_counters.urls_launched - pre_launched;
        let batch_skipped = (total_counters.urls_dedup_skipped
            + total_counters.urls_feed_memory_skipped)
            - pre_skipped;
        eprintln!(
            "gossip: replay batch {batch_number}: {batch_count} notifications, \
             launched={batch_launched}, skipped={batch_skipped}",
        );

        if batch_count < ARCHIVE_REPLAY_BATCH_SIZE {
            break;
        }
    }

    eprintln!(
        "gossip: replay complete ({batch_number} batches): seen={} accepted={} filtered={} \
         urls_seen={} launched={} dedup_skipped={} memory_skipped={}",
        total_counters.notifications_seen,
        total_counters.notifications_accepted,
        total_counters.notifications_filtered,
        total_counters.urls_seen,
        total_counters.urls_launched,
        total_counters.urls_dedup_skipped,
        total_counters.urls_feed_memory_skipped,
    );

    total_counters
}

#[allow(
    clippy::too_many_arguments,
    reason = "SSE streaming threads through shared clients, counters, progress, and output policy"
)]
async fn stream_sse_events(
    sse_url: &str,
    dedup: &Arc<Mutex<Dedup>>,
    client: &Arc<Client>,
    config: &Arc<CrawlConfig>,
    sem: &Arc<Semaphore>,
    counters: &mut GossipCounters,
    progress: &Arc<std::sync::Mutex<ProgressStore>>,
    skip_db: &Arc<std::sync::Mutex<crate::feed_skip::FeedSkipDb>>,
    skip_known_non_music: bool,
    skip_ttl_days: Option<u64>,
    quiet: bool,
) -> Result<(), String> {
    eprintln!("gossip: connecting to SSE at {sse_url}");

    let async_client = create_async_client();
    let response = async_client
        .get(sse_url)
        .header("Accept", "text/event-stream")
        .send()
        .await
        .map_err(|e| format!("failed to connect to SSE: {e}"))?;

    if !response.status().is_success() {
        return Err(format!("SSE endpoint returned {}", response.status()));
    }

    eprintln!("gossip: connected, streaming events...");

    let stream = response.bytes_stream();
    futures_util::pin_mut!(stream);

    let mut parser = SseParser::default();

    loop {
        let chunk = match tokio::time::timeout(Duration::from_secs(120), stream.next()).await {
            Ok(Some(result)) => result.map_err(|e| format!("SSE stream error: {e}"))?,
            Ok(None) => break,
            Err(_) => return Err("SSE stream stalled: no data for 120s".to_string()),
        };
        let text = String::from_utf8_lossy(&chunk);

        for event_data in parser.push_chunk(&text) {
            if let Ok(notif) = serde_json::from_str::<GossipNotification>(&event_data) {
                counters.notifications_seen += 1;

                let ts = notif.effective_timestamp();
                progress.lock().unwrap().set_last_timestamp(ts);

                process_notification_urls(
                    &notif,
                    counters,
                    dedup,
                    client,
                    config,
                    sem,
                    progress,
                    skip_db,
                    skip_known_non_music,
                    skip_ttl_days,
                    quiet,
                )
                .await;
            }
        }
    }

    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "reconciliation threads through shared clients, progress, and output policy"
)]
async fn reconcile_archive_batch(
    archive_path: &str,
    dedup: &Arc<Mutex<Dedup>>,
    client: &Arc<Client>,
    config: &Arc<CrawlConfig>,
    sem: &Arc<Semaphore>,
    progress: &Arc<std::sync::Mutex<ProgressStore>>,
    skip_db: &Arc<std::sync::Mutex<crate::feed_skip::FeedSkipDb>>,
    skip_known_non_music: bool,
    skip_ttl_days: Option<u64>,
    quiet: bool,
) -> u64 {
    // Collect all messages synchronously, then drop the connection before any .await
    let messages: Vec<ArchiveMessage> = {
        let conn = match Connection::open_with_flags(
            archive_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        ) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("gossip: reconciliation: failed to open archive: {e}");
                return 0;
            }
        };

        let Some(cursor) = progress.lock().unwrap().get_archive_cursor() else {
            return 0;
        };

        let mut stmt = match conn.prepare(
            "SELECT hash, payload, created_at FROM messages
             WHERE created_at > ?1 OR (created_at = ?1 AND hash > ?2)
             ORDER BY created_at ASC, hash ASC
             LIMIT 1000",
        ) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("gossip: reconciliation: failed to prepare query: {e}");
                return 0;
            }
        };

        match stmt.query_map(params![cursor.created_at, cursor.hash], |row| {
            Ok(ArchiveMessage {
                hash: row.get(0)?,
                payload: row.get::<_, String>(1)?.into_bytes(),
                created_at: row.get(2)?,
            })
        }) {
            Ok(rows) => rows.filter_map(Result::ok).collect(),
            Err(e) => {
                eprintln!("gossip: reconciliation: failed to query archive: {e}");
                return 0;
            }
        }
    };

    if messages.is_empty() {
        return 0;
    }

    let count = messages.len() as u64;
    let mut counters = GossipCounters::default();
    let mut last_cursor: Option<ArchiveCursor> = None;

    for msg in &messages {
        let notif: GossipNotification = match serde_json::from_slice(&msg.payload) {
            Ok(n) => n,
            Err(_) => continue,
        };

        counters.notifications_seen += 1;
        last_cursor = Some(ArchiveCursor {
            created_at: msg.created_at,
            hash: msg.hash.clone(),
        });

        process_notification_urls(
            &notif,
            &mut counters,
            dedup,
            client,
            config,
            sem,
            progress,
            skip_db,
            skip_known_non_music,
            skip_ttl_days,
            quiet,
        )
        .await;
    }

    if let Some(ref cur) = last_cursor {
        progress.lock().unwrap().set_archive_cursor(cur);
    }

    eprintln!(
        "gossip: reconciliation: {count} rows, launched={}, skipped={}",
        counters.urls_launched,
        counters.urls_dedup_skipped + counters.urls_feed_memory_skipped,
    );

    count
}

#[allow(
    clippy::too_many_arguments,
    reason = "reconciliation loop threads through shared clients, progress, and output policy"
)]
async fn archive_reconciliation_loop(
    archive_path: String,
    dedup: Arc<Mutex<Dedup>>,
    client: Arc<Client>,
    config: Arc<CrawlConfig>,
    sem: Arc<Semaphore>,
    progress: Arc<std::sync::Mutex<ProgressStore>>,
    skip_db: Arc<std::sync::Mutex<crate::feed_skip::FeedSkipDb>>,
    skip_known_non_music: bool,
    skip_ttl_days: Option<u64>,
    quiet: bool,
) {
    let mut interval_secs = RECONCILIATION_INITIAL_DELAY_SECS;

    loop {
        tokio::time::sleep(Duration::from_secs(interval_secs)).await;

        let rows = reconcile_archive_batch(
            &archive_path,
            &dedup,
            &client,
            &config,
            &sem,
            &progress,
            &skip_db,
            skip_known_non_music,
            skip_ttl_days,
            quiet,
        )
        .await;

        if interval_secs == RECONCILIATION_INITIAL_DELAY_SECS {
            // After the first (fast) reconciliation, switch to steady-state cadence
            interval_secs = RECONCILIATION_STEADY_STATE_SECS;
        } else if rows > 0 {
            interval_secs = RECONCILIATION_STEADY_STATE_SECS;
        } else {
            interval_secs = std::cmp::min(
                interval_secs.saturating_mul(2),
                RECONCILIATION_MAX_INTERVAL_SECS,
            );
        }
    }
}

#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
pub async fn run(
    state_path: String,
    skip_db_path: String,
    sse_url: Option<String>,
    archive_db: Option<String>,
    since_hours: Option<u64>,
    concurrency: usize,
    skip_known_non_music: bool,
    skip_ttl_days: Option<u64>,
    quiet: bool,
) {
    let sse_url = sse_url.unwrap_or_else(|| GOSSIP_LISTENER_SSE_URL.to_string());

    let config = Arc::new(CrawlConfig::from_env());
    let client = Arc::new(create_async_client());
    let sem = Arc::new(Semaphore::new(concurrency));
    let dedup = Arc::new(Mutex::new(Dedup::new()));
    let progress_store = ProgressStore::open(&state_path);
    let skip_db = Arc::new(std::sync::Mutex::new(
        crate::feed_skip::FeedSkipDb::open(&skip_db_path),
    ));

    let archive_cursor = progress_store.get_archive_cursor();

    // --- Archive-backed startup: validate, check continuity, resolve cursor ---
    let (effective_cursor, high_water, since) = if let Some(ref archive) = archive_db {
        let archive_conn = match Connection::open_with_flags(
            archive,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        ) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("gossip: FATAL: failed to open archive db {archive}: {e}");
                std::process::exit(1);
            }
        };

        if let Err(e) = validate_archive_schema(&archive_conn) {
            eprintln!("gossip: FATAL: archive schema validation failed: {e}");
            std::process::exit(1);
        }

        // Resolve cursor: stored > legacy migration > none
        let effective_cursor = if let Some(ref cur) = archive_cursor {
            if let Err(e) = check_archive_continuity(&archive_conn, cur) {
                eprintln!("gossip: FATAL: {e}");
                std::process::exit(1);
            }
            archive_cursor.clone()
        } else if progress_store.get_last_timestamp().is_some() {
            progress_store.migrate_legacy_cursor(&archive_conn)
        } else {
            None
        };

        let high_water =
            get_archive_high_water_mark(&archive_conn).expect("archive validated non-empty above");

        // Resolve since: --since-hours > cursor (unused) > oldest archive row
        let since = if let Some(hours) = since_hours {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                .saturating_sub(hours * 3600)
        } else if effective_cursor.is_some() {
            0 // cursor-based replay, since is unused
        } else {
            // Bootstrap from oldest available archive row
            get_archive_oldest_cursor(&archive_conn)
                .map_or(0, |c| u64::try_from(c.created_at).unwrap_or(0))
        };

        (effective_cursor, Some(high_water), since)
    } else {
        // Live-only mode: no archive validation
        let since = if let Some(hours) = since_hours {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                .saturating_sub(hours * 3600)
        } else if let Some(ts) = progress_store.get_last_timestamp() {
            ts
        } else {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                .saturating_sub(86400)
        };
        (None, None, since)
    };

    // --- Startup logging ---
    if let Some(ref _archive) = archive_db {
        if let Some(ref cur) = effective_cursor {
            eprintln!(
                "gossip: archive-backed mode, cursor=({}, {}), state={state_path}, concurrency={concurrency}",
                cur.created_at, cur.hash,
            );
        } else {
            eprintln!(
                "gossip: archive-backed mode, since={since}, state={state_path}, concurrency={concurrency}",
            );
        }
    } else if let Some(ts) = progress_store.get_last_timestamp() {
        eprintln!(
            "gossip: live-only mode (best-effort, not restart-safe), state={state_path}, \
             last_seen_timestamp={ts}, concurrency={concurrency}",
        );
    } else {
        eprintln!(
            "gossip: live-only mode (best-effort, not restart-safe), state={state_path}, \
             concurrency={concurrency}",
        );
    }
    if skip_known_non_music {
        eprintln!(
            "gossip: skip-known-non-music enabled{}",
            skip_ttl_days.map_or(String::new(), |d| format!(", ttl={d}d")),
        );
    }
    eprintln!("gossip: SSE endpoint={sse_url}");

    let progress = Arc::new(std::sync::Mutex::new(progress_store));

    // --- Archive replay with batched backpressure ---
    if let Some(ref archive) = archive_db {
        let hw = high_water.as_ref().expect("set above for archive mode");
        replay_from_archive(
            archive,
            effective_cursor.as_ref(),
            since,
            hw,
            &dedup,
            &client,
            &config,
            &sem,
            concurrency,
            &progress,
            &skip_db,
            skip_known_non_music,
            skip_ttl_days,
            quiet,
        )
        .await;

        // Spawn archive reconciliation to catch missed SSE events
        let recon_archive = archive.clone();
        let recon_dedup = Arc::clone(&dedup);
        let recon_client = Arc::clone(&client);
        let recon_config = Arc::clone(&config);
        let recon_sem = Arc::clone(&sem);
        let recon_progress = Arc::clone(&progress);
        let recon_skip_db = Arc::clone(&skip_db);
        tokio::spawn(async move {
            archive_reconciliation_loop(
                recon_archive,
                recon_dedup,
                recon_client,
                recon_config,
                recon_sem,
                recon_progress,
                recon_skip_db,
                skip_known_non_music,
                skip_ttl_days,
                quiet,
            )
            .await;
        });
    }

    let dedup_cleanup = Arc::clone(&dedup);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10 * 60));
        loop {
            interval.tick().await;
            dedup_cleanup.lock().await.cleanup();
        }
    });

    loop {
        let mut session_counters = GossipCounters::default();

        match stream_sse_events(
            &sse_url,
            &dedup,
            &client,
            &config,
            &sem,
            &mut session_counters,
            &progress,
            &skip_db,
            skip_known_non_music,
            skip_ttl_days,
            quiet,
        )
        .await
        {
            Ok(()) => {
                eprintln!("gossip: SSE stream ended, reconnecting...");
            }
            Err(e) => {
                eprintln!("gossip: SSE error: {e}, reconnecting in 5s...");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }

        if session_counters.notifications_seen > 0 || session_counters.urls_launched > 0 {
            eprintln!(
                "gossip: session stats: seen={} accepted={} filtered={} urls_seen={} launched={} dedup_skipped={} memory_skipped={}",
                session_counters.notifications_seen,
                session_counters.notifications_accepted,
                session_counters.notifications_filtered,
                session_counters.urls_seen,
                session_counters.urls_launched,
                session_counters.urls_dedup_skipped,
                session_counters.urls_feed_memory_skipped
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn create_test_archive(messages: &[(&str, &str, i64)]) -> Connection {
        let conn = Connection::open_in_memory().expect("in-memory db");
        conn.execute_batch(
            "CREATE TABLE messages (
                hash       TEXT PRIMARY KEY,
                payload    BLOB,
                created_at INTEGER
            )",
        )
        .expect("create messages table");
        for (hash, payload, created_at) in messages {
            conn.execute(
                "INSERT INTO messages (hash, payload, created_at) VALUES (?1, ?2, ?3)",
                params![hash, payload.as_bytes(), created_at],
            )
            .expect("insert message");
        }
        conn
    }

    #[test]
    fn sse_parser_handles_data_line_split_across_chunks() {
        let mut parser = SseParser::default();

        let first = "event: podping\ndata: {\"iris\":[\"https://exa";
        let second = "mple.com/feed.xml\"],\"timestamp\":123}\n\n";

        assert!(parser.push_chunk(first).is_empty());

        let events = parser.push_chunk(second);
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0],
            "{\"iris\":[\"https://example.com/feed.xml\"],\"timestamp\":123}"
        );
    }

    #[test]
    fn sse_parser_joins_multiple_data_lines() {
        let mut parser = SseParser::default();

        let events = parser.push_chunk("event: podping\ndata: {\"a\":1,\ndata: \"b\":2}\n\n");

        assert_eq!(events, vec!["{\"a\":1,\n\"b\":2}".to_string()]);
    }

    #[test]
    fn progress_store_creates_feed_memory_table() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let db_path = tempdir.path().join("gossip_state.db");
        let store = ProgressStore::open(db_path.to_str().expect("utf-8 path"));

        let count: i64 = store
            .conn
            .query_row("SELECT COUNT(*) FROM gossip_feed_memory", [], |row| {
                row.get(0)
            })
            .expect("feed memory table should exist");
        assert_eq!(count, 0);
    }

    #[test]
    fn archive_cursor_round_trips() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let db_path = tempdir.path().join("gossip_state.db");
        let store = ProgressStore::open(db_path.to_str().expect("utf-8 path"));

        assert!(store.get_archive_cursor().is_none());

        let cursor = ArchiveCursor {
            created_at: 1700000000,
            hash: "abc123def456".to_string(),
        };
        store.set_archive_cursor(&cursor);

        let loaded = store.get_archive_cursor().expect("cursor should exist");
        assert_eq!(loaded.created_at, 1700000000);
        assert_eq!(loaded.hash, "abc123def456");
    }

    #[test]
    fn legacy_migration_converts_timestamp_to_archive_cursor() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let db_path = tempdir.path().join("gossip_state.db");
        let store = ProgressStore::open(db_path.to_str().expect("utf-8 path"));
        store.set_last_timestamp(1000);

        let archive_conn = create_test_archive(&[
            ("hash_a", "{}", 999),
            ("hash_b", "{}", 1000),
            ("hash_c", "{}", 1001),
        ]);

        let migrated = store
            .migrate_legacy_cursor(&archive_conn)
            .expect("migration should succeed");
        assert_eq!(migrated.created_at, 1000);
        assert_eq!(migrated.hash, "hash_b");

        // Legacy key should be removed
        assert!(store.get_last_timestamp().is_none());

        // Archive cursor should be persisted
        let stored = store.get_archive_cursor().expect("cursor should exist");
        assert_eq!(stored.created_at, 1000);
        assert_eq!(stored.hash, "hash_b");
    }

    #[test]
    fn legacy_migration_skips_when_archive_cursor_exists() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let db_path = tempdir.path().join("gossip_state.db");
        let store = ProgressStore::open(db_path.to_str().expect("utf-8 path"));
        store.set_last_timestamp(1000);
        store.set_archive_cursor(&ArchiveCursor {
            created_at: 999,
            hash: "existing".to_string(),
        });

        let archive_conn = create_test_archive(&[("hash_a", "{}", 1000)]);

        assert!(store.migrate_legacy_cursor(&archive_conn).is_none());
    }

    #[test]
    fn validate_archive_schema_accepts_valid_db() {
        let conn = create_test_archive(&[("hash_a", "{}", 1000)]);
        assert!(validate_archive_schema(&conn).is_ok());
    }

    #[test]
    fn validate_archive_schema_rejects_empty_db() {
        let conn = create_test_archive(&[]);
        let result = validate_archive_schema(&conn);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("empty"));
    }

    #[test]
    fn validate_archive_schema_rejects_missing_table() {
        let conn = Connection::open_in_memory().expect("in-memory db");
        let result = validate_archive_schema(&conn);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("no 'messages' table"));
    }

    #[test]
    fn validate_archive_schema_rejects_missing_column() {
        let conn = Connection::open_in_memory().expect("in-memory db");
        conn.execute_batch("CREATE TABLE messages (hash TEXT PRIMARY KEY, payload BLOB)")
            .expect("create table");
        conn.execute("INSERT INTO messages (hash, payload) VALUES ('h', 'p')", [])
            .expect("insert");
        let result = validate_archive_schema(&conn);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("created_at"));
    }

    #[test]
    fn cursor_based_replay_orders_by_created_at_then_hash() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let archive_path = tempdir.path().join("archive.db");
        let conn = Connection::open(&archive_path).expect("open");
        conn.execute_batch(
            "CREATE TABLE messages (
                hash TEXT PRIMARY KEY,
                payload BLOB,
                created_at INTEGER
            )",
        )
        .expect("create");

        // Three messages at the same created_at, hashes out of alphabetical order
        let payload =
            r#"{"version":"1.1","sender":"abc","iris":["https://example.com"],"timestamp":100}"#;
        conn.execute(
            "INSERT INTO messages VALUES ('zzz', ?1, 1000)",
            params![payload],
        )
        .expect("insert zzz");
        conn.execute(
            "INSERT INTO messages VALUES ('aaa', ?1, 1000)",
            params![payload],
        )
        .expect("insert aaa");
        conn.execute(
            "INSERT INTO messages VALUES ('mmm', ?1, 1000)",
            params![payload],
        )
        .expect("insert mmm");
        drop(conn);

        // Replay from cursor at (1000, "aaa") should skip "aaa" and return "mmm" then "zzz"
        let conn = Connection::open_with_flags(
            &archive_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .expect("open read-only");

        let cursor = ArchiveCursor {
            created_at: 1000,
            hash: "aaa".to_string(),
        };
        let mut stmt = conn
            .prepare(
                "SELECT hash, payload, created_at FROM messages
                 WHERE created_at > ?1 OR (created_at = ?1 AND hash > ?2)
                 ORDER BY created_at ASC, hash ASC",
            )
            .expect("prepare");
        let results: Vec<String> = stmt
            .query_map(params![cursor.created_at, cursor.hash], |row| {
                row.get::<_, String>(0)
            })
            .expect("query")
            .filter_map(Result::ok)
            .collect();

        assert_eq!(results, vec!["mmm", "zzz"]);
    }

    #[test]
    fn gossip_state_migration_preserves_existing_db() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let db_path = tempdir.path().join("gossip_state.db");

        // Create a legacy-style DB with only gossip_progress
        let conn = Connection::open(&db_path).expect("open");
        conn.execute_batch(
            "CREATE TABLE gossip_progress (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
        )
        .expect("create legacy table");
        conn.execute(
            "INSERT INTO gossip_progress (key, value) VALUES ('last_seen_timestamp', '1700000000')",
            [],
        )
        .expect("insert legacy cursor");
        drop(conn);

        // Opening with ProgressStore should add feed_memory table without breaking
        let store = ProgressStore::open(db_path.to_str().expect("utf-8 path"));
        assert_eq!(store.get_last_timestamp(), Some(1_700_000_000));

        let count: i64 = store
            .conn
            .query_row("SELECT COUNT(*) FROM gossip_feed_memory", [], |row| {
                row.get(0)
            })
            .expect("feed memory table should be created");
        assert_eq!(count, 0);
    }

    fn make_report(
        outcome_label: &str,
        reason: Option<&str>,
        http_status: Option<u16>,
        medium: Option<&str>,
        guid: Option<&str>,
    ) -> CrawlReport {
        use crate::crawl::CrawlOutcome;
        let outcome = match outcome_label {
            "accepted" => CrawlOutcome::Accepted {
                warnings: Vec::new(),
            },
            "rejected" => CrawlOutcome::Rejected {
                reason: reason.unwrap_or("unknown").to_string(),
                warnings: Vec::new(),
            },
            "no_change" => CrawlOutcome::NoChange,
            "fetch_error" => CrawlOutcome::FetchError {
                reason: reason.unwrap_or("timeout").to_string(),
                retryable: true,
                retry_after_secs: None,
            },
            _ => CrawlOutcome::ParseError(reason.unwrap_or("bad xml").to_string()),
        };
        CrawlReport {
            outcome,
            fetch_http_status: http_status,
            raw_medium: medium.map(ToString::to_string),
            parsed_feed_guid: guid.map(ToString::to_string),
            final_url: None,
            content_sha256: None,
            raw_xml: None,
            parsed_feed: None,
        }
    }

    #[test]
    fn upsert_feed_memory_stores_and_updates() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let db_path = tempdir.path().join("gossip_state.db");
        let store = ProgressStore::open(db_path.to_str().expect("utf-8 path"));

        let report = make_report("accepted", None, Some(200), Some("music"), Some("guid-1"));
        store.upsert_feed_memory("https://example.com/feed.xml", &report, 150);

        // Verify stored values
        let (outcome, medium, count): (String, Option<String>, i64) = store
            .conn
            .query_row(
                "SELECT fetch_outcome, raw_medium, attempt_count FROM gossip_feed_memory WHERE feed_url = ?1",
                params!["https://example.com/feed.xml"],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("row should exist");
        assert_eq!(outcome, "accepted");
        assert_eq!(medium.as_deref(), Some("music"));
        assert_eq!(count, 1);

        // Second upsert increments attempt_count
        let report2 = make_report(
            "rejected",
            Some("[medium_music] not music"),
            Some(200),
            Some("podcast"),
            None,
        );
        store.upsert_feed_memory("https://example.com/feed.xml", &report2, 200);

        let (outcome, medium, count): (String, Option<String>, i64) = store
            .conn
            .query_row(
                "SELECT fetch_outcome, raw_medium, attempt_count FROM gossip_feed_memory WHERE feed_url = ?1",
                params!["https://example.com/feed.xml"],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("row should exist");
        assert_eq!(outcome, "rejected");
        assert_eq!(medium.as_deref(), Some("podcast"));
        assert_eq!(count, 2);
    }

    #[test]
    fn should_skip_non_music_medium() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let db_path = tempdir.path().join("gossip_state.db");
        let store = ProgressStore::open(db_path.to_str().expect("utf-8 path"));

        let report = make_report("accepted", None, Some(200), Some("podcast"), None);
        store.upsert_feed_memory("https://example.com/podcast.xml", &report, 100);

        let skip = store.should_skip_feed("https://example.com/podcast.xml", None);
        assert!(skip.is_some());
        assert!(skip.unwrap().contains("non-music medium"));
    }

    #[test]
    fn should_skip_medium_gate_rejection() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let db_path = tempdir.path().join("gossip_state.db");
        let store = ProgressStore::open(db_path.to_str().expect("utf-8 path"));

        let report = make_report(
            "rejected",
            Some("[medium_music] medium is absent"),
            Some(200),
            None,
            None,
        );
        store.upsert_feed_memory("https://example.com/no-medium.xml", &report, 100);

        let skip = store.should_skip_feed("https://example.com/no-medium.xml", None);
        assert!(skip.is_some());
        assert!(skip.unwrap().contains("medium-gate rejection"));
    }

    #[test]
    fn should_not_skip_music_feed() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let db_path = tempdir.path().join("gossip_state.db");
        let store = ProgressStore::open(db_path.to_str().expect("utf-8 path"));

        let report = make_report("accepted", None, Some(200), Some("music"), Some("guid-1"));
        store.upsert_feed_memory("https://example.com/music.xml", &report, 100);

        assert!(
            store
                .should_skip_feed("https://example.com/music.xml", None)
                .is_none()
        );
    }

    #[test]
    fn should_not_skip_publisher_feed() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let db_path = tempdir.path().join("gossip_state.db");
        let store = ProgressStore::open(db_path.to_str().expect("utf-8 path"));

        let report = make_report(
            "accepted",
            None,
            Some(200),
            Some("publisher"),
            Some("guid-2"),
        );
        store.upsert_feed_memory("https://example.com/pub.xml", &report, 100);

        assert!(
            store
                .should_skip_feed("https://example.com/pub.xml", None)
                .is_none()
        );
    }

    #[test]
    fn should_not_skip_fetch_errors() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let db_path = tempdir.path().join("gossip_state.db");
        let store = ProgressStore::open(db_path.to_str().expect("utf-8 path"));

        let report = make_report(
            "fetch_error",
            Some("http 404 Not Found"),
            Some(404),
            None,
            None,
        );
        store.upsert_feed_memory("https://example.com/gone.xml", &report, 100);

        assert!(
            store
                .should_skip_feed("https://example.com/gone.xml", None)
                .is_none()
        );
    }

    #[test]
    fn should_not_skip_unknown_feed() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let db_path = tempdir.path().join("gossip_state.db");
        let store = ProgressStore::open(db_path.to_str().expect("utf-8 path"));

        // Feed not in memory at all
        assert!(
            store
                .should_skip_feed("https://example.com/new.xml", None)
                .is_none()
        );
    }

    #[test]
    fn skip_ttl_expires_old_decisions() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let db_path = tempdir.path().join("gossip_state.db");
        let store = ProgressStore::open(db_path.to_str().expect("utf-8 path"));

        let report = make_report("accepted", None, Some(200), Some("podcast"), None);
        store.upsert_feed_memory("https://example.com/old.xml", &report, 100);

        // Manually backdate last_attempted_at to 60 days ago
        let old_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
            - (60 * 86400);
        store
            .conn
            .execute(
                "UPDATE gossip_feed_memory SET last_attempted_at = ?1 WHERE feed_url = ?2",
                params![old_ts, "https://example.com/old.xml"],
            )
            .expect("backdate");

        // Without TTL: still skipped
        assert!(
            store
                .should_skip_feed("https://example.com/old.xml", None)
                .is_some()
        );

        // With 30-day TTL: expired, should re-evaluate
        assert!(
            store
                .should_skip_feed("https://example.com/old.xml", Some(30))
                .is_none()
        );

        // With 90-day TTL: not yet expired, still skipped
        assert!(
            store
                .should_skip_feed("https://example.com/old.xml", Some(90))
                .is_some()
        );
    }

    #[test]
    fn high_water_mark_returns_newest_row() {
        let conn = create_test_archive(&[
            ("hash_a", "{}", 1000),
            ("hash_b", "{}", 2000),
            ("hash_c", "{}", 1500),
        ]);
        let hw = get_archive_high_water_mark(&conn).expect("should find max");
        assert_eq!(hw.created_at, 2000);
        assert_eq!(hw.hash, "hash_b");
    }

    #[test]
    fn high_water_mark_tiebreaks_by_hash_desc() {
        let conn = create_test_archive(&[
            ("aaa", "{}", 1000),
            ("zzz", "{}", 1000),
            ("mmm", "{}", 1000),
        ]);
        let hw = get_archive_high_water_mark(&conn).expect("should find max");
        assert_eq!(hw.created_at, 1000);
        assert_eq!(hw.hash, "zzz");
    }

    #[test]
    fn oldest_cursor_returns_earliest_row() {
        let conn = create_test_archive(&[
            ("hash_a", "{}", 2000),
            ("hash_b", "{}", 1000),
            ("hash_c", "{}", 1500),
        ]);
        let oldest = get_archive_oldest_cursor(&conn).expect("should find min");
        assert_eq!(oldest.created_at, 1000);
        assert_eq!(oldest.hash, "hash_b");
    }

    #[test]
    fn continuity_check_accepts_valid_cursor() {
        let conn = create_test_archive(&[("hash_a", "{}", 1000), ("hash_b", "{}", 2000)]);
        let cursor = ArchiveCursor {
            created_at: 1000,
            hash: "hash_a".to_string(),
        };
        assert!(check_archive_continuity(&conn, &cursor).is_ok());
    }

    #[test]
    fn continuity_check_accepts_cursor_between_rows() {
        let conn = create_test_archive(&[("hash_a", "{}", 1000), ("hash_b", "{}", 2000)]);
        let cursor = ArchiveCursor {
            created_at: 1500,
            hash: "hash_x".to_string(),
        };
        assert!(check_archive_continuity(&conn, &cursor).is_ok());
    }

    #[test]
    fn continuity_check_rejects_stale_cursor() {
        let conn = create_test_archive(&[("hash_a", "{}", 1000), ("hash_b", "{}", 2000)]);
        let cursor = ArchiveCursor {
            created_at: 500,
            hash: "old".to_string(),
        };
        let result = check_archive_continuity(&conn, &cursor);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("archive gap"));
    }
}
