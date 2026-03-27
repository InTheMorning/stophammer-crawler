use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use reqwest::Client;
use rusqlite::{Connection, params};
use tokio::sync::{Mutex, Semaphore};

use crate::crawl::{CrawlConfig, crawl_feed};
use crate::dedup::Dedup;

fn create_async_client() -> reqwest::Client {
    reqwest::Client::builder()
        .use_rustls_tls()
        .connect_timeout(Duration::from_secs(10))
        .build()
        .expect("failed to create async HTTP client")
}

const GOSSIP_LISTENER_SSE_URL: &str = "http://localhost:8089/events";

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
        let result = self.conn.execute_batch(&format!(
            "INSERT OR REPLACE INTO gossip_progress (key, value) VALUES ('archive_cursor_created_at', '{}');
             INSERT OR REPLACE INTO gossip_progress (key, value) VALUES ('archive_cursor_hash', '{}');",
            cursor.created_at, cursor.hash
        ));
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
}

#[derive(Default)]
struct GossipCounters {
    notifications_seen: u64,
    notifications_accepted: u64,
    notifications_filtered: u64,
    urls_seen: u64,
    urls_launched: u64,
    urls_dedup_skipped: u64,
}

#[derive(Default)]
struct SseParser {
    line_buffer: String,
    event_data: String,
    in_event: bool,
}

impl SseParser {
    fn push_chunk(&mut self, text: &str) -> Vec<String> {
        self.line_buffer.push_str(text);

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

async fn launch_gossip_urls(
    notification: &GossipNotification,
    counters: &mut GossipCounters,
    dedup: &Arc<Mutex<Dedup>>,
    client: &Arc<Client>,
    config: &Arc<CrawlConfig>,
    sem: &Arc<Semaphore>,
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
        counters.urls_launched += 1;

        let url = url.to_string();
        let client = Arc::clone(client);
        let config = Arc::clone(config);
        let sem = Arc::clone(sem);

        tokio::spawn(async move {
            let _permit = sem.acquire().await.expect("semaphore closed");
            let outcome = crawl_feed(&client, &url, None, &config).await;
            if !(quiet && outcome.is_medium_rejection()) {
                eprintln!("  {outcome}: {url}");
            }
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
    dedup: &Arc<Mutex<Dedup>>,
    client: &Arc<Client>,
    config: &Arc<CrawlConfig>,
    sem: &Arc<Semaphore>,
    progress: &ProgressStore,
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

    let messages: Vec<ArchiveMessage> = if let Some(cur) = cursor {
        eprintln!(
            "gossip: replay from archive cursor ({}, {}) db={archive_path}",
            cur.created_at, cur.hash
        );
        let mut stmt = match conn.prepare(
            "SELECT hash, payload, created_at FROM messages
             WHERE created_at > ?1 OR (created_at = ?1 AND hash > ?2)
             ORDER BY created_at ASC, hash ASC",
        ) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("gossip: ERROR: failed to prepare cursor query: {e}");
                return GossipCounters::default();
            }
        };
        match stmt.query_map(params![cur.created_at, cur.hash], |row| {
            Ok(ArchiveMessage {
                hash: row.get(0)?,
                payload: row.get::<_, String>(1)?.into_bytes(),
                created_at: row.get(2)?,
            })
        }) {
            Ok(rows) => rows.filter_map(Result::ok).collect(),
            Err(e) => {
                eprintln!("gossip: ERROR: failed to query archive: {e}");
                return GossipCounters::default();
            }
        }
    } else {
        let since_i64 = i64::try_from(since).unwrap_or(i64::MAX);
        eprintln!("gossip: replay from archive since={since} db={archive_path}");
        let mut stmt = match conn.prepare(
            "SELECT hash, payload, created_at FROM messages
             WHERE created_at >= ?1
             ORDER BY created_at ASC, hash ASC",
        ) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("gossip: ERROR: failed to prepare since query: {e}");
                return GossipCounters::default();
            }
        };
        match stmt.query_map(params![since_i64], |row| {
            Ok(ArchiveMessage {
                hash: row.get(0)?,
                payload: row.get::<_, String>(1)?.into_bytes(),
                created_at: row.get(2)?,
            })
        }) {
            Ok(rows) => rows.filter_map(Result::ok).collect(),
            Err(e) => {
                eprintln!("gossip: ERROR: failed to query archive: {e}");
                return GossipCounters::default();
            }
        }
    };

    eprintln!("gossip: found {} messages in archive", messages.len());

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

        if should_accept(&notif) {
            counters.notifications_accepted += 1;

            for url in notif.all_urls() {
                counters.urls_seen += 1;
                let should_crawl = dedup.lock().await.should_process(url);
                if !should_crawl {
                    counters.urls_dedup_skipped += 1;
                    continue;
                }
                counters.urls_launched += 1;

                let url = url.to_string();
                let client = Arc::clone(client);
                let config = Arc::clone(config);
                let sem = Arc::clone(sem);

                tokio::spawn(async move {
                    let _permit = sem.acquire().await.expect("semaphore closed");
                    let outcome = crawl_feed(&client, &url, None, &config).await;
                    if !(quiet && outcome.is_medium_rejection()) {
                        eprintln!("  {outcome}: {url}");
                    }
                });
            }
        } else {
            counters.notifications_filtered += 1;
        }
    }

    if let Some(ref cur) = last_cursor {
        progress.set_archive_cursor(cur);
    }

    eprintln!(
        "gossip: replay complete: seen={} accepted={} filtered={} urls_seen={} launched={} dedup_skipped={}",
        counters.notifications_seen,
        counters.notifications_accepted,
        counters.notifications_filtered,
        counters.urls_seen,
        counters.urls_launched,
        counters.urls_dedup_skipped
    );

    counters
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
    progress: &ProgressStore,
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

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| format!("SSE stream error: {e}"))?;
        let text = String::from_utf8_lossy(&chunk);

        for event_data in parser.push_chunk(&text) {
            if let Ok(notif) = serde_json::from_str::<GossipNotification>(&event_data) {
                counters.notifications_seen += 1;

                let ts = notif.effective_timestamp();
                progress.set_last_timestamp(ts);

                launch_gossip_urls(&notif, counters, dedup, client, config, sem, quiet).await;
            }
        }
    }

    Ok(())
}

#[allow(clippy::too_many_lines)]
pub async fn run(
    state_path: String,
    sse_url: Option<String>,
    archive_db: Option<String>,
    since_hours: Option<u64>,
    concurrency: usize,
    quiet: bool,
) {
    let sse_url = sse_url.unwrap_or_else(|| GOSSIP_LISTENER_SSE_URL.to_string());

    let config = Arc::new(CrawlConfig::from_env());
    let client = Arc::new(reqwest::Client::new());
    let sem = Arc::new(Semaphore::new(concurrency));
    let dedup = Arc::new(Mutex::new(Dedup::new()));
    let progress = ProgressStore::open(&state_path);

    // Resolve the archive cursor: stored cursor > legacy migration > --since-hours > default
    let archive_cursor = progress.get_archive_cursor();
    let since = if let Some(hours) = since_hours {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .saturating_sub(hours * 3600)
    } else if archive_cursor.is_some() {
        0 // cursor-based replay, since is unused
    } else if let Some(ts) = progress.get_last_timestamp() {
        ts
    } else {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .saturating_sub(86400)
    };

    if let Some(ref _archive) = archive_db {
        if let Some(ref cur) = archive_cursor {
            eprintln!(
                "gossip: archive-backed mode, cursor=({}, {}), state={state_path}, concurrency={concurrency}",
                cur.created_at, cur.hash
            );
        } else {
            eprintln!(
                "gossip: archive-backed mode, since={since}, state={state_path}, concurrency={concurrency}"
            );
        }
    } else if let Some(ts) = progress.get_last_timestamp() {
        eprintln!(
            "gossip: live-only mode, state={state_path}, last_seen_timestamp={ts}, concurrency={concurrency}"
        );
    } else {
        eprintln!("gossip: live-only mode, state={state_path}, concurrency={concurrency}");
    }
    eprintln!("gossip: SSE endpoint={sse_url}");

    if let Some(ref archive) = archive_db {
        // Try legacy migration if no archive cursor exists yet
        let effective_cursor = if archive_cursor.is_some() {
            archive_cursor
        } else if progress.get_last_timestamp().is_some() {
            let archive_conn = Connection::open_with_flags(
                archive,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                    | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .ok();
            archive_conn.and_then(|c| progress.migrate_legacy_cursor(&c))
        } else {
            None
        };

        let archive_counters = replay_from_archive(
            archive,
            effective_cursor.as_ref(),
            since,
            &dedup,
            &client,
            &config,
            &sem,
            &progress,
            quiet,
        )
        .await;

        if archive_counters.urls_launched > 0 {
            eprintln!(
                "gossip: replay stats: seen={} accepted={} filtered={} urls_seen={} launched={} dedup_skipped={}",
                archive_counters.notifications_seen,
                archive_counters.notifications_accepted,
                archive_counters.notifications_filtered,
                archive_counters.urls_seen,
                archive_counters.urls_launched,
                archive_counters.urls_dedup_skipped
            );
        }
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
                "gossip: session stats: seen={} accepted={} filtered={} urls_seen={} launched={} dedup_skipped={}",
                session_counters.notifications_seen,
                session_counters.notifications_accepted,
                session_counters.notifications_filtered,
                session_counters.urls_seen,
                session_counters.urls_launched,
                session_counters.urls_dedup_skipped
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
        conn.execute_batch(
            "CREATE TABLE messages (hash TEXT PRIMARY KEY, payload BLOB)",
        )
        .expect("create table");
        conn.execute(
            "INSERT INTO messages (hash, payload) VALUES ('h', 'p')",
            [],
        )
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
        let payload = r#"{"version":"1.1","sender":"abc","iris":["https://example.com"],"timestamp":100}"#;
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
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
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
}
