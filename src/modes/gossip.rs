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
            )",
        )
        .expect("failed to create gossip progress table");

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

#[allow(
    clippy::too_many_arguments,
    reason = "archive replay threads through shared clients, counters, progress, and output policy"
)]
async fn replay_from_archive(
    archive_path: &str,
    since: u64,
    dedup: &Arc<Mutex<Dedup>>,
    client: &Arc<Client>,
    config: &Arc<CrawlConfig>,
    sem: &Arc<Semaphore>,
    progress: &ProgressStore,
    quiet: bool,
) -> GossipCounters {
    eprintln!("gossip: replay from archive db={archive_path} since={since}");

    let conn = match Connection::open(archive_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("gossip: ERROR: failed to open archive db: {e}");
            return GossipCounters::default();
        }
    };

    let mut stmt = match conn
        .prepare("SELECT payload FROM messages WHERE created_at >= ?1 ORDER BY created_at ASC")
    {
        Ok(s) => s,
        Err(e) => {
            eprintln!("gossip: ERROR: failed to prepare query: {e}");
            return GossipCounters::default();
        }
    };

    let mut counters = GossipCounters::default();

    let since_i64 = i64::try_from(since).unwrap_or(i64::MAX);

    let payloads: Vec<Vec<u8>> =
        match stmt.query_map(params![since_i64], |row| row.get::<_, String>(0)) {
            Ok(rows) => {
                let mut collected = Vec::new();
                for text in rows.flatten() {
                    collected.push(text.into_bytes());
                }
                collected
            }
            Err(e) => {
                eprintln!("gossip: ERROR: failed to query messages: {e}");
                return GossipCounters::default();
            }
        };

    eprintln!("gossip: found {} messages in archive", payloads.len());

    let mut last_ts = since;

    for payload in payloads {
        let notif: GossipNotification = match serde_json::from_slice(&payload) {
            Ok(n) => n,
            Err(_) => continue,
        };

        counters.notifications_seen += 1;

        let ts = notif.effective_timestamp();
        if ts > last_ts {
            last_ts = ts;
            progress.set_last_timestamp(ts);
        }

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

    let last_timestamp = progress.get_last_timestamp();
    let since = if let Some(hours) = since_hours {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .saturating_sub(hours * 3600)
    } else if let Some(ts) = last_timestamp {
        ts
    } else {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .saturating_sub(86400)
    };

    if archive_db.is_some() {
        eprintln!(
            "gossip: starting replay from timestamp={since}, state={state_path}, concurrency={concurrency}"
        );
    } else if let Some(ts) = last_timestamp {
        eprintln!(
            "gossip: live-only mode, state={state_path}, last_seen_timestamp={ts}, concurrency={concurrency}"
        );
    } else {
        eprintln!("gossip: live-only mode, state={state_path}, concurrency={concurrency}");
    }
    eprintln!("gossip: SSE endpoint={sse_url}");

    if let Some(ref archive) = archive_db {
        let archive_counters = replay_from_archive(
            archive, since, &dedup, &client, &config, &sem, &progress, quiet,
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
    use super::SseParser;

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
}
