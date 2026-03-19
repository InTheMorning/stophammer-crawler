use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Datelike, Duration as ChronoDuration, NaiveTime, TimeZone, Utc};
use futures_util::StreamExt;
use rusqlite::Connection;
use tokio::sync::{Mutex, Semaphore};

use crate::crawl::{CrawlConfig, crawl_feed};
use crate::dedup::Dedup;

// ── Podping wire types ────────────────────────────────────────────────────────

#[derive(serde::Deserialize)]
struct PodpingBlock {
    #[serde(default)]
    n: u64,
    #[serde(default)]
    p: Vec<PodpingEntry>,
}

#[derive(serde::Deserialize)]
struct PodpingEntry {
    #[serde(default)]
    p: PodpingMessage,
}

#[derive(serde::Deserialize, Default)]
struct PodpingMessage {
    #[serde(default)]
    medium: Option<String>,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    iris: Option<Vec<String>>,
    // v0.x compat
    #[serde(default)]
    urls: Option<Vec<String>>,
    #[serde(default)]
    url: Option<String>,
}

impl PodpingMessage {
    fn all_urls(&self) -> Vec<&str> {
        let mut urls: Vec<&str> = self
            .iris
            .as_deref()
            .or(self.urls.as_deref())
            .unwrap_or_default()
            .iter()
            .map(String::as_str)
            .collect();
        if let Some(url) = self.url.as_deref() {
            urls.push(url);
        }
        urls
    }
}

fn should_accept(msg: &PodpingMessage) -> bool {
    // Medium: accept "music" or unset (uncategorized worth checking)
    let medium_ok = match msg.medium.as_deref() {
        Some("music") | None => true,
        Some(_) => false,
    };

    // Reason: drop "newValueBlock" (payment-only events)
    let reason_ok = msg.reason.as_deref() != Some("newValueBlock");

    medium_ok && reason_ok
}

fn allowed_operation_id(operation_id: &str) -> bool {
    operation_id == "podping" || operation_id.starts_with("pp_")
}

const HIVE_BLOCK_SECS: i64 = 3;
const DEFAULT_HIVE_API_URL: &str = "https://api.hive.blog";
const DEFAULT_PODPING_WS_URL: &str = "wss://api.livewire.io/ws/podping";
const BLOCK_PROGRESS_LOG_INTERVAL: u64 = 100;
const HIVE_HISTORY_BATCH_SIZE: u64 = 50;

#[derive(serde::Deserialize)]
struct HiveRpcResponse<T> {
    id: u64,
    result: T,
}

#[derive(serde::Deserialize)]
struct HiveBlockApiResult {
    block: HiveBlock,
}

#[derive(serde::Deserialize)]
struct HiveBlock {
    transactions: Vec<HiveTransaction>,
}

#[derive(serde::Deserialize)]
struct HiveTransaction {
    operations: Vec<HiveOperation>,
}

#[derive(serde::Deserialize)]
struct HiveOperation {
    #[serde(rename = "type")]
    op_type: String,
    value: serde_json::Value,
}

#[derive(serde::Deserialize)]
struct HiveCustomJsonOperation {
    id: String,
    json: String,
}

struct HistoryOp {
    payload: PodpingMessage,
}

enum StartSource {
    ExplicitBlock(u64),
    StoredCursor(u64),
    HoursAgo {
        hours: u64,
        current_block: u64,
    },
    Timestamp {
        raw_time: String,
        current_block: u64,
    },
    PreviousSunday {
        current_block: u64,
        start_time: DateTime<Utc>,
    },
}

impl StartSource {
    fn describe(&self) -> String {
        match self {
            Self::ExplicitBlock(block) => format!("--block {block}"),
            Self::StoredCursor(block) => format!("stored cursor block={block}"),
            Self::HoursAgo {
                hours,
                current_block,
            } => {
                format!("--old {hours}h from current Hive head block={current_block}")
            }
            Self::Timestamp {
                raw_time,
                current_block,
            } => {
                format!("--time {raw_time} from current Hive head block={current_block}")
            }
            Self::PreviousSunday {
                current_block,
                start_time,
            } => format!(
                "default previous Sunday UTC start={} from current Hive head block={current_block}",
                start_time.to_rfc3339()
            ),
        }
    }
}

#[derive(Default)]
struct PodpingCounters {
    blocks_seen: u64,
    entries_seen: u64,
    entries_accepted: u64,
    entries_skipped_filter: u64,
    urls_seen: u64,
    urls_launched: u64,
    urls_dedup_skipped: u64,
}

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
                    "failed to create podping state directory {}: {e}",
                    parent.display()
                )
            });
        }

        let conn = Connection::open(path).expect("failed to open podping state DB");
        conn.pragma_update(None, "journal_mode", "MEMORY")
            .expect("failed to set podping state journal mode");
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS podping_progress (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )",
        )
        .expect("failed to create podping progress table");
        Self { conn }
    }

    fn get_last_block(&self) -> Option<u64> {
        self.conn
            .query_row(
                "SELECT value FROM podping_progress WHERE key = 'last_seen_block'",
                [],
                |row| row.get::<_, String>(0),
            )
            .ok()
            .and_then(|value| value.parse().ok())
    }

    fn set_last_block(&self, block: u64) {
        if let Err(e) = self.conn.execute(
            "INSERT INTO podping_progress (key, value) VALUES ('last_seen_block', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            [block.to_string()],
        ) {
            eprintln!("podping: WARNING: failed to persist cursor at block={block}: {e}");
        }
    }
}

#[derive(serde::Deserialize)]
struct HiveRpcEnvelope<T> {
    result: T,
}

#[derive(serde::Deserialize)]
struct HiveDynamicGlobalProperties {
    head_block_number: u64,
}

fn previous_sunday_start_utc(now: DateTime<Utc>) -> DateTime<Utc> {
    let days_since_sunday = i64::from(now.weekday().num_days_from_sunday()) + 7;
    let date = now.date_naive() - ChronoDuration::days(days_since_sunday);
    Utc.from_utc_datetime(&date.and_time(NaiveTime::MIN))
}

fn estimated_block_from_age(current_block: u64, age_secs: i64) -> u64 {
    let blocks_back = age_secs.max(0).saturating_add(HIVE_BLOCK_SECS - 1) / HIVE_BLOCK_SECS;
    current_block.saturating_sub(u64::try_from(blocks_back).unwrap_or(u64::MAX))
}

async fn fetch_current_hive_head_block(client: &reqwest::Client) -> Result<u64, String> {
    let hive_api_url =
        std::env::var("HIVE_API_URL").unwrap_or_else(|_| DEFAULT_HIVE_API_URL.to_string());
    let payload = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "condenser_api.get_dynamic_global_properties",
        "params": []
    });

    let resp = client
        .post(&hive_api_url)
        .json(&payload)
        .send()
        .await
        .map_err(|e| format!("failed to query Hive head block from {hive_api_url}: {e}"))?;

    let resp = resp
        .error_for_status()
        .map_err(|e| format!("Hive head block query returned error from {hive_api_url}: {e}"))?;

    let body: HiveRpcEnvelope<HiveDynamicGlobalProperties> = resp
        .json()
        .await
        .map_err(|e| format!("failed to decode Hive head block response: {e}"))?;

    Ok(body.result.head_block_number)
}

async fn fetch_hive_blocks(
    client: &reqwest::Client,
    block_nums: &[u64],
) -> Result<Vec<(u64, HiveBlock)>, String> {
    if block_nums.is_empty() {
        return Ok(Vec::new());
    }

    let hive_api_url =
        std::env::var("HIVE_API_URL").unwrap_or_else(|_| DEFAULT_HIVE_API_URL.to_string());
    let payload: Vec<_> = block_nums
        .iter()
        .map(|block_num| {
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": block_num,
                "method": "block_api.get_block",
                "params": { "block_num": block_num }
            })
        })
        .collect();

    let resp = client
        .post(&hive_api_url)
        .json(&payload)
        .send()
        .await
        .map_err(|e| format!("failed to fetch Hive blocks from {hive_api_url}: {e}"))?;

    let resp = resp
        .error_for_status()
        .map_err(|e| format!("Hive block fetch returned error from {hive_api_url}: {e}"))?;

    let mut results: Vec<HiveRpcResponse<HiveBlockApiResult>> = resp
        .json()
        .await
        .map_err(|e| format!("failed to decode Hive block batch response: {e}"))?;
    results.sort_by_key(|row| row.id);

    Ok(results
        .into_iter()
        .map(|row| (row.id, row.result.block))
        .collect())
}

fn extract_history_ops(block: HiveBlock) -> Vec<HistoryOp> {
    block
        .transactions
        .into_iter()
        .flat_map(|tx| tx.operations.into_iter())
        .filter_map(|op| {
            if op.op_type != "custom_json_operation" {
                return None;
            }

            let custom_json: HiveCustomJsonOperation = serde_json::from_value(op.value).ok()?;
            if !allowed_operation_id(&custom_json.id) {
                return None;
            }

            let payload: PodpingMessage = serde_json::from_str(&custom_json.json).ok()?;
            Some(HistoryOp { payload })
        })
        .collect()
}

async fn resolve_start_block(
    progress: &ProgressStore,
    client: &reqwest::Client,
    explicit_block: Option<u64>,
    old_hours: Option<u64>,
    start_time: Option<&str>,
) -> Result<(u64, StartSource), String> {
    if let Some(block) = explicit_block {
        return Ok((block, StartSource::ExplicitBlock(block)));
    }

    if let Some(block) = progress.get_last_block() {
        return Ok((block, StartSource::StoredCursor(block)));
    }

    let current_block = fetch_current_hive_head_block(client).await?;
    let now = Utc::now();

    if let Some(hours) = old_hours {
        let age_secs = i64::try_from(hours)
            .unwrap_or(i64::MAX)
            .saturating_mul(60 * 60);
        return Ok((
            estimated_block_from_age(current_block, age_secs),
            StartSource::HoursAgo {
                hours,
                current_block,
            },
        ));
    }

    if let Some(raw_time) = start_time {
        let start = DateTime::parse_from_rfc3339(raw_time)
            .map_err(|e| format!("invalid --time value {raw_time:?}: {e}"))?
            .with_timezone(&Utc);
        let age_secs = now.signed_duration_since(start).num_seconds();
        return Ok((
            estimated_block_from_age(current_block, age_secs),
            StartSource::Timestamp {
                raw_time: raw_time.to_string(),
                current_block,
            },
        ));
    }

    let default_start = previous_sunday_start_utc(now);
    let age_secs = now.signed_duration_since(default_start).num_seconds();
    Ok((
        estimated_block_from_age(current_block, age_secs),
        StartSource::PreviousSunday {
            current_block,
            start_time: default_start,
        },
    ))
}

fn ws_url_for_live_tail(ws_url: &str) -> Result<String, String> {
    reqwest::Url::parse(ws_url)
        .map(|url| url.to_string())
        .map_err(|e| format!("invalid PODPING_WS_URL {ws_url:?}: {e}"))
}

async fn launch_podping_urls(
    payload: &PodpingMessage,
    counters: &mut PodpingCounters,
    dedup: &Arc<Mutex<Dedup>>,
    client: &Arc<reqwest::Client>,
    config: &Arc<CrawlConfig>,
    sem: &Arc<Semaphore>,
) {
    if !should_accept(payload) {
        counters.entries_skipped_filter = counters.entries_skipped_filter.saturating_add(1);
        return;
    }
    counters.entries_accepted = counters.entries_accepted.saturating_add(1);

    for url in payload.all_urls() {
        counters.urls_seen = counters.urls_seen.saturating_add(1);
        let should_crawl = dedup.lock().await.should_process(url);
        if !should_crawl {
            counters.urls_dedup_skipped = counters.urls_dedup_skipped.saturating_add(1);
            continue;
        }
        counters.urls_launched = counters.urls_launched.saturating_add(1);

        let url = url.to_string();
        let client = Arc::clone(client);
        let config = Arc::clone(config);
        let sem = Arc::clone(sem);

        tokio::spawn(async move {
            let _permit = sem.acquire().await.expect("semaphore closed");
            let outcome = crawl_feed(&client, &url, None, &config).await;
            eprintln!("  {outcome}: {url}");
        });
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "Hive catch-up needs the shared crawler state and progress sinks in one helper"
)]
async fn catch_up_via_hive(
    start_block: u64,
    end_block: u64,
    progress: &ProgressStore,
    client: &Arc<reqwest::Client>,
    config: &Arc<CrawlConfig>,
    sem: &Arc<Semaphore>,
    dedup: &Arc<Mutex<Dedup>>,
    counters: &mut PodpingCounters,
) -> Result<(), String> {
    if start_block > end_block {
        return Ok(());
    }

    eprintln!("podping: Hive catch-up from block={start_block} to block={end_block}");
    let mut next_progress_log = start_block;
    let mut batch_start = start_block;

    while batch_start <= end_block {
        let batch_end = end_block.min(batch_start + HIVE_HISTORY_BATCH_SIZE - 1);
        let block_nums: Vec<u64> = (batch_start..=batch_end).collect();
        let blocks = fetch_hive_blocks(client, &block_nums).await?;

        for (block_num, block) in blocks {
            progress.set_last_block(block_num);
            counters.blocks_seen = counters.blocks_seen.saturating_add(1);
            let history_ops = extract_history_ops(block);
            counters.entries_seen = counters
                .entries_seen
                .saturating_add(u64::try_from(history_ops.len()).unwrap_or(u64::MAX));
            for op in history_ops {
                launch_podping_urls(&op.payload, counters, dedup, client, config, sem).await;
            }

            if block_num >= next_progress_log {
                eprintln!(
                    "podping: Hive catch-up at block={} blocks_seen={} accepted_entries={} urls_seen={} launched={} dedup_skipped={}",
                    block_num,
                    counters.blocks_seen,
                    counters.entries_accepted,
                    counters.urls_seen,
                    counters.urls_launched,
                    counters.urls_dedup_skipped
                );
                next_progress_log = block_num.saturating_add(BLOCK_PROGRESS_LOG_INTERVAL);
            }
        }

        batch_start = batch_end.saturating_add(1);
    }

    eprintln!("podping: Hive catch-up complete at block={end_block}");
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "podping mode keeps replay setup, websocket loop, dedup, and cursor persistence in one entrypoint"
)]
pub async fn run(
    state_path: String,
    explicit_block: Option<u64>,
    old_hours: Option<u64>,
    start_time: Option<String>,
    concurrency: usize,
) {
    let ws_url =
        std::env::var("PODPING_WS_URL").unwrap_or_else(|_| DEFAULT_PODPING_WS_URL.to_string());

    let config = Arc::new(CrawlConfig::from_env());
    let client = Arc::new(reqwest::Client::new());
    let sem = Arc::new(Semaphore::new(concurrency));
    let dedup = Arc::new(Mutex::new(Dedup::new()));
    let progress = ProgressStore::open(&state_path);

    let (mut start_block, start_source) = match resolve_start_block(
        &progress,
        &client,
        explicit_block,
        old_hours,
        start_time.as_deref(),
    )
    .await
    {
        Ok(values) => values,
        Err(e) => {
            eprintln!("podping: failed to resolve replay start block: {e}");
            std::process::exit(1);
        }
    };
    let current_head = match fetch_current_hive_head_block(&client).await {
        Ok(block) => block,
        Err(e) => {
            eprintln!("podping: failed to fetch current Hive head block: {e}");
            std::process::exit(1);
        }
    };

    eprintln!(
        "podping: starting from block={start_block}, state={state_path}, concurrency={concurrency}"
    );
    eprintln!("podping: start source = {}", start_source.describe());
    eprintln!("podping: current Hive head block = {current_head}");

    // Periodic dedup cleanup
    let dedup_cleanup = Arc::clone(&dedup);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10 * 60));
        loop {
            interval.tick().await;
            dedup_cleanup.lock().await.cleanup();
        }
    });

    let mut backoff = Duration::from_secs(1);
    let max_backoff = Duration::from_secs(60);
    let mut last_logged_block = 0_u64;
    let mut counters = PodpingCounters::default();
    let mut next_live_block = start_block;

    if start_block <= current_head {
        if let Err(e) = catch_up_via_hive(
            start_block,
            current_head,
            &progress,
            &client,
            &config,
            &sem,
            &dedup,
            &mut counters,
        )
        .await
        {
            eprintln!("podping: Hive catch-up failed: {e}");
            std::process::exit(1);
        }
        next_live_block = current_head.saturating_add(1);
        start_block = next_live_block;
    }

    loop {
        let live_url = match ws_url_for_live_tail(&ws_url) {
            Ok(url) => url,
            Err(e) => {
                eprintln!("podping: invalid websocket URL: {e}");
                std::process::exit(1);
            }
        };
        eprintln!(
            "podping: connecting to live websocket {live_url} expecting next block >= {next_live_block}"
        );

        let ws_result = tokio_tungstenite::connect_async(&live_url).await;

        let ws_stream = match ws_result {
            Ok((stream, _)) => {
                eprintln!("podping: connected");
                backoff = Duration::from_secs(1);
                stream
            }
            Err(e) => {
                eprintln!("podping: connect error: {e}, retrying in {backoff:?}");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
                continue;
            }
        };

        let (_write, mut read) = ws_stream.split();
        let mut first_live_block = true;

        while let Some(msg) = read.next().await {
            let text = match msg {
                Ok(tokio_tungstenite::tungstenite::Message::Text(t)) => t,
                Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => {
                    eprintln!("podping: server closed connection");
                    break;
                }
                Ok(_) => continue,
                Err(e) => {
                    eprintln!("podping: read error: {e}");
                    break;
                }
            };

            let block: PodpingBlock = match serde_json::from_str(&text) {
                Ok(b) => b,
                Err(_) => continue,
            };

            if block.n > 0 {
                if first_live_block {
                    if block.n > next_live_block {
                        eprintln!(
                            "podping: live websocket first block={} is ahead of expected next block={}, filling gap via Hive",
                            block.n, next_live_block
                        );
                        if let Err(e) = catch_up_via_hive(
                            next_live_block,
                            block.n.saturating_sub(1),
                            &progress,
                            &client,
                            &config,
                            &sem,
                            &dedup,
                            &mut counters,
                        )
                        .await
                        {
                            eprintln!("podping: Hive gap fill failed: {e}");
                            break;
                        }
                    } else {
                        eprintln!(
                            "podping: first live websocket block={} expected_next={next_live_block}",
                            block.n
                        );
                    }
                    first_live_block = false;
                }

                progress.set_last_block(block.n);
                start_block = block.n;
                next_live_block = next_live_block.max(block.n.saturating_add(1));
                counters.blocks_seen = counters.blocks_seen.saturating_add(1);
                if last_logged_block == 0
                    || block.n.saturating_sub(last_logged_block) >= BLOCK_PROGRESS_LOG_INTERVAL
                {
                    eprintln!(
                        "podping: cursor advanced to block={} blocks_seen={} entries_seen={} urls_seen={} launched={} dedup_skipped={}",
                        block.n,
                        counters.blocks_seen,
                        counters.entries_seen,
                        counters.urls_seen,
                        counters.urls_launched,
                        counters.urls_dedup_skipped
                    );
                    last_logged_block = block.n;
                }
            }

            for entry in &block.p {
                counters.entries_seen = counters.entries_seen.saturating_add(1);
                launch_podping_urls(&entry.p, &mut counters, &dedup, &client, &config, &sem).await;
            }
        }

        eprintln!(
            "podping: connection ended at block={} blocks_seen={} entries_seen={} accepted_entries={} filtered_entries={} urls_seen={} launched={} dedup_skipped={}",
            start_block,
            counters.blocks_seen,
            counters.entries_seen,
            counters.entries_accepted,
            counters.entries_skipped_filter,
            counters.urls_seen,
            counters.urls_launched,
            counters.urls_dedup_skipped
        );
        eprintln!("podping: disconnected, retrying in {backoff:?}");
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(max_backoff);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn previous_sunday_start_uses_prior_week() {
        let now = DateTime::parse_from_rfc3339("2026-03-18T12:30:00Z")
            .expect("parse time")
            .with_timezone(&Utc);
        let prev = previous_sunday_start_utc(now);
        assert_eq!(prev.to_rfc3339(), "2026-03-08T00:00:00+00:00");
    }

    #[test]
    fn previous_sunday_start_on_sunday_still_uses_previous_sunday() {
        let now = DateTime::parse_from_rfc3339("2026-03-15T09:00:00Z")
            .expect("parse time")
            .with_timezone(&Utc);
        let prev = previous_sunday_start_utc(now);
        assert_eq!(prev.to_rfc3339(), "2026-03-08T00:00:00+00:00");
    }

    #[test]
    fn estimated_block_from_age_uses_three_second_blocks() {
        assert_eq!(estimated_block_from_age(1_000, 0), 1_000);
        assert_eq!(estimated_block_from_age(1_000, 3), 999);
        assert_eq!(estimated_block_from_age(1_000, 7), 997);
    }

    #[test]
    fn ws_url_for_live_tail_preserves_bare_endpoint() {
        let url = ws_url_for_live_tail("wss://api.livewire.io/ws/podping").expect("url");
        assert_eq!(url, "wss://api.livewire.io/ws/podping");
    }

    #[test]
    fn allowed_operation_ids_match_upstream_pattern() {
        assert!(allowed_operation_id("podping"));
        assert!(allowed_operation_id("pp_podcast_update"));
        assert!(!allowed_operation_id("podping-startup"));
        assert!(!allowed_operation_id("something_else"));
    }
}
