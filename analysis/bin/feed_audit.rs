use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::Parser;
use reqwest::header::{HeaderMap, RETRY_AFTER};
use rusqlite::{Connection, OpenFlags};
use serde::Serialize;
use sha2::{Digest, Sha256};
use stophammer_parser::profile;

#[derive(Parser, Debug)]
#[command(
    name = "feed_audit",
    about = "Fetch and dump full RSS feeds from a stophammer DB"
)]
struct Cli {
    /// Path to a populated stophammer SQLite DB.
    #[arg(long, default_value = "./analysis/data/stophammer-feeds.db")]
    db: String,

    /// Output NDJSON file containing one record per fetched feed.
    #[arg(long, default_value = "./analysis/data/feed_audit.ndjson")]
    output: String,

    /// Maximum number of feeds to fetch.
    #[arg(long)]
    limit: Option<usize>,

    /// HTTP timeout per fetch.
    #[arg(long, default_value_t = 15)]
    timeout_secs: u64,

    /// Minimum delay after a successful fetch before hitting the same host again.
    #[arg(long, default_value_t = 1500)]
    success_delay_ms: u64,

    /// Initial backoff after a failed fetch against the same host.
    #[arg(long, default_value_t = 10)]
    failure_backoff_secs: u64,

    /// Maximum host backoff after repeated failures.
    #[arg(long, default_value_t = 300)]
    max_backoff_secs: u64,
}

#[derive(Debug)]
struct FeedRow {
    feed_guid: String,
    feed_url: String,
    title: String,
    newest_item_at: Option<i64>,
    created_at: i64,
    updated_at: i64,
}

#[derive(Debug, Default)]
struct HostState {
    next_allowed_at: Option<Instant>,
    consecutive_failures: u32,
}

#[derive(Serialize)]
struct AuditRecord {
    source_db: DbFeedRecord,
    fetched_at: i64,
    host: Option<String>,
    throttle_wait_ms: u64,
    fetch: FetchRecord,
    raw_xml: Option<String>,
    parsed_feed: Option<stophammer_parser::types::IngestFeedData>,
    parse_error: Option<String>,
}

#[derive(Serialize)]
struct DbFeedRecord {
    feed_guid: String,
    feed_url: String,
    title: String,
    newest_item_at: Option<i64>,
    created_at: i64,
    updated_at: i64,
}

#[derive(Serialize)]
struct FetchRecord {
    final_url: Option<String>,
    http_status: Option<u16>,
    content_sha256: Option<String>,
    content_bytes: Option<usize>,
    response_headers: HashMap<String, String>,
    error: Option<String>,
}

fn query_feeds(conn: &Connection, limit: Option<usize>) -> rusqlite::Result<Vec<FeedRow>> {
    let sql = if limit.is_some() {
        "SELECT feed_guid, feed_url, title, newest_item_at, created_at, updated_at
         FROM feeds
         ORDER BY COALESCE(newest_item_at, updated_at, created_at) DESC
         LIMIT ?1"
    } else {
        "SELECT feed_guid, feed_url, title, newest_item_at, created_at, updated_at
         FROM feeds
         ORDER BY COALESCE(newest_item_at, updated_at, created_at) DESC"
    };

    let mut stmt = conn.prepare(sql)?;
    let rows = match limit {
        Some(limit) => stmt.query_map([limit as i64], map_feed_row)?,
        None => stmt.query_map([], map_feed_row)?,
    };

    rows.collect()
}

fn map_feed_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<FeedRow> {
    Ok(FeedRow {
        feed_guid: row.get(0)?,
        feed_url: row.get(1)?,
        title: row.get(2)?,
        newest_item_at: row.get(3)?,
        created_at: row.get(4)?,
        updated_at: row.get(5)?,
    })
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn format_error_chain(err: &dyn std::error::Error) -> String {
    let mut parts = vec![err.to_string()];
    let mut current = err.source();
    while let Some(source) = current {
        parts.push(source.to_string());
        current = source.source();
    }
    parts.join(": ")
}

fn headers_to_map(headers: &HeaderMap) -> HashMap<String, String> {
    headers
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.as_str().to_string(), value.to_string()))
        })
        .collect()
}

fn parse_retry_after_secs(headers: &HeaderMap) -> Option<u64> {
    headers
        .get(RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
}

fn host_key(url: &str) -> Option<String> {
    reqwest::Url::parse(url)
        .ok()
        .and_then(|parsed| parsed.host_str().map(|host| host.to_string()))
}

async fn wait_for_host(host_state: &HostState) -> u64 {
    let Some(next_allowed_at) = host_state.next_allowed_at else {
        return 0;
    };

    let now = Instant::now();
    if next_allowed_at <= now {
        return 0;
    }

    let delay = next_allowed_at.duration_since(now);
    tokio::time::sleep(delay).await;
    delay.as_millis().try_into().unwrap_or(u64::MAX)
}

fn mark_success(host_state: &mut HostState, success_delay: Duration) {
    host_state.consecutive_failures = 0;
    host_state.next_allowed_at = Some(Instant::now() + success_delay);
}

fn mark_failure(
    host_state: &mut HostState,
    initial_backoff: Duration,
    max_backoff: Duration,
    retry_after_secs: Option<u64>,
) {
    host_state.consecutive_failures = host_state.consecutive_failures.saturating_add(1);

    let retry_after = retry_after_secs.map(Duration::from_secs);
    let exp_backoff =
        initial_backoff.saturating_mul(2_u32.saturating_pow(host_state.consecutive_failures - 1));
    let backoff = retry_after
        .unwrap_or_else(|| exp_backoff.min(max_backoff))
        .min(max_backoff);

    host_state.next_allowed_at = Some(Instant::now() + backoff);
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    if let Some(parent) = std::path::Path::new(&cli.output).parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }

    let conn = Connection::open_with_flags(
        &cli.db,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    let feeds = query_feeds(&conn, cli.limit)?;

    let client = reqwest::Client::builder()
        .user_agent("stophammer-feed-audit/0.1")
        .build()?;

    let mut host_states: HashMap<String, HostState> = HashMap::new();
    let output = File::create(&cli.output)?;
    let mut writer = BufWriter::new(output);

    let success_delay = Duration::from_millis(cli.success_delay_ms);
    let initial_backoff = Duration::from_secs(cli.failure_backoff_secs);
    let max_backoff = Duration::from_secs(cli.max_backoff_secs);
    let timeout = Duration::from_secs(cli.timeout_secs);

    eprintln!(
        "feed_audit: db={} feeds={} output={} timeout={}s success_delay={}ms failure_backoff={}s",
        cli.db,
        feeds.len(),
        cli.output,
        cli.timeout_secs,
        cli.success_delay_ms,
        cli.failure_backoff_secs,
    );

    for (index, feed) in feeds.iter().enumerate() {
        let host = host_key(&feed.feed_url);
        let throttle_wait_ms = if let Some(host) = &host {
            let state = host_states.entry(host.clone()).or_default();
            wait_for_host(state).await
        } else {
            0
        };

        eprintln!(
            "feed_audit: [{}/{}] fetching {} ({})",
            index + 1,
            feeds.len(),
            feed.title,
            feed.feed_url
        );

        let mut response_headers = HashMap::new();
        let mut final_url = None;
        let mut http_status = None;
        let mut raw_xml = None;
        let mut parsed_feed = None;
        let mut parse_error = None;
        let mut content_sha256 = None;
        let mut content_bytes = None;
        let mut fetch_error = None;
        let mut retry_after_secs = None;

        match client.get(&feed.feed_url).timeout(timeout).send().await {
            Ok(response) => {
                retry_after_secs = parse_retry_after_secs(response.headers());
                response_headers = headers_to_map(response.headers());
                final_url = Some(response.url().to_string());
                http_status = Some(response.status().as_u16());

                match response.bytes().await {
                    Ok(body) => {
                        content_bytes = Some(body.len());
                        content_sha256 = Some(hex::encode(Sha256::digest(&body)));
                        let xml = String::from_utf8_lossy(&body).to_string();
                        let parser = profile::stophammer_with_fallback(feed.feed_guid.clone());

                        match parser.parse(&xml) {
                            Ok(parsed) => {
                                parsed_feed = Some(parsed);
                            }
                            Err(err) => {
                                parse_error = Some(err.to_string());
                            }
                        }

                        raw_xml = Some(xml);
                    }
                    Err(err) => {
                        fetch_error = Some(format_error_chain(&err));
                    }
                }
            }
            Err(err) => {
                fetch_error = Some(format_error_chain(&err));
            }
        }

        if let Some(host) = &host {
            let state = host_states.entry(host.clone()).or_default();
            if fetch_error.is_none() {
                mark_success(state, success_delay);
            } else {
                mark_failure(state, initial_backoff, max_backoff, retry_after_secs);
            }
        }

        let record = AuditRecord {
            source_db: DbFeedRecord {
                feed_guid: feed.feed_guid.clone(),
                feed_url: feed.feed_url.clone(),
                title: feed.title.clone(),
                newest_item_at: feed.newest_item_at,
                created_at: feed.created_at,
                updated_at: feed.updated_at,
            },
            fetched_at: unix_now(),
            host,
            throttle_wait_ms,
            fetch: FetchRecord {
                final_url,
                http_status,
                content_sha256,
                content_bytes,
                response_headers,
                error: fetch_error,
            },
            raw_xml,
            parsed_feed,
            parse_error,
        };

        serde_json::to_writer(&mut writer, &record)?;
        writer.write_all(b"\n")?;
        writer.flush()?;
    }

    Ok(())
}
