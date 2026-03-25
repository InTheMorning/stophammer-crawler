#![allow(
    clippy::too_many_lines,
    reason = "audit binary keeps its full offline fetch/report flow in one file"
)]

use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader};
use std::io::{BufWriter, Write};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::Parser;
use reqwest::header::{HeaderMap, RETRY_AFTER};
use rusqlite::{Connection, OpenFlags};
use serde::Serialize;
use sha2::{Digest, Sha256};
use stophammer_parser::{extract_podcast_namespace, profile};

#[path = "../../src/url_queue.rs"]
mod url_queue;

use url_queue::{host_key, interleave_by_host};

#[derive(Parser, Debug)]
#[command(
    name = "feed_audit",
    about = "Fetch and dump full RSS feeds from a stophammer DB"
)]
struct Cli {
    /// Path to a populated stophammer `SQLite` DB.
    #[arg(long, default_value = "./analysis/data/stophammer-feeds.db")]
    db: String,

    /// Optional plain-text URL file to fetch instead of loading feeds from `--db`.
    #[arg(long)]
    urls_file: Option<String>,

    /// Output NDJSON file containing one record per fetched feed.
    #[arg(long, default_value = "./analysis/data/feed_audit.ndjson")]
    output: String,

    /// Append successful rows to `--output` instead of truncating it first.
    #[arg(long)]
    append: bool,

    /// Plain-text output file containing retryable / failed feed URLs.
    #[arg(long, default_value = "./analysis/data/failed_feeds.txt")]
    failed_feeds_output: String,

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
    podcast_namespace: Option<stophammer_parser::types::IngestPodcastNamespaceSnapshot>,
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
        Some(limit) => {
            let limit_i64 = i64::try_from(limit).unwrap_or(i64::MAX);
            stmt.query_map([limit_i64], map_feed_row)?
        }
        None => stmt.query_map([], map_feed_row)?,
    };

    rows.collect()
}

fn load_feeds_from_urls(path: &str, limit: Option<usize>) -> std::io::Result<Vec<FeedRow>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut feeds = Vec::new();
    let now = unix_now();

    for line in reader.lines() {
        let line = line?;
        let feed_url = line.trim();
        if feed_url.is_empty() || feed_url.starts_with('#') {
            continue;
        }

        feeds.push(FeedRow {
            feed_guid: feed_url.to_string(),
            feed_url: feed_url.to_string(),
            title: feed_url.to_string(),
            newest_item_at: None,
            created_at: now,
            updated_at: now,
        });

        if let Some(limit) = limit
            && feeds.len() >= limit
        {
            break;
        }
    }

    Ok(feeds)
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
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    )
    .unwrap_or(i64::MAX)
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

fn is_retryable_status(status: u16) -> bool {
    status == 408 || status == 425 || status == 429 || (500..=599).contains(&status)
}

fn should_keep_in_audit(fetch_error: Option<&String>, http_status: Option<u16>) -> bool {
    fetch_error.is_none() && http_status == Some(200)
}

fn write_failed_feeds(path: &str, urls: &BTreeSet<String>) -> std::io::Result<()> {
    if let Some(parent) = std::path::Path::new(path).parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }

    let output = File::create(path)?;
    let mut writer = BufWriter::new(output);
    for url in urls {
        writer.write_all(url.as_bytes())?;
        writer.write_all(b"\n")?;
    }
    writer.flush()
}

fn format_http_fetch_error(status: u16) -> String {
    let reason = reqwest::StatusCode::from_u16(status)
        .ok()
        .and_then(|code| code.canonical_reason().map(str::to_string))
        .unwrap_or_else(|| "Unknown Status".to_string());
    format!("http {status} {reason}")
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

    if let Some(parent) = std::path::Path::new(&cli.output).parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }

    let feeds = if let Some(urls_file) = &cli.urls_file {
        interleave_by_host(load_feeds_from_urls(urls_file, cli.limit)?, |feed| {
            host_key(&feed.feed_url)
        })
    } else {
        let conn = Connection::open_with_flags(
            &cli.db,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        interleave_by_host(query_feeds(&conn, cli.limit)?, |feed| {
            host_key(&feed.feed_url)
        })
    };

    let client = reqwest::Client::builder()
        .user_agent("stophammer-feed-audit/0.1")
        .build()?;

    let mut host_states: HashMap<String, HostState> = HashMap::new();
    let output = OpenOptions::new()
        .create(true)
        .write(true)
        .append(cli.append)
        .truncate(!cli.append)
        .open(&cli.output)?;
    let mut writer = BufWriter::new(output);
    let mut failed_feeds = BTreeSet::new();
    let mut kept_rows = 0usize;

    let success_delay = Duration::from_millis(cli.success_delay_ms);
    let initial_backoff = Duration::from_secs(cli.failure_backoff_secs);
    let max_backoff = Duration::from_secs(cli.max_backoff_secs);
    let timeout = Duration::from_secs(cli.timeout_secs);

    eprintln!(
        "feed_audit: source={} feeds={} output={} append={} timeout={}s success_delay={}ms failure_backoff={}s",
        cli.urls_file.as_deref().unwrap_or(&cli.db),
        feeds.len(),
        cli.output,
        cli.append,
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
        let mut podcast_namespace = None;
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
                        if http_status == Some(200) {
                            let parser = profile::stophammer_with_fallback(feed.feed_guid.clone());
                            podcast_namespace = extract_podcast_namespace(&xml).ok().flatten();

                            match parser.parse(&xml) {
                                Ok(parsed) => {
                                    parsed_feed = Some(parsed);
                                }
                                Err(err) => {
                                    parse_error = Some(err.to_string());
                                }
                            }
                        } else if let Some(status) = http_status {
                            fetch_error = Some(format_http_fetch_error(status));
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
            if should_keep_in_audit(fetch_error.as_ref(), http_status) {
                mark_success(state, success_delay);
            } else {
                mark_failure(state, initial_backoff, max_backoff, retry_after_secs);
            }
        }

        let should_retry = fetch_error.is_some() || http_status.is_some_and(is_retryable_status);
        if should_retry {
            failed_feeds.insert(feed.feed_url.clone());
        }

        let source_db = parsed_feed.as_ref().map_or_else(
            || DbFeedRecord {
                feed_guid: feed.feed_guid.clone(),
                feed_url: feed.feed_url.clone(),
                title: feed.title.clone(),
                newest_item_at: feed.newest_item_at,
                created_at: feed.created_at,
                updated_at: feed.updated_at,
            },
            |parsed| DbFeedRecord {
                feed_guid: parsed.feed_guid.clone(),
                feed_url: feed.feed_url.clone(),
                title: parsed.title.clone(),
                newest_item_at: feed.newest_item_at,
                created_at: feed.created_at,
                updated_at: feed.updated_at,
            },
        );

        let record = AuditRecord {
            source_db,
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
            podcast_namespace,
            parse_error,
        };

        if should_keep_in_audit(record.fetch.error.as_ref(), record.fetch.http_status) {
            serde_json::to_writer(&mut writer, &record)?;
            writer.write_all(b"\n")?;
            writer.flush()?;
            kept_rows += 1;
        }
    }

    write_failed_feeds(&cli.failed_feeds_output, &failed_feeds)?;
    eprintln!(
        "feed_audit: kept={} failed_urls={} output={} append={} failed_output={}",
        kept_rows,
        failed_feeds.len(),
        cli.output,
        cli.append,
        cli.failed_feeds_output,
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{is_retryable_status, should_keep_in_audit};
    use stophammer_parser::extract_podcast_namespace;

    #[test]
    fn keeps_only_successful_fetches_in_clean_audit_output() {
        assert!(should_keep_in_audit(None, Some(200)));
        assert!(!should_keep_in_audit(
            Some(&"timeout".to_string()),
            Some(200)
        ));
        assert!(!should_keep_in_audit(None, Some(429)));
    }

    #[test]
    fn retries_429_and_5xx_statuses() {
        assert!(is_retryable_status(429));
        assert!(is_retryable_status(503));
        assert!(!is_retryable_status(404));
    }

    #[test]
    fn extracts_podcast_namespace_tags_into_snapshot() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"
     xmlns:podcast="https://podcastindex.org/namespace/1.0">
  <channel>
    <title>Feed</title>
    <podcast:guid>feed-guid</podcast:guid>
    <podcast:person role="artist" href="https://example.com/artist">Artist Name</podcast:person>
    <podcast:liveItem status="live">
      <guid>live-guid</guid>
      <title>Live</title>
      <podcast:alternateEnclosure type="audio/flac" length="123" />
    </podcast:liveItem>
    <item>
      <guid>track-guid</guid>
      <title>Track</title>
      <podcast:txt purpose="npub">npub1example</podcast:txt>
      <podcast:remoteItem medium="publisher" feedGuid="publisher-guid" feedUrl="https://example.com/publisher.xml" />
    </item>
  </channel>
</rss>"#;

        let snapshot = extract_podcast_namespace(xml)
            .expect("parser namespace extraction")
            .expect("snapshot");
        assert_eq!(snapshot.tags.len(), 6);

        assert_eq!(snapshot.tags[0].path, "rss.channel.podcast:guid");
        assert_eq!(snapshot.tags[0].entity_scope, "channel");
        assert_eq!(snapshot.tags[0].text.as_deref(), Some("feed-guid"));

        assert_eq!(snapshot.tags[1].tag, "podcast:person");
        assert_eq!(
            snapshot.tags[1].attributes.get("role").map(String::as_str),
            Some("artist")
        );

        assert_eq!(snapshot.tags[2].entity_scope, "channel");
        assert_eq!(snapshot.tags[2].tag, "podcast:liveItem");

        assert_eq!(snapshot.tags[3].entity_scope, "live_item");
        assert_eq!(snapshot.tags[3].entity_guid.as_deref(), Some("live-guid"));
        assert_eq!(snapshot.tags[3].tag, "podcast:alternateEnclosure");

        assert_eq!(snapshot.tags[4].entity_scope, "item");
        assert_eq!(snapshot.tags[4].entity_guid.as_deref(), Some("track-guid"));
        assert_eq!(snapshot.tags[4].tag, "podcast:txt");

        assert_eq!(snapshot.tags[5].entity_scope, "item");
        assert_eq!(snapshot.tags[5].entity_guid.as_deref(), Some("track-guid"));
        assert_eq!(snapshot.tags[5].tag, "podcast:remoteItem");
        assert_eq!(
            snapshot.tags[5]
                .attributes
                .get("feedGuid")
                .map(String::as_str),
            Some("publisher-guid")
        );
    }
}
