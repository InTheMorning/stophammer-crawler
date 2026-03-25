#![allow(
    clippy::too_many_lines,
    reason = "offline publisher expansion keeps discovery and append flow in one executable"
)]

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::Parser;
use reqwest::header::{HeaderMap, RETRY_AFTER};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use stophammer_parser::profile;

#[path = "../../src/url_queue.rs"]
mod url_queue;

use url_queue::{host_key, interleave_by_host};

#[derive(Parser, Debug)]
#[command(
    name = "audit_expand_publishers",
    about = "Append missing publisher feeds referenced by feed_audit.ndjson"
)]
struct Cli {
    /// Path to the existing feed audit NDJSON corpus.
    #[arg(long, default_value = "./analysis/data/feed_audit.ndjson")]
    input: String,

    /// NDJSON file to append newly fetched publisher feeds to.
    #[arg(long, default_value = "./analysis/data/feed_audit.ndjson")]
    output: String,

    /// Optional plain-text output of missing publisher feed URLs.
    #[arg(long, default_value = "./analysis/data/missing_publisher_feeds.txt")]
    missing_urls_output: String,

    /// Plain-text output file containing retryable / failed publisher feed URLs.
    #[arg(long, default_value = "./analysis/data/failed_publisher_feeds.txt")]
    failed_feeds_output: String,

    /// Maximum number of missing publisher feeds to fetch.
    #[arg(long)]
    limit: Option<usize>,

    /// Log missing publisher feed URLs without fetching them.
    #[arg(long)]
    dry_run: bool,

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

#[derive(Debug, Deserialize, Serialize)]
struct AuditRow {
    source_db: SourceDbRow,
    fetch: FetchRow,
    raw_xml: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct SourceDbRow {
    feed_guid: String,
    feed_url: String,
    title: String,
    #[serde(default)]
    newest_item_at: Option<i64>,
    #[serde(default)]
    created_at: i64,
    #[serde(default)]
    updated_at: i64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct FetchRow {
    #[serde(default)]
    final_url: Option<String>,
    #[serde(default)]
    http_status: Option<u16>,
    #[serde(default)]
    content_sha256: Option<String>,
    #[serde(default)]
    content_bytes: Option<usize>,
    #[serde(default)]
    response_headers: HashMap<String, String>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Debug)]
struct PublisherCandidate {
    feed_guid: String,
    feed_url: String,
}

#[derive(Debug, Default)]
struct DiscoveryStats {
    total_rows: usize,
    rows_with_raw_xml: usize,
    xml_reparse_failures: usize,
    malformed_rows: usize,
    publisher_refs_seen: usize,
    publisher_refs_without_url: usize,
    publisher_refs_already_present_by_guid: usize,
    publisher_refs_already_present_by_url: usize,
    duplicate_candidate_refs: usize,
    missing_candidates: usize,
}

#[derive(Debug, Default)]
struct HostState {
    next_allowed_at: Option<Instant>,
    consecutive_failures: u32,
}

#[derive(Serialize)]
struct AuditRecord {
    source_db: SourceDbRow,
    fetched_at: i64,
    host: Option<String>,
    throttle_wait_ms: u64,
    fetch: FetchRow,
    raw_xml: Option<String>,
    parsed_feed: Option<stophammer_parser::types::IngestFeedData>,
    parse_error: Option<String>,
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

fn normalize_url(url: &str) -> String {
    let trimmed = url.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    match reqwest::Url::parse(trimmed) {
        Ok(mut parsed) => {
            parsed.set_fragment(None);
            parsed.to_string()
        }
        Err(_) => trimmed.to_string(),
    }
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

fn should_keep_in_audit(fetch_error: Option<&String>, http_status: Option<u16>) -> bool {
    fetch_error.is_none() && http_status == Some(200)
}

fn format_http_fetch_error(status: u16) -> String {
    let reason = reqwest::StatusCode::from_u16(status)
        .ok()
        .and_then(|code| code.canonical_reason().map(str::to_string))
        .unwrap_or_else(|| "Unknown Status".to_string());
    format!("http {status} {reason}")
}

fn write_url_list(path: &str, urls: &BTreeSet<String>) -> std::io::Result<()> {
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

fn ensure_output_ends_with_newline(path: &str) -> std::io::Result<()> {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err),
    };
    if metadata.len() == 0 {
        return Ok(());
    }

    let content = fs::read(path)?;
    if content.last() == Some(&b'\n') {
        return Ok(());
    }

    let mut output = OpenOptions::new().append(true).open(path)?;
    output.write_all(b"\n")
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

fn discover_missing_publisher_candidates(
    path: &str,
) -> Result<(Vec<PublisherCandidate>, DiscoveryStats), Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let parser = profile::stophammer();
    let mut seen_urls = HashSet::new();
    let mut seen_guids = HashSet::new();

    let mut rows = Vec::new();
    let mut stats = DiscoveryStats::default();
    for (index, line) in reader.lines().enumerate() {
        let line = line.map_err(|err| format!("failed reading NDJSON row {}: {err}", index + 1))?;
        let row: AuditRow = if let Ok(row) = serde_json::from_str(&line) {
            row
        } else {
            stats.malformed_rows += 1;
            continue;
        };

        let normalized_source_url = normalize_url(&row.source_db.feed_url);
        if !normalized_source_url.is_empty() {
            seen_urls.insert(normalized_source_url);
        }
        if !row.source_db.feed_guid.trim().is_empty() {
            seen_guids.insert(row.source_db.feed_guid.clone());
        }
        if let Some(final_url) = row.fetch.final_url.as_deref() {
            let normalized_final_url = normalize_url(final_url);
            if !normalized_final_url.is_empty() {
                seen_urls.insert(normalized_final_url);
            }
        }

        rows.push(row);
    }

    stats.total_rows = rows.len() + stats.malformed_rows;
    let mut emitted_urls = HashSet::new();
    let mut candidates = Vec::new();

    for row in rows {
        let Some(raw_xml) = row.raw_xml.as_deref() else {
            continue;
        };
        stats.rows_with_raw_xml += 1;

        let Ok(reparsed) = parser.parse(raw_xml) else {
            stats.xml_reparse_failures += 1;
            continue;
        };

        for remote in reparsed.remote_items {
            if remote.medium.as_deref() != Some("publisher") {
                continue;
            }
            stats.publisher_refs_seen += 1;

            if seen_guids.contains(&remote.remote_feed_guid) {
                stats.publisher_refs_already_present_by_guid += 1;
                continue;
            }

            let Some(remote_feed_url) = remote.remote_feed_url.as_deref() else {
                stats.publisher_refs_without_url += 1;
                continue;
            };
            let normalized_remote_url = normalize_url(remote_feed_url);
            if normalized_remote_url.is_empty() {
                stats.publisher_refs_without_url += 1;
                continue;
            }

            if seen_urls.contains(&normalized_remote_url) {
                stats.publisher_refs_already_present_by_url += 1;
                continue;
            }

            if !emitted_urls.insert(normalized_remote_url.clone()) {
                stats.duplicate_candidate_refs += 1;
                continue;
            }

            candidates.push(PublisherCandidate {
                feed_guid: remote.remote_feed_guid,
                feed_url: normalized_remote_url,
            });
        }
    }

    stats.missing_candidates = candidates.len();
    Ok((candidates, stats))
}

fn candidate_urls(candidates: &[PublisherCandidate]) -> BTreeSet<String> {
    candidates
        .iter()
        .map(|candidate| candidate.feed_url.clone())
        .collect()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let (mut candidates, stats) = discover_missing_publisher_candidates(&cli.input)?;

    if let Some(limit) = cli.limit {
        candidates.truncate(limit);
    }

    let missing_urls = candidate_urls(&candidates);
    write_url_list(&cli.missing_urls_output, &missing_urls)?;

    eprintln!(
        "audit_expand_publishers: rows={} raw_xml={} reparse_failures={} malformed_rows={} publisher_refs={} missing={} already_by_guid={} already_by_url={} unresolved_without_url={} duplicate_refs={}",
        stats.total_rows,
        stats.rows_with_raw_xml,
        stats.xml_reparse_failures,
        stats.malformed_rows,
        stats.publisher_refs_seen,
        candidates.len(),
        stats.publisher_refs_already_present_by_guid,
        stats.publisher_refs_already_present_by_url,
        stats.publisher_refs_without_url,
        stats.duplicate_candidate_refs,
    );
    eprintln!(
        "audit_expand_publishers: wrote missing publisher URLs to {}",
        cli.missing_urls_output
    );

    if cli.dry_run || candidates.is_empty() {
        if cli.dry_run {
            for candidate in &candidates {
                eprintln!(
                    "  [dry-run] publisher guid={} {}",
                    candidate.feed_guid, candidate.feed_url
                );
            }
        }
        return Ok(());
    }

    if let Some(parent) = std::path::Path::new(&cli.output).parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }

    ensure_output_ends_with_newline(&cli.output)?;

    let output = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&cli.output)?;
    let mut writer = BufWriter::new(output);

    let client = reqwest::Client::builder()
        .user_agent("stophammer-feed-audit/0.1")
        .build()?;

    let feeds = interleave_by_host(candidates, |candidate| host_key(&candidate.feed_url));
    let mut host_states: HashMap<String, HostState> = HashMap::new();
    let mut failed_feeds = BTreeSet::new();
    let mut kept_rows = 0usize;

    let success_delay = Duration::from_millis(cli.success_delay_ms);
    let initial_backoff = Duration::from_secs(cli.failure_backoff_secs);
    let max_backoff = Duration::from_secs(cli.max_backoff_secs);
    let timeout = Duration::from_secs(cli.timeout_secs);

    eprintln!(
        "audit_expand_publishers: fetching missing publisher feeds={} output={} timeout={}s success_delay={}ms failure_backoff={}s",
        feeds.len(),
        cli.output,
        cli.timeout_secs,
        cli.success_delay_ms,
        cli.failure_backoff_secs,
    );

    for (index, feed) in feeds.iter().enumerate() {
        let host = host_key(&feed.feed_url);
        let throttle_wait_ms = if let Some(host) = &host {
            let host_state = host_states.entry(host.clone()).or_default();
            wait_for_host(host_state).await
        } else {
            0
        };

        eprintln!(
            "audit_expand_publishers: [{}/{}] fetching {}",
            index + 1,
            feeds.len(),
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
                        if http_status == Some(200) {
                            let parser = profile::stophammer_with_fallback(feed.feed_guid.clone());
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
            let host_state = host_states.entry(host.clone()).or_default();
            if should_keep_in_audit(fetch_error.as_ref(), http_status) {
                mark_success(host_state, success_delay);
            } else {
                mark_failure(host_state, initial_backoff, max_backoff, retry_after_secs);
            }
        }

        if let Some(error) = &fetch_error {
            eprintln!("  fetch_error: {error}");
        }
        if let Some(error) = &parse_error {
            eprintln!("  parse_error: {error}");
        }

        let record = AuditRecord {
            source_db: SourceDbRow {
                feed_guid: feed.feed_guid.clone(),
                feed_url: feed.feed_url.clone(),
                title: feed.feed_url.clone(),
                newest_item_at: None,
                created_at: unix_now(),
                updated_at: unix_now(),
            },
            fetched_at: unix_now(),
            host,
            throttle_wait_ms,
            fetch: FetchRow {
                final_url,
                http_status,
                content_sha256,
                content_bytes,
                response_headers,
                error: fetch_error.clone(),
            },
            raw_xml,
            parsed_feed,
            parse_error,
        };

        if should_keep_in_audit(fetch_error.as_ref(), http_status) {
            serde_json::to_writer(&mut writer, &record)?;
            writer.write_all(b"\n")?;
            kept_rows += 1;
        } else {
            failed_feeds.insert(feed.feed_url.clone());
        }
    }

    writer.flush()?;
    write_url_list(&cli.failed_feeds_output, &failed_feeds)?;

    eprintln!(
        "audit_expand_publishers: appended {} publisher feeds; failed_urls={} (written to {})",
        kept_rows,
        failed_feeds.len(),
        cli.failed_feeds_output,
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{AuditRow, FetchRow, SourceDbRow, discover_missing_publisher_candidates};
    use std::collections::HashMap;
    use std::fs;
    use tempfile::NamedTempFile;

    fn write_ndjson(rows: &[AuditRow]) -> NamedTempFile {
        let file = NamedTempFile::new().expect("temp NDJSON");
        let mut out = String::new();
        for row in rows {
            out.push_str(&serde_json::to_string(row).expect("serialize row"));
            out.push('\n');
        }
        fs::write(file.path(), out).expect("write NDJSON");
        file
    }

    fn base_row(feed_guid: &str, feed_url: &str, raw_xml: &str) -> AuditRow {
        AuditRow {
            source_db: SourceDbRow {
                feed_guid: feed_guid.to_string(),
                feed_url: feed_url.to_string(),
                title: feed_url.to_string(),
                newest_item_at: None,
                created_at: 1,
                updated_at: 1,
            },
            fetch: FetchRow {
                final_url: Some(feed_url.to_string()),
                http_status: Some(200),
                content_sha256: None,
                content_bytes: None,
                response_headers: HashMap::new(),
                error: None,
            },
            raw_xml: Some(raw_xml.to_string()),
        }
    }

    #[test]
    fn discovers_missing_publisher_urls_from_cached_xml() {
        let row = base_row(
            "music-feed-guid",
            "https://example.com/music.xml",
            r#"<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"
     xmlns:podcast="https://podcastindex.org/namespace/1.0">
  <channel>
    <title>Music Feed</title>
    <podcast:guid>music-feed-guid</podcast:guid>
    <podcast:medium>music</podcast:medium>
    <podcast:remoteItem
      medium="publisher"
      feedGuid="publisher-feed-guid"
      feedUrl="https://example.com/publisher.xml" />
    <item>
      <title>Track</title>
      <guid>track-guid</guid>
      <enclosure url="https://example.com/track.mp3" type="audio/mpeg" length="123" />
    </item>
  </channel>
</rss>"#,
        );
        let file = write_ndjson(&[row]);

        let (candidates, stats) =
            discover_missing_publisher_candidates(file.path().to_str().expect("path"))
                .expect("discover");

        assert_eq!(stats.publisher_refs_seen, 1, "expected one publisher ref");
        assert_eq!(candidates.len(), 1, "expected one missing candidate");
        assert_eq!(candidates[0].feed_guid, "publisher-feed-guid");
        assert_eq!(candidates[0].feed_url, "https://example.com/publisher.xml");
    }

    #[test]
    fn skips_targets_already_present_by_guid_or_url() {
        let existing_publisher = base_row(
            "publisher-feed-guid",
            "https://example.com/publisher.xml",
            r#"<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"
     xmlns:podcast="https://podcastindex.org/namespace/1.0">
  <channel>
    <title>Publisher Feed</title>
    <podcast:guid>publisher-feed-guid</podcast:guid>
    <podcast:medium>publisher</podcast:medium>
    <podcast:remoteItem medium="music" feedGuid="music-feed-guid" feedUrl="https://example.com/music.xml" />
  </channel>
</rss>"#,
        );
        let music_row = base_row(
            "music-feed-guid",
            "https://example.com/music.xml",
            r#"<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"
     xmlns:podcast="https://podcastindex.org/namespace/1.0">
  <channel>
    <title>Music Feed</title>
    <podcast:guid>music-feed-guid</podcast:guid>
    <podcast:medium>music</podcast:medium>
    <podcast:remoteItem
      medium="publisher"
      feedGuid="publisher-feed-guid"
      feedUrl="https://example.com/publisher.xml" />
    <item>
      <title>Track</title>
      <guid>track-guid</guid>
      <enclosure url="https://example.com/track.mp3" type="audio/mpeg" length="123" />
    </item>
  </channel>
</rss>"#,
        );
        let file = write_ndjson(&[existing_publisher, music_row]);

        let (candidates, stats) =
            discover_missing_publisher_candidates(file.path().to_str().expect("path"))
                .expect("discover");

        assert!(candidates.is_empty(), "expected no missing candidates");
        assert_eq!(
            stats.publisher_refs_already_present_by_guid, 1,
            "expected GUID dedupe"
        );
    }

    #[test]
    fn counts_publisher_refs_without_urls() {
        let row = base_row(
            "music-feed-guid",
            "https://example.com/music.xml",
            r#"<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"
     xmlns:podcast="https://podcastindex.org/namespace/1.0">
  <channel>
    <title>Music Feed</title>
    <podcast:guid>music-feed-guid</podcast:guid>
    <podcast:medium>music</podcast:medium>
    <podcast:remoteItem medium="publisher" feedGuid="publisher-feed-guid" />
    <item>
      <title>Track</title>
      <guid>track-guid</guid>
      <enclosure url="https://example.com/track.mp3" type="audio/mpeg" length="123" />
    </item>
  </channel>
</rss>"#,
        );
        let file = write_ndjson(&[row]);

        let (candidates, stats) =
            discover_missing_publisher_candidates(file.path().to_str().expect("path"))
                .expect("discover");

        assert!(candidates.is_empty(), "expected no fetchable candidates");
        assert_eq!(
            stats.publisher_refs_without_url, 1,
            "expected unresolved GUID-only publisher ref"
        );
    }

    #[test]
    fn skips_malformed_rows_during_discovery() {
        let file = NamedTempFile::new().expect("temp NDJSON");
        fs::write(
            file.path(),
            concat!(
                "{\"source_db\":{\"feed_guid\":\"ok\",\"feed_url\":\"https://example.com/music.xml\",\"title\":\"ok\",\"newest_item_at\":null,\"created_at\":1,\"updated_at\":1},",
                "\"fetch\":{\"final_url\":\"https://example.com/music.xml\",\"http_status\":200,\"content_sha256\":null,\"content_bytes\":null,\"response_headers\":{},\"error\":null},",
                "\"raw_xml\":\"<?xml version=\\\"1.0\\\" encoding=\\\"UTF-8\\\"?><rss version=\\\"2.0\\\" xmlns:podcast=\\\"https://podcastindex.org/namespace/1.0\\\"><channel><title>Music Feed</title><podcast:guid>ok</podcast:guid><podcast:medium>music</podcast:medium><podcast:remoteItem medium=\\\"publisher\\\" feedGuid=\\\"publisher-feed-guid\\\" feedUrl=\\\"https://example.com/publisher.xml\\\" /><item><title>Track</title><guid>track-guid</guid><enclosure url=\\\"https://example.com/track.mp3\\\" type=\\\"audio/mpeg\\\" length=\\\"123\\\" /></item></channel></rss>\"}\n",
                "{\"broken\":"
            ),
        )
        .expect("write NDJSON");

        let (candidates, stats) =
            discover_missing_publisher_candidates(file.path().to_str().expect("path"))
                .expect("discover");

        assert_eq!(stats.malformed_rows, 1, "expected one malformed row");
        assert_eq!(candidates.len(), 1, "expected valid row to survive");
    }
}
