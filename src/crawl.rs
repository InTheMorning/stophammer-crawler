use std::sync::{Arc, Mutex};
use std::time::Duration;

use reqwest::header::{
    ETAG, HeaderMap, IF_MODIFIED_SINCE, IF_NONE_MATCH, LAST_MODIFIED, RETRY_AFTER,
};
use sha2::{Digest, Sha256};
use stophammer_parser::profile;
use stophammer_parser::types::IngestFeedData;

use crate::feed_cache::{CachedFeed, FeedCacheDb, FetchedFeed, unix_now};

const HTTP_ERROR_PREVIEW_LIMIT: usize = 160;

/// Shared fetch cache handle (ADR 0050 §1, `stophammer` repository).
///
/// A mode opens one `FeedCacheDb` and shares this handle with every task
/// that fetches a feed. Each use of the store takes the lock for one call
/// only, and releases it before any `.await`.
pub type FeedCache = Arc<Mutex<FeedCacheDb>>;

/// Configuration shared by all crawl modes.
pub struct CrawlConfig {
    pub crawl_token: String,
    pub ingest_url: String,
    pub user_agent: String,
    pub fetch_timeout: Duration,
    pub ingest_timeout: Duration,
    /// When true, the ingest server skips the content-hash dedup check.
    pub force_reingest: bool,
    /// When true, the crawler sends a conditional GET for a URL the cache
    /// already holds (ADR 0050 §2 and §5, `stophammer` repository). A pass
    /// with `--no-revalidate` sets this to `false`.
    pub revalidate: bool,
    /// Dedicated HTTP client for ingest POSTs, built with connection pooling
    /// disabled so stale keep-alive sockets never cause spurious failures.
    ingest_client: reqwest::Client,
}

impl CrawlConfig {
    pub fn from_env_with_force(force_reingest: bool) -> Self {
        let ingest_timeout = Duration::from_secs(10);
        Self {
            crawl_token: std::env::var("CRAWL_TOKEN").expect("CRAWL_TOKEN is required"),
            ingest_url: std::env::var("INGEST_URL")
                .unwrap_or_else(|_| "http://localhost:8008/ingest/feed".to_string()),
            user_agent: "stophammer-crawler/0.1".to_string(),
            fetch_timeout: Duration::from_secs(20),
            ingest_timeout,
            force_reingest,
            revalidate: true,
            ingest_client: Self::build_ingest_client(ingest_timeout),
        }
    }

    /// Placeholder config for dry-run modes that skip the ingest POST.
    pub fn dry_run(user_agent: &str, fetch_timeout: Duration) -> Self {
        let ingest_timeout = Duration::from_secs(10);
        Self {
            crawl_token: String::new(),
            ingest_url: String::new(),
            user_agent: user_agent.to_string(),
            fetch_timeout,
            ingest_timeout,
            force_reingest: false,
            revalidate: true,
            ingest_client: Self::build_ingest_client(ingest_timeout),
        }
    }

    /// Change whether the crawler sends a conditional GET (ADR 0050 §5,
    /// `stophammer` repository).
    #[must_use]
    pub fn with_revalidate(mut self, revalidate: bool) -> Self {
        self.revalidate = revalidate;
        self
    }

    fn build_ingest_client(timeout: Duration) -> reqwest::Client {
        reqwest::Client::builder()
            .pool_max_idle_per_host(0)
            .connect_timeout(timeout)
            .build()
            .expect("failed to build ingest HTTP client")
    }
}

/// Outcome of a single feed crawl attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CrawlOutcome {
    Accepted {
        warnings: Vec<String>,
    },
    Rejected {
        reason: String,
        warnings: Vec<String>,
    },
    NoChange,
    FetchError {
        reason: String,
        retryable: bool,
        retry_after_secs: Option<u64>,
    },
    ParseError(String),
    IngestError {
        reason: String,
        retryable: bool,
        retry_after_secs: Option<u64>,
    },
}

impl std::fmt::Display for CrawlOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Accepted { warnings } => {
                write!(f, "accepted")?;
                for w in warnings {
                    write!(f, " [{w}]")?;
                }
                Ok(())
            }
            Self::Rejected { reason, warnings } => {
                write!(f, "rejected: {reason}")?;
                for w in warnings {
                    write!(f, " [{w}]")?;
                }
                Ok(())
            }
            Self::NoChange => write!(f, "no_change"),
            Self::FetchError { reason, .. } => write!(f, "fetch_error: {reason}"),
            Self::ParseError(e) => write!(f, "parse_error: {e}"),
            Self::IngestError { reason, .. } => write!(f, "ingest_error: {reason}"),
        }
    }
}

impl CrawlOutcome {
    /// Returns `true` when the rejection was caused by `[medium_music]`,
    /// i.e. the feed's `podcast:medium` is not `"music"`.
    #[must_use]
    pub fn is_medium_rejection(&self) -> bool {
        matches!(self, Self::Rejected { reason, .. } if reason.starts_with("[medium_music]"))
    }

    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::FetchError {
                retryable: true,
                ..
            } | Self::IngestError {
                retryable: true,
                ..
            }
        )
    }

    #[must_use]
    pub fn retry_delay(&self, attempt: u32) -> Option<Duration> {
        if !self.is_retryable() {
            return None;
        }

        let retry_after_secs = match self {
            Self::FetchError {
                retry_after_secs, ..
            }
            | Self::IngestError {
                retry_after_secs, ..
            } => *retry_after_secs,
            _ => None,
        };

        Some(retry_after_secs.map_or_else(
            || Duration::from_secs(1_u64 << (attempt.saturating_sub(1))),
            Duration::from_secs,
        ))
    }

    #[must_use]
    pub fn label(&self) -> &'static str {
        match self {
            Self::Accepted { .. } => "accepted",
            Self::Rejected { .. } => "rejected",
            Self::NoChange => "no_change",
            Self::FetchError { .. } => "fetch_error",
            Self::ParseError(_) => "parse_error",
            Self::IngestError { .. } => "ingest_error",
        }
    }

    #[must_use]
    pub fn reason(&self) -> Option<&str> {
        match self {
            Self::Rejected { reason, .. }
            | Self::FetchError { reason, .. }
            | Self::ParseError(reason)
            | Self::IngestError { reason, .. } => Some(reason.as_str()),
            Self::Accepted { .. } | Self::NoChange => None,
        }
    }
}

/// Importer-facing details preserved from the shared crawl pipeline.
#[derive(Debug, Clone, PartialEq)]
pub struct CrawlReport {
    pub outcome: CrawlOutcome,
    pub fetch_http_status: Option<u16>,
    pub raw_medium: Option<String>,
    pub parsed_feed_guid: Option<String>,
    pub final_url: Option<String>,
    pub content_sha256: Option<String>,
    pub raw_xml: Option<String>,
    pub parsed_feed: Option<IngestFeedData>,
}

impl CrawlReport {
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        self.outcome.is_retryable()
    }
}

#[derive(serde::Deserialize)]
struct IngestResponse {
    #[serde(default)]
    accepted: bool,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    warnings: Option<Vec<String>>,
}

fn parse_feed_xml(
    xml: &str,
    fallback_guid: Option<&str>,
) -> Result<Option<IngestFeedData>, String> {
    let parser = match fallback_guid {
        Some(guid) => profile::stophammer_with_fallback(guid.to_string()),
        None => profile::stophammer(),
    };

    match parser.parse(xml) {
        Ok(data) => Ok(Some(data)),
        Err(e) => {
            if e.is_xml() {
                Err(e.to_string())
            } else {
                // Missing fields (no title, no guid) → still POST with `feed_data: null`
                // so the server can record the crawl attempt.
                Ok(None)
            }
        }
    }
}

fn is_retryable_http_status(status: u16) -> bool {
    status == 408 || status == 425 || status == 429 || (500..=599).contains(&status)
}

fn is_retryable_ingest_status(status: reqwest::StatusCode) -> bool {
    matches!(
        status,
        reqwest::StatusCode::REQUEST_TIMEOUT
            | reqwest::StatusCode::TOO_EARLY
            | reqwest::StatusCode::TOO_MANY_REQUESTS
            | reqwest::StatusCode::BAD_GATEWAY
            | reqwest::StatusCode::SERVICE_UNAVAILABLE
            | reqwest::StatusCode::GATEWAY_TIMEOUT
    )
}

fn body_preview(body: &[u8]) -> String {
    let preview = String::from_utf8_lossy(body)
        .chars()
        .map(|ch| if ch.is_whitespace() { ' ' } else { ch })
        .collect::<String>();
    let preview = preview.split_whitespace().collect::<Vec<_>>().join(" ");
    if preview.chars().count() <= HTTP_ERROR_PREVIEW_LIMIT {
        return preview;
    }

    preview
        .chars()
        .take(HTTP_ERROR_PREVIEW_LIMIT)
        .collect::<String>()
        + "..."
}

fn describe_http_status(status: u16) -> String {
    reqwest::StatusCode::from_u16(status)
        .ok()
        .and_then(|code| code.canonical_reason().map(str::to_string))
        .unwrap_or_else(|| "Unknown Status".to_string())
}

fn format_http_fetch_error(status: u16, headers: &HeaderMap, body: &[u8]) -> String {
    let mut parts = vec![format!("http {status} {}", describe_http_status(status))];
    if let Some(retry_after) = headers
        .get(RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
    {
        parts.push(format!("retry-after={retry_after}"));
    }
    let preview = body_preview(body);
    if !preview.is_empty() {
        parts.push(format!("body=\"{preview}\""));
    }
    parts.join(" ")
}

fn parse_retry_after_secs(headers: &HeaderMap) -> Option<u64> {
    headers
        .get(RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
}

fn normalize_rejection_reason(
    reason: Option<String>,
    ingest_status: reqwest::StatusCode,
    resp_body: &str,
) -> String {
    reason
        .map(|reason| reason.trim().to_string())
        .filter(|reason| !reason.is_empty())
        .unwrap_or_else(|| {
            format!("empty ingest rejection reason (http {ingest_status}, response={resp_body})")
        })
}

fn format_ingest_http_error(status: reqwest::StatusCode, resp_body: &str) -> String {
    format!(
        "ingest http {status} {} response={resp_body}",
        status.canonical_reason().unwrap_or("Unknown Status")
    )
}

fn build_crawl_report(
    outcome: CrawlOutcome,
    fetch_http_status: Option<u16>,
    feed_data: Option<&IngestFeedData>,
    final_url: Option<String>,
    content_sha256: Option<String>,
    raw_xml: Option<String>,
) -> CrawlReport {
    CrawlReport {
        outcome,
        fetch_http_status,
        raw_medium: feed_data.and_then(|data| data.raw_medium.clone()),
        parsed_feed_guid: feed_data.map(|data| data.feed_guid.clone()),
        final_url,
        content_sha256,
        raw_xml,
        parsed_feed: feed_data.cloned(),
    }
}

async fn post_ingest_payload(
    canonical_url: &str,
    source_url: &str,
    http_status: u16,
    content_hash: &str,
    feed_data: Option<IngestFeedData>,
    config: &CrawlConfig,
) -> CrawlOutcome {
    let mut payload = serde_json::json!({
        "canonical_url": canonical_url,
        "source_url": source_url,
        "crawl_token": config.crawl_token,
        "http_status": http_status,
        "content_hash": content_hash,
        "feed_data": feed_data,
    });
    if config.force_reingest {
        payload["force_reingest"] = serde_json::json!(true);
    }

    let ingest_resp = match config
        .ingest_client
        .post(&config.ingest_url)
        .json(&payload)
        .timeout(config.ingest_timeout)
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            return CrawlOutcome::IngestError {
                reason: e.to_string(),
                retryable: true,
                retry_after_secs: None,
            };
        }
    };

    let ingest_status = ingest_resp.status();
    let ingest_headers = ingest_resp.headers().clone();
    let resp_body = match ingest_resp.text().await {
        Ok(t) => t,
        Err(e) => {
            return CrawlOutcome::IngestError {
                reason: e.to_string(),
                retryable: true,
                retry_after_secs: None,
            };
        }
    };

    if !ingest_status.is_success() {
        return CrawlOutcome::IngestError {
            reason: format_ingest_http_error(ingest_status, &resp_body),
            retryable: is_retryable_ingest_status(ingest_status),
            retry_after_secs: parse_retry_after_secs(&ingest_headers),
        };
    }

    let parsed: IngestResponse = match serde_json::from_str(&resp_body) {
        Ok(r) => r,
        Err(_) => {
            return CrawlOutcome::IngestError {
                reason: format!("ingest http {ingest_status} non-JSON response: {resp_body}"),
                retryable: false,
                retry_after_secs: None,
            };
        }
    };

    let warnings = parsed.warnings.unwrap_or_default();

    if parsed.accepted {
        CrawlOutcome::Accepted { warnings }
    } else {
        let reason = normalize_rejection_reason(parsed.reason, ingest_status, &resp_body);
        if reason == "no_change" {
            CrawlOutcome::NoChange
        } else {
            CrawlOutcome::Rejected { reason, warnings }
        }
    }
}

/// Parse cached XML and POST it to `/ingest/feed`. Never panics.
pub async fn ingest_cached_feed_report(
    source_url: &str,
    canonical_url: &str,
    http_status: u16,
    raw_xml: &str,
    content_hash: Option<&str>,
    fallback_guid: Option<&str>,
    config: &CrawlConfig,
) -> CrawlReport {
    let content_hash = content_hash.map_or_else(
        || hex::encode(Sha256::digest(raw_xml.as_bytes())),
        ToOwned::to_owned,
    );

    let feed_data = match parse_feed_xml(raw_xml, fallback_guid) {
        Ok(data) => data,
        Err(e) => {
            return build_crawl_report(
                CrawlOutcome::ParseError(e),
                Some(http_status),
                None,
                Some(canonical_url.to_string()),
                Some(content_hash),
                Some(raw_xml.to_string()),
            );
        }
    };

    let outcome = post_ingest_payload(
        canonical_url,
        source_url,
        http_status,
        &content_hash,
        feed_data.clone(),
        config,
    )
    .await;

    build_crawl_report(
        outcome,
        Some(http_status),
        feed_data.as_ref(),
        Some(canonical_url.to_string()),
        Some(content_hash),
        Some(raw_xml.to_string()),
    )
}

/// Parse cached XML and POST it to `/ingest/feed`. Never panics.
pub async fn ingest_cached_feed(
    source_url: &str,
    canonical_url: &str,
    http_status: u16,
    raw_xml: &str,
    content_hash: Option<&str>,
    fallback_guid: Option<&str>,
    config: &CrawlConfig,
) -> CrawlOutcome {
    ingest_cached_feed_report(
        source_url,
        canonical_url,
        http_status,
        raw_xml,
        content_hash,
        fallback_guid,
        config,
    )
    .await
    .outcome
}

/// The action to take after a fetch response (ADR 0050 §3, `stophammer`
/// repository).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FetchAction {
    /// A `200` body. Parse it, POST it, and cache it.
    IngestFresh,
    /// A `304` this crawl must still submit: a forced pass, or a row whose
    /// last node answer does not prove the node holds the content.
    IngestKept,
    /// A `304` the node has already seen and answered. No POST.
    SkipIngest,
    /// A `304` with no kept body to trust. Fetch once more, unconditionally.
    RefetchUnconditional,
    /// Any other status. As today: a retryable or a final fetch error.
    FetchError,
}

/// Decide what a fetch response means, from ADR 0050 §3 (`stophammer`
/// repository). A pure function: it reads no clock and no store.
fn plan_after_response(status: u16, cached: Option<&CachedFeed>, force: bool) -> FetchAction {
    if status == 200 {
        return FetchAction::IngestFresh;
    }
    if status != 304 {
        return FetchAction::FetchError;
    }

    let Some(cached) = cached else {
        return FetchAction::RefetchUnconditional;
    };

    if force {
        return FetchAction::IngestKept;
    }

    match cached.node_answer.as_deref() {
        Some("accepted" | "no_change" | "rejected" | "parse_error") => FetchAction::SkipIngest,
        // A null answer, or `ingest_error`, means the node does not yet
        // hold this content. Submit the kept body so it does.
        None | Some(_) => FetchAction::IngestKept,
    }
}

/// The rejection reasons that mean the node holds no content for this URL
/// under its own source URL, or that an operator decision on the URL may
/// change with no changed body (`stophammer` ADR 0051 §5, ADR 0053 §1). The
/// crawler must not cache one of these as the node's answer, so a later
/// `304` submits the kept body again and the new decision takes effect.
const UNCACHED_NODE_REASONS: [&str; 5] = [
    "source_conflict",
    "record_conflict",
    "guid_change_pending",
    "blocked",
    "stale_submission",
];

/// Returns `true` when `outcome` is a rejection for one of the reasons in
/// `UNCACHED_NODE_REASONS` (`stophammer` ADR 0051 §5, ADR 0053 §1).
#[must_use]
fn is_uncached_node_answer(outcome: &CrawlOutcome) -> bool {
    matches!(
        outcome,
        CrawlOutcome::Rejected { reason, .. } if UNCACHED_NODE_REASONS.contains(&reason.as_str())
    )
}

/// Read one header's value as owned text, when it is present and valid
/// UTF-8.
fn header_str(headers: &HeaderMap, name: reqwest::header::HeaderName) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
}

/// Read the cached row for `url`, if a cache is given (ADR 0050 §2,
/// `stophammer` repository). A poisoned lock counts as no row. It does not
/// panic the crawl.
fn read_cached_row(cache: Option<&FeedCache>, url: &str) -> Option<CachedFeed> {
    let cache = cache?;
    match cache.lock() {
        Ok(guard) => guard.get(url),
        Err(e) => {
            eprintln!("crawl: WARNING: feed cache lock poisoned; treating {url} as uncached: {e}");
            None
        }
    }
}

/// Write a fresh cache row, then record the node's answer for it. Skips the
/// answer when `outcome` is one of `UNCACHED_NODE_REASONS` (`stophammer` ADR
/// 0051 §5, ADR 0053 §1): the row then keeps a null answer, so a later `304`
/// submits the kept body again. Each step takes its own lock, and neither is
/// held across an `.await` (ADR 0050 §2, `stophammer` repository). A
/// poisoned lock skips that one write. It does not panic the crawl.
fn write_cache_row(
    cache: &FeedCache,
    url: &str,
    entry: &FetchedFeed<'_>,
    outcome: &CrawlOutcome,
    at: i64,
) {
    match cache.lock() {
        Ok(guard) => guard.put(url, entry),
        Err(e) => {
            eprintln!(
                "crawl: WARNING: feed cache lock poisoned; skipped writing cache row for {url}: {e}"
            );
        }
    }
    if is_uncached_node_answer(outcome) {
        return;
    }
    match cache.lock() {
        Ok(guard) => guard.record_node_answer(url, outcome.label(), outcome.reason(), at),
        Err(e) => {
            eprintln!(
                "crawl: WARNING: feed cache lock poisoned; skipped recording node answer for {url}: {e}"
            );
        }
    }
}

/// Parse, POST, and cache a freshly fetched `200` body (ADR 0050 §3,
/// `stophammer` repository).
async fn ingest_fresh_and_cache(
    url: &str,
    final_url: Option<String>,
    headers: &HeaderMap,
    body: &[u8],
    fallback_guid: Option<&str>,
    config: &CrawlConfig,
    cache: Option<&FeedCache>,
) -> CrawlReport {
    let hash = hex::encode(Sha256::digest(body));
    let xml = String::from_utf8_lossy(body);
    let canonical_url = final_url.as_deref().unwrap_or(url);

    let report = ingest_cached_feed_report(
        url,
        canonical_url,
        200,
        &xml,
        Some(&hash),
        fallback_guid,
        config,
    )
    .await;

    if let Some(cache) = cache {
        let etag = header_str(headers, ETAG);
        let last_modified = header_str(headers, LAST_MODIFIED);
        let fetched_at = unix_now();
        let entry = FetchedFeed {
            final_url: canonical_url,
            etag: etag.as_deref(),
            last_modified: last_modified.as_deref(),
            content_sha256: &hash,
            body: &xml,
            fetched_at,
        };
        write_cache_row(cache, url, &entry, &report.outcome, fetched_at);
    }

    report
}

/// Fetch a URL with no conditional header, after a `304` this crawl cannot
/// trust (ADR 0050 §3, `stophammer` repository). Sends exactly one more
/// GET, then continues as for `200`. Never loops.
async fn refetch_unconditional(
    client: &reqwest::Client,
    url: &str,
    fallback_guid: Option<&str>,
    config: &CrawlConfig,
    cache: Option<&FeedCache>,
) -> CrawlReport {
    let resp = match client
        .get(url)
        .header("User-Agent", &config.user_agent)
        .timeout(config.fetch_timeout)
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            return build_crawl_report(
                CrawlOutcome::FetchError {
                    reason: e.to_string(),
                    retryable: true,
                    retry_after_secs: None,
                },
                None,
                None,
                None,
                None,
                None,
            );
        }
    };

    let status = resp.status().as_u16();
    let final_url = Some(resp.url().to_string());
    let headers = resp.headers().clone();

    let body = match resp.bytes().await {
        Ok(b) => b,
        Err(e) => {
            return build_crawl_report(
                CrawlOutcome::FetchError {
                    reason: e.to_string(),
                    retryable: true,
                    retry_after_secs: None,
                },
                Some(status),
                None,
                final_url,
                None,
                None,
            );
        }
    };

    if status != 200 {
        return build_crawl_report(
            CrawlOutcome::FetchError {
                reason: format_http_fetch_error(status, &headers, &body),
                retryable: is_retryable_http_status(status),
                retry_after_secs: parse_retry_after_secs(&headers),
            },
            Some(status),
            None,
            final_url,
            None,
            None,
        );
    }

    ingest_fresh_and_cache(
        url,
        final_url,
        &headers,
        &body,
        fallback_guid,
        config,
        cache,
    )
    .await
}

/// Submit the kept body for a `304` that must still reach the node (ADR
/// 0050 §3, `stophammer` repository): a forced pass, or a row whose last
/// answer does not prove the node holds this content.
async fn ingest_kept_body(
    url: &str,
    cached: &CachedFeed,
    fallback_guid: Option<&str>,
    config: &CrawlConfig,
    cache: Option<&FeedCache>,
) -> CrawlReport {
    let mut report = ingest_cached_feed_report(
        url,
        &cached.final_url,
        200,
        &cached.body,
        Some(&cached.content_sha256),
        fallback_guid,
        config,
    )
    .await;
    // The node saw an ingest, but the crawler's own fetch answered `304`.
    report.fetch_http_status = Some(304);

    if let Some(cache) = cache {
        // ADR 0051 §5, ADR 0053 §1 (`stophammer` repository): an uncached
        // node answer clears the earlier answer, so a later `304` submits
        // this kept body again.
        let at = unix_now();
        match cache.lock() {
            Ok(guard) if is_uncached_node_answer(&report.outcome) => guard.clear_node_answer(url),
            Ok(guard) => {
                guard.record_node_answer(url, report.outcome.label(), report.outcome.reason(), at);
            }
            Err(e) => {
                eprintln!(
                    "crawl: WARNING: feed cache lock poisoned; skipped recording node answer for {url}: {e}"
                );
            }
        }
    }

    report
}

/// Build a `NoChange` report from the kept body, with no ingest POST (ADR
/// 0050 §3, `stophammer` repository). The node has already answered for
/// this content.
fn skip_ingest_with_kept_body(cached: &CachedFeed, fallback_guid: Option<&str>) -> CrawlReport {
    let feed_data = match parse_feed_xml(&cached.body, fallback_guid) {
        Ok(data) => data,
        Err(e) => {
            return build_crawl_report(
                CrawlOutcome::ParseError(e),
                Some(304),
                None,
                Some(cached.final_url.clone()),
                Some(cached.content_sha256.clone()),
                Some(cached.body.clone()),
            );
        }
    };

    build_crawl_report(
        CrawlOutcome::NoChange,
        Some(304),
        feed_data.as_ref(),
        Some(cached.final_url.clone()),
        Some(cached.content_sha256.clone()),
        Some(cached.body.clone()),
    )
}

/// Fetch → SHA-256 → parse → POST. Never panics.
///
/// With `cache: None`, this behaves exactly as it did before ADR 0050
/// (`stophammer` repository). With a cache, it sends a conditional GET for
/// a URL the cache already holds, and follows `plan_after_response` for
/// the answer.
pub async fn crawl_feed_report(
    client: &reqwest::Client,
    url: &str,
    fallback_guid: Option<&str>,
    config: &CrawlConfig,
    cache: Option<&FeedCache>,
) -> CrawlReport {
    // Read the cached row before the request, and release the lock right
    // away. No lock is held across an `.await` (ADR 0050 §2, `stophammer`
    // repository).
    let cached = read_cached_row(cache, url);
    let send_conditional = config.revalidate && cached.is_some();

    // 1. Fetch
    let mut request = client
        .get(url)
        .header("User-Agent", &config.user_agent)
        .timeout(config.fetch_timeout);
    if send_conditional {
        let row = cached
            .as_ref()
            .expect("send_conditional is true only when a cached row exists");
        if let Some(etag) = &row.etag {
            request = request.header(IF_NONE_MATCH, etag.as_str());
        }
        if let Some(last_modified) = &row.last_modified {
            request = request.header(IF_MODIFIED_SINCE, last_modified.as_str());
        }
    }

    let resp = match request.send().await {
        Ok(r) => r,
        Err(e) => {
            return build_crawl_report(
                CrawlOutcome::FetchError {
                    reason: e.to_string(),
                    retryable: true,
                    retry_after_secs: None,
                },
                None,
                None,
                None,
                None,
                None,
            );
        }
    };

    let status = resp.status().as_u16();
    let final_url = Some(resp.url().to_string());
    let headers = resp.headers().clone();

    let body = match resp.bytes().await {
        Ok(b) => b,
        Err(e) => {
            return build_crawl_report(
                CrawlOutcome::FetchError {
                    reason: e.to_string(),
                    retryable: true,
                    retry_after_secs: None,
                },
                Some(status),
                None,
                final_url,
                None,
                None,
            );
        }
    };

    match plan_after_response(status, cached.as_ref(), config.force_reingest) {
        FetchAction::IngestFresh => {
            ingest_fresh_and_cache(
                url,
                final_url,
                &headers,
                &body,
                fallback_guid,
                config,
                cache,
            )
            .await
        }
        // A `304` to a request with no conditional header is a server
        // fault. Report it as before ADR 0050. Only a conditional request
        // earns the one unconditional refetch.
        FetchAction::RefetchUnconditional if send_conditional => {
            refetch_unconditional(client, url, fallback_guid, config, cache).await
        }
        FetchAction::IngestKept => {
            let row = cached
                .as_ref()
                .expect("plan_after_response gives IngestKept only with a cached row");
            ingest_kept_body(url, row, fallback_guid, config, cache).await
        }
        FetchAction::SkipIngest => {
            let row = cached
                .as_ref()
                .expect("plan_after_response gives SkipIngest only with a cached row");
            skip_ingest_with_kept_body(row, fallback_guid)
        }
        FetchAction::FetchError | FetchAction::RefetchUnconditional => build_crawl_report(
            CrawlOutcome::FetchError {
                reason: format_http_fetch_error(status, &headers, &body),
                retryable: is_retryable_http_status(status),
                retry_after_secs: parse_retry_after_secs(&headers),
            },
            Some(status),
            None,
            final_url,
            None,
            None,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CachedFeed, CrawlConfig, CrawlOutcome, FeedCache, FetchAction, body_preview,
        build_crawl_report, crawl_feed_report, format_http_fetch_error, format_ingest_http_error,
        is_retryable_http_status, is_retryable_ingest_status, is_uncached_node_answer,
        normalize_rejection_reason, parse_feed_xml, plan_after_response,
    };
    use crate::feed_cache::{FeedCacheDb, FetchedFeed};
    use reqwest::StatusCode;
    use reqwest::header::{HeaderMap, HeaderValue, RETRY_AFTER};
    use sha2::Digest;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use stophammer_parser::types::IngestFeedData;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{TcpListener, TcpStream};

    fn sample_feed_data() -> IngestFeedData {
        IngestFeedData {
            feed_guid: "feed-guid".to_string(),
            title: "Feed".to_string(),
            description: None,
            image_url: None,
            language: None,
            explicit: false,
            itunes_type: None,
            raw_medium: Some("music".to_string()),
            author_name: None,
            owner_name: None,
            pub_date: None,
            last_build_date: None,
            remote_items: Vec::new(),
            persons: Vec::new(),
            entity_ids: Vec::new(),
            links: Vec::new(),
            podcast_namespace: None,
            feed_payment_routes: Vec::new(),
            live_items: Vec::new(),
            tracks: Vec::new(),
        }
    }

    #[test]
    fn parse_feed_xml_preserves_publisher_feed_level_people_and_rss_artwork() {
        let xml = r#"<?xml version="1.0"?>
        <rss xmlns:podcast="https://podcastindex.org/namespace/1.0">
          <channel>
            <title>Publisher Feed</title>
            <podcast:guid>publisher-feed-guid</podcast:guid>
            <podcast:medium>publisher</podcast:medium>
            <image>
              <url>https://img.example.com/publisher-rss.jpg</url>
            </image>
            <podcast:person role="artist" group="music" href="https://example.com/artist" img="https://img.example.com/artist.jpg">Publisher Artist</podcast:person>
            <podcast:remoteItem medium="music" feedGuid="music-feed-guid" feedUrl="https://example.com/music.xml"/>
          </channel>
        </rss>"#;

        let parsed = parse_feed_xml(xml, None)
            .expect("publisher feed should parse")
            .expect("publisher feed data should be present");

        assert_eq!(parsed.raw_medium.as_deref(), Some("publisher"));
        assert_eq!(
            parsed.image_url.as_deref(),
            Some("https://img.example.com/publisher-rss.jpg")
        );
        assert_eq!(parsed.persons.len(), 1);
        assert_eq!(parsed.persons[0].name, "Publisher Artist");
        assert_eq!(
            parsed.persons[0].img.as_deref(),
            Some("https://img.example.com/artist.jpg")
        );
        assert_eq!(parsed.remote_items.len(), 1);
    }

    #[test]
    fn retryable_statuses_cover_429_and_5xx() {
        assert!(is_retryable_http_status(429));
        assert!(is_retryable_http_status(503));
        assert!(!is_retryable_http_status(404));
    }

    #[test]
    fn ingest_retries_429_but_not_internal_server_errors() {
        assert!(is_retryable_ingest_status(StatusCode::TOO_MANY_REQUESTS));
        assert!(is_retryable_ingest_status(StatusCode::SERVICE_UNAVAILABLE));
        assert!(!is_retryable_ingest_status(
            StatusCode::INTERNAL_SERVER_ERROR
        ));
    }

    #[test]
    fn fetch_outcome_reports_retryability() {
        let outcome = CrawlOutcome::FetchError {
            reason: "http 429 Too Many Requests".to_string(),
            retryable: true,
            retry_after_secs: Some(30),
        };

        assert!(outcome.is_retryable());
        assert_eq!(outcome.retry_delay(1), Some(Duration::from_secs(30)));
    }

    #[test]
    fn ingest_outcome_reports_retryability() {
        let outcome = CrawlOutcome::IngestError {
            reason: "ingest http 429 Too Many Requests".to_string(),
            retryable: true,
            retry_after_secs: None,
        };

        assert!(outcome.is_retryable());
        assert_eq!(outcome.retry_delay(2), Some(Duration::from_secs(2)));
    }

    #[test]
    fn http_error_message_includes_retry_after_and_body_preview() {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_static("60"));
        let message = format_http_fetch_error(429, &headers, b"Too Many Requests");

        assert!(message.contains("http 429"));
        assert!(message.contains("retry-after=60"));
        assert!(message.contains("Too Many Requests"));
    }

    #[test]
    fn body_preview_collapses_whitespace() {
        assert_eq!(body_preview(b"Too   Many\nRequests"), "Too Many Requests");
    }

    #[test]
    fn empty_rejection_reason_falls_back_to_response_context() {
        let reason = normalize_rejection_reason(None, StatusCode::OK, "{\"accepted\":false}");

        assert!(reason.contains("empty ingest rejection reason"));
        assert!(reason.contains("{\"accepted\":false}"));
    }

    #[test]
    fn ingest_http_error_includes_status_and_body() {
        let reason = format_ingest_http_error(
            StatusCode::TOO_MANY_REQUESTS,
            "{\"error\":\"rate limit exceeded\"}",
        );

        assert!(reason.contains("429"));
        assert!(reason.contains("rate limit exceeded"));
    }

    #[test]
    fn crawl_report_keeps_fetch_status_for_fetch_errors() {
        let report = build_crawl_report(
            CrawlOutcome::FetchError {
                reason: "http 404 Not Found".to_string(),
                retryable: false,
                retry_after_secs: None,
            },
            Some(404),
            None,
            None,
            None,
            None,
        );

        assert_eq!(report.fetch_http_status, Some(404));
        assert_eq!(report.raw_medium, None);
        assert_eq!(report.parsed_feed_guid, None);
        assert_eq!(report.outcome.label(), "fetch_error");
    }

    #[test]
    fn crawl_report_keeps_parsed_medium_and_guid() {
        let feed_data = sample_feed_data();
        let report = build_crawl_report(
            CrawlOutcome::Accepted {
                warnings: Vec::new(),
            },
            Some(200),
            Some(&feed_data),
            Some("https://example.com/feed.xml".to_string()),
            Some("abc123".to_string()),
            Some("<rss/>".to_string()),
        );

        assert_eq!(report.fetch_http_status, Some(200));
        assert_eq!(report.raw_medium.as_deref(), Some("music"));
        assert_eq!(report.parsed_feed_guid.as_deref(), Some("feed-guid"));
        assert_eq!(
            report.final_url.as_deref(),
            Some("https://example.com/feed.xml")
        );
        assert_eq!(report.content_sha256.as_deref(), Some("abc123"));
        assert_eq!(report.raw_xml.as_deref(), Some("<rss/>"));
    }

    #[test]
    fn crawl_report_parse_error_keeps_fetch_status() {
        let report = build_crawl_report(
            CrawlOutcome::ParseError("invalid xml".to_string()),
            Some(200),
            None,
            Some("https://example.com/feed.xml".to_string()),
            Some("abc123".to_string()),
            Some("<rss/>".to_string()),
        );

        assert_eq!(report.fetch_http_status, Some(200));
        assert_eq!(report.raw_medium, None);
        assert_eq!(report.outcome.reason(), Some("invalid xml"));
    }

    // ---- `plan_after_response`: one test for each ADR 0050 §3 rule ----

    fn cached_with_answer(node_answer: Option<&str>) -> CachedFeed {
        CachedFeed {
            url: "https://example.com/feed.xml".to_string(),
            final_url: "https://example.com/feed.xml".to_string(),
            etag: Some("\"v1\"".to_string()),
            last_modified: None,
            content_sha256: "deadbeef".to_string(),
            body: "<rss/>".to_string(),
            fetched_at: 0,
            node_answer: node_answer.map(ToOwned::to_owned),
            node_reason: None,
            answered_at: None,
        }
    }

    #[test]
    fn plan_after_response_200_is_always_ingest_fresh() {
        assert_eq!(
            plan_after_response(200, None, false),
            FetchAction::IngestFresh
        );
        let cached = cached_with_answer(Some("accepted"));
        assert_eq!(
            plan_after_response(200, Some(&cached), true),
            FetchAction::IngestFresh,
            "a 200 body is always fresh, even with a cached row and force set"
        );
    }

    #[test]
    fn plan_after_response_304_with_force_ingests_kept_body() {
        let cached = cached_with_answer(Some("accepted"));
        assert_eq!(
            plan_after_response(304, Some(&cached), true),
            FetchAction::IngestKept,
            "a force pass must submit the kept body on a 304"
        );
    }

    #[test]
    fn plan_after_response_304_no_force_accepted_skips_ingest() {
        let cached = cached_with_answer(Some("accepted"));
        assert_eq!(
            plan_after_response(304, Some(&cached), false),
            FetchAction::SkipIngest
        );
    }

    #[test]
    fn plan_after_response_304_no_force_no_change_skips_ingest() {
        let cached = cached_with_answer(Some("no_change"));
        assert_eq!(
            plan_after_response(304, Some(&cached), false),
            FetchAction::SkipIngest
        );
    }

    #[test]
    fn plan_after_response_304_no_force_rejected_skips_ingest() {
        let cached = cached_with_answer(Some("rejected"));
        assert_eq!(
            plan_after_response(304, Some(&cached), false),
            FetchAction::SkipIngest
        );
    }

    #[test]
    fn plan_after_response_304_no_force_parse_error_skips_ingest() {
        let cached = cached_with_answer(Some("parse_error"));
        assert_eq!(
            plan_after_response(304, Some(&cached), false),
            FetchAction::SkipIngest
        );
    }

    #[test]
    fn plan_after_response_304_no_force_null_answer_ingests_kept_body() {
        let cached = cached_with_answer(None);
        assert_eq!(
            plan_after_response(304, Some(&cached), false),
            FetchAction::IngestKept,
            "a null node answer means the node may not hold this content yet"
        );
    }

    #[test]
    fn plan_after_response_304_no_force_ingest_error_ingests_kept_body() {
        let cached = cached_with_answer(Some("ingest_error"));
        assert_eq!(
            plan_after_response(304, Some(&cached), false),
            FetchAction::IngestKept,
            "an ingest_error answer means the node does not hold this content"
        );
    }

    #[test]
    fn plan_after_response_304_with_no_kept_body_refetches_unconditionally() {
        assert_eq!(
            plan_after_response(304, None, false),
            FetchAction::RefetchUnconditional
        );
        assert_eq!(
            plan_after_response(304, None, true),
            FetchAction::RefetchUnconditional,
            "a force pass with no kept body still cannot trust the 304"
        );
    }

    #[test]
    fn plan_after_response_other_status_is_fetch_error() {
        assert_eq!(
            plan_after_response(429, None, false),
            FetchAction::FetchError
        );
        let cached = cached_with_answer(Some("accepted"));
        assert_eq!(
            plan_after_response(500, Some(&cached), false),
            FetchAction::FetchError
        );
    }

    // ---- `is_uncached_node_answer` (`stophammer` ADR 0051 §5, ADR 0053 §1) ----

    #[test]
    fn is_uncached_node_answer_is_true_for_each_of_the_five_reasons() {
        for reason in [
            "source_conflict",
            "record_conflict",
            "guid_change_pending",
            "blocked",
            "stale_submission",
        ] {
            let outcome = CrawlOutcome::Rejected {
                reason: reason.to_string(),
                warnings: Vec::new(),
            };
            assert!(
                is_uncached_node_answer(&outcome),
                "{reason} must not be cached as the node's answer under ADR 0051 §5 / ADR 0053 §1"
            );
        }
    }

    #[test]
    fn is_uncached_node_answer_is_false_for_a_non_matching_reason_or_outcome() {
        for reason in [
            "[medium_music] absent",
            "source_conflict_extra",
            "blocked_extra",
        ] {
            let outcome = CrawlOutcome::Rejected {
                reason: reason.to_string(),
                warnings: Vec::new(),
            };
            assert!(
                !is_uncached_node_answer(&outcome),
                "{reason} must be cached as an ordinary node answer"
            );
        }

        assert!(
            !is_uncached_node_answer(&CrawlOutcome::Accepted {
                warnings: Vec::new()
            }),
            "an accepted outcome always caches a node answer"
        );
    }

    // ---- stub-server tests for `crawl_feed_report` ----
    //
    // Each stub is a plain TCP listener on `127.0.0.1`. It reads one
    // request head (and body, for a POST), then writes back a fixed
    // HTTP/1.1 response with `Content-Length` and `Connection: close`. No
    // test sends a request to an external host.

    /// One HTTP/1.1 request, captured from a stub connection.
    struct StubRequest {
        method: String,
        body: Vec<u8>,
        headers: HashMap<String, String>,
    }

    impl StubRequest {
        /// Read one header's value, by a case-insensitive name.
        fn header(&self, name: &str) -> Option<&str> {
            self.headers
                .get(&name.to_ascii_lowercase())
                .map(String::as_str)
        }
    }

    /// Read one HTTP/1.1 request head and body from `stream`.
    async fn read_stub_request(stream: &mut TcpStream) -> StubRequest {
        let mut reader = BufReader::new(stream);

        let mut request_line = String::new();
        reader
            .read_line(&mut request_line)
            .await
            .expect("read stub request line");
        let method = request_line
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .to_string();

        let mut headers = HashMap::new();
        loop {
            let mut line = String::new();
            reader
                .read_line(&mut line)
                .await
                .expect("read stub header line");
            let line = line.trim_end_matches(['\r', '\n']);
            if line.is_empty() {
                break;
            }
            if let Some((name, value)) = line.split_once(':') {
                headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
            }
        }

        let content_length: usize = headers
            .get("content-length")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        let mut body = vec![0_u8; content_length];
        if content_length > 0 {
            reader
                .read_exact(&mut body)
                .await
                .expect("read stub request body");
        }

        StubRequest {
            method,
            body,
            headers,
        }
    }

    /// Build a fixed HTTP/1.1 response, with `Content-Length` and
    /// `Connection: close` set from `body`.
    fn stub_response(status_line: &str, extra_headers: &[(&str, &str)], body: &[u8]) -> Vec<u8> {
        use std::fmt::Write as _;

        let mut head = format!("{status_line}\r\n");
        for (name, value) in extra_headers {
            let _ = writeln!(head, "{name}: {value}\r");
        }
        let _ = writeln!(head, "content-length: {}\r", body.len());
        head.push_str("connection: close\r\n\r\n");
        let mut out = head.into_bytes();
        out.extend_from_slice(body);
        out
    }

    /// Start a stub HTTP/1.1 server, on `127.0.0.1`, for exactly one
    /// request. It answers with `response`, then gives the captured
    /// request back through the returned handle.
    async fn spawn_stub(response: Vec<u8>) -> (String, tokio::task::JoinHandle<StubRequest>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind stub listener");
        let addr = listener.local_addr().expect("stub local addr");
        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept stub connection");
            let request = read_stub_request(&mut stream).await;
            stream
                .write_all(&response)
                .await
                .expect("write stub response");
            let _ = stream.shutdown().await;
            request
        });
        (format!("127.0.0.1:{}", addr.port()), handle)
    }

    /// A fresh, empty fetch-cache database, at a temporary path that
    /// outlives the test.
    fn test_cache() -> FeedCache {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("feed_cache.db");
        let path = path.to_str().expect("path is valid UTF-8").to_string();
        // Leak so the directory survives the test.
        std::mem::forget(dir);
        Arc::new(Mutex::new(FeedCacheDb::open(&path)))
    }

    /// A `CrawlConfig` for a stub test. Never reads the environment.
    fn test_config(ingest_url: String) -> CrawlConfig {
        let mut config =
            CrawlConfig::dry_run("stophammer-crawler-test/1.0", Duration::from_secs(5));
        config.ingest_url = ingest_url;
        config
    }

    /// A minimal feed that `parse_feed_xml` turns into `Some(..)`.
    const KEPT_BODY: &str = r#"<?xml version="1.0"?>
<rss xmlns:podcast="https://podcastindex.org/namespace/1.0">
  <channel>
    <title>Cached Feed</title>
    <podcast:guid>cached-feed-guid</podcast:guid>
    <podcast:medium>music</podcast:medium>
  </channel>
</rss>"#;

    fn kept_body_hash() -> String {
        hex::encode(sha2::Sha256::digest(KEPT_BODY.as_bytes()))
    }

    /// Seed a cache row for `url`, with the kept body and its hash. Also
    /// record `node_answer`, when one is given.
    fn seed_cached_row(cache: &FeedCache, url: &str, node_answer: Option<&str>) {
        let db = cache.lock().expect("cache lock");
        db.put(
            url,
            &FetchedFeed {
                final_url: url,
                etag: Some("\"v1\""),
                last_modified: None,
                content_sha256: &kept_body_hash(),
                body: KEPT_BODY,
                fetched_at: 0,
            },
        );
        if let Some(node_answer) = node_answer {
            db.record_node_answer(url, node_answer, None, 0);
        }
    }

    /// Proves ADR 0050 §3 (`stophammer` repository), the `200` row: a `200`
    /// still parses and POSTs the body, and now also writes the cache row
    /// with the response's `ETag`, the body hash, and the node's answer.
    #[tokio::test]
    async fn stub_200_caches_the_row_and_posts_the_ingest() {
        let body = b"<rss><channel><title>Feed</title></channel></rss>";
        let (feed_addr, feed_handle) = spawn_stub(stub_response(
            "HTTP/1.1 200 OK",
            &[("etag", "\"v1\"")],
            body,
        ))
        .await;
        let (ingest_addr, ingest_handle) = spawn_stub(stub_response(
            "HTTP/1.1 200 OK",
            &[("content-type", "application/json")],
            br#"{"accepted":true}"#,
        ))
        .await;

        let client = reqwest::Client::new();
        let config = test_config(format!("http://{ingest_addr}/ingest/feed"));
        let cache = test_cache();
        let feed_url = format!("http://{feed_addr}/feed.xml");

        let report = crawl_feed_report(&client, &feed_url, None, &config, Some(&cache)).await;

        let feed_request = feed_handle.await.expect("feed stub task");
        let ingest_request = ingest_handle.await.expect("ingest stub task");

        assert_eq!(feed_request.method, "GET");
        assert_eq!(
            ingest_request.method, "POST",
            "a 200 fetch must still POST the ingest, as before ADR 0050"
        );
        assert_eq!(
            report.outcome,
            CrawlOutcome::Accepted {
                warnings: Vec::new()
            }
        );
        assert_eq!(report.fetch_http_status, Some(200));

        let expected_hash = hex::encode(sha2::Sha256::digest(body));
        let cached = cache
            .lock()
            .expect("cache lock")
            .get(&feed_url)
            .expect("a 200 fetch must write a cache row");
        assert_eq!(cached.etag.as_deref(), Some("\"v1\""));
        assert_eq!(cached.content_sha256, expected_hash);
        assert_eq!(
            cached.node_answer.as_deref(),
            Some("accepted"),
            "the row must carry the node's answer to the fresh submission"
        );
    }

    /// Proves `stophammer` ADR 0051 §5, the `200` node-conflict row: the row
    /// is still written for the fresh body, but the crawler does not cache
    /// `source_conflict` as the node's answer. The row keeps a null answer,
    /// so a later `304` submits the kept body again.
    #[tokio::test]
    async fn stub_200_with_a_source_conflict_leaves_the_row_with_no_node_answer() {
        let body = b"<rss><channel><title>Feed</title></channel></rss>";
        let (feed_addr, feed_handle) = spawn_stub(stub_response(
            "HTTP/1.1 200 OK",
            &[("etag", "\"v1\"")],
            body,
        ))
        .await;
        let (ingest_addr, ingest_handle) = spawn_stub(stub_response(
            "HTTP/1.1 200 OK",
            &[("content-type", "application/json")],
            br#"{"accepted":false,"reason":"source_conflict"}"#,
        ))
        .await;

        let client = reqwest::Client::new();
        let config = test_config(format!("http://{ingest_addr}/ingest/feed"));
        let cache = test_cache();
        let feed_url = format!("http://{feed_addr}/feed.xml");

        let report = crawl_feed_report(&client, &feed_url, None, &config, Some(&cache)).await;

        feed_handle.await.expect("feed stub task");
        ingest_handle.await.expect("ingest stub task");

        assert_eq!(
            report.outcome,
            CrawlOutcome::Rejected {
                reason: "source_conflict".to_string(),
                warnings: Vec::new(),
            },
            "the run summary must still report the rejection and its reason"
        );

        let cached = cache
            .lock()
            .expect("cache lock")
            .get(&feed_url)
            .expect("a 200 fetch must still write a cache row");
        assert_eq!(
            cached.node_answer, None,
            "ADR 0051 §5: a node conflict must leave no cached node answer"
        );
    }

    /// Proves `stophammer` ADR 0053 §1, the `200` blocked row: the row is
    /// still written for the fresh body, but the crawler does not cache
    /// `blocked` as the node's answer. The row keeps a null answer, so an
    /// unblock takes effect on the next `304` with no changed body.
    #[tokio::test]
    async fn stub_200_with_a_blocked_answer_leaves_the_row_with_no_node_answer() {
        let body = b"<rss><channel><title>Feed</title></channel></rss>";
        let (feed_addr, feed_handle) = spawn_stub(stub_response(
            "HTTP/1.1 200 OK",
            &[("etag", "\"v1\"")],
            body,
        ))
        .await;
        let (ingest_addr, ingest_handle) = spawn_stub(stub_response(
            "HTTP/1.1 200 OK",
            &[("content-type", "application/json")],
            br#"{"accepted":false,"reason":"blocked"}"#,
        ))
        .await;

        let client = reqwest::Client::new();
        let config = test_config(format!("http://{ingest_addr}/ingest/feed"));
        let cache = test_cache();
        let feed_url = format!("http://{feed_addr}/feed.xml");

        let report = crawl_feed_report(&client, &feed_url, None, &config, Some(&cache)).await;

        feed_handle.await.expect("feed stub task");
        ingest_handle.await.expect("ingest stub task");

        assert_eq!(
            report.outcome,
            CrawlOutcome::Rejected {
                reason: "blocked".to_string(),
                warnings: Vec::new(),
            },
            "the run summary must still report the rejection and its reason"
        );

        let cached = cache
            .lock()
            .expect("cache lock")
            .get(&feed_url)
            .expect("a 200 fetch must still write a cache row");
        assert_eq!(
            cached.node_answer, None,
            "ADR 0053 §1: a blocked answer must leave no cached node answer"
        );
    }

    /// Proves `stophammer` ADR 0051 §5 does not change an ordinary
    /// rejection: the node answer is still cached as `rejected`, so a `304`
    /// after it still skips the ingest (see
    /// `plan_after_response_304_no_force_rejected_skips_ingest`).
    #[tokio::test]
    async fn stub_200_with_an_ordinary_rejection_still_caches_the_node_answer() {
        let body = b"<rss><channel><title>Feed</title></channel></rss>";
        let (feed_addr, feed_handle) = spawn_stub(stub_response(
            "HTTP/1.1 200 OK",
            &[("etag", "\"v1\"")],
            body,
        ))
        .await;
        let (ingest_addr, ingest_handle) = spawn_stub(stub_response(
            "HTTP/1.1 200 OK",
            &[("content-type", "application/json")],
            br#"{"accepted":false,"reason":"[medium_music] absent"}"#,
        ))
        .await;

        let client = reqwest::Client::new();
        let config = test_config(format!("http://{ingest_addr}/ingest/feed"));
        let cache = test_cache();
        let feed_url = format!("http://{feed_addr}/feed.xml");

        let report = crawl_feed_report(&client, &feed_url, None, &config, Some(&cache)).await;

        feed_handle.await.expect("feed stub task");
        ingest_handle.await.expect("ingest stub task");

        assert_eq!(
            report.outcome,
            CrawlOutcome::Rejected {
                reason: "[medium_music] absent".to_string(),
                warnings: Vec::new(),
            }
        );

        let cached = cache
            .lock()
            .expect("cache lock")
            .get(&feed_url)
            .expect("a 200 fetch must still write a cache row");
        assert_eq!(
            cached.node_answer.as_deref(),
            Some("rejected"),
            "an ordinary rejection is not a node conflict, so it is still cached"
        );
    }

    /// Proves ADR 0050 §3 (`stophammer` repository), the `304` normal row:
    /// the crawler sends the stored `ETag`, and a `304` from a row the node
    /// already answered sends no ingest.
    #[tokio::test]
    async fn stub_304_normal_skips_ingest_and_reports_no_change() {
        let (feed_addr, feed_handle) =
            spawn_stub(stub_response("HTTP/1.1 304 Not Modified", &[], b"")).await;
        let feed_url = format!("http://{feed_addr}/feed.xml");

        let cache = test_cache();
        seed_cached_row(&cache, &feed_url, Some("accepted"));

        let client = reqwest::Client::new();
        // No ingest stub: a SkipIngest report must send no POST.
        let config = test_config(String::new());

        let report = crawl_feed_report(&client, &feed_url, None, &config, Some(&cache)).await;

        let feed_request = feed_handle.await.expect("feed stub task");
        assert_eq!(
            feed_request.header("if-none-match"),
            Some("\"v1\""),
            "the request must carry the stored ETag"
        );

        assert_eq!(report.outcome, CrawlOutcome::NoChange);
        assert_eq!(report.fetch_http_status, Some(304));
        assert!(
            report.parsed_feed.is_some(),
            "a SkipIngest report must still carry the parsed feed"
        );
    }

    /// Proves ADR 0050 §3 (`stophammer` repository), the `304` force row: a
    /// forced pass submits the kept body, with its kept hash, and updates
    /// the row's node answer.
    #[tokio::test]
    async fn stub_304_force_ingests_kept_body_and_updates_the_answer() {
        let (feed_addr, feed_handle) =
            spawn_stub(stub_response("HTTP/1.1 304 Not Modified", &[], b"")).await;
        let feed_url = format!("http://{feed_addr}/feed.xml");

        let cache = test_cache();
        seed_cached_row(&cache, &feed_url, Some("accepted"));

        let (ingest_addr, ingest_handle) = spawn_stub(stub_response(
            "HTTP/1.1 200 OK",
            &[("content-type", "application/json")],
            br#"{"accepted":true}"#,
        ))
        .await;

        let client = reqwest::Client::new();
        let mut config = test_config(format!("http://{ingest_addr}/ingest/feed"));
        config.force_reingest = true;

        let report = crawl_feed_report(&client, &feed_url, None, &config, Some(&cache)).await;

        let ingest_request = ingest_handle.await.expect("ingest stub task");
        feed_handle.await.expect("feed stub task");

        assert_eq!(ingest_request.method, "POST");
        let ingest_body: serde_json::Value =
            serde_json::from_slice(&ingest_request.body).expect("ingest body is JSON");
        assert_eq!(
            ingest_body["content_hash"].as_str(),
            Some(kept_body_hash().as_str()),
            "a forced pass must submit the kept body's hash"
        );

        assert_eq!(report.fetch_http_status, Some(304));
        assert_eq!(
            report.outcome,
            CrawlOutcome::Accepted {
                warnings: Vec::new()
            }
        );

        let updated = cache
            .lock()
            .expect("cache lock")
            .get(&feed_url)
            .expect("row must still exist");
        assert_eq!(
            updated.node_answer.as_deref(),
            Some("accepted"),
            "the row's answer must be updated after the forced submission"
        );
    }

    /// Proves `stophammer` ADR 0051 §5, the `304` force node-conflict row: a
    /// forced pass that resubmits the kept body and gets a node conflict
    /// clears the row's earlier answer. A later normal `304` then submits the
    /// kept body again, after an operator decision.
    #[tokio::test]
    async fn stub_304_force_with_a_node_conflict_clears_the_earlier_answer() {
        let (feed_addr, feed_handle) =
            spawn_stub(stub_response("HTTP/1.1 304 Not Modified", &[], b"")).await;
        let feed_url = format!("http://{feed_addr}/feed.xml");

        let cache = test_cache();
        seed_cached_row(&cache, &feed_url, Some("accepted"));

        let (ingest_addr, ingest_handle) = spawn_stub(stub_response(
            "HTTP/1.1 200 OK",
            &[("content-type", "application/json")],
            br#"{"accepted":false,"reason":"record_conflict"}"#,
        ))
        .await;

        let client = reqwest::Client::new();
        let mut config = test_config(format!("http://{ingest_addr}/ingest/feed"));
        config.force_reingest = true;

        let report = crawl_feed_report(&client, &feed_url, None, &config, Some(&cache)).await;

        ingest_handle.await.expect("ingest stub task");
        feed_handle.await.expect("feed stub task");

        assert_eq!(
            report.outcome,
            CrawlOutcome::Rejected {
                reason: "record_conflict".to_string(),
                warnings: Vec::new(),
            },
            "the run summary must still report the rejection and its reason"
        );

        let updated = cache
            .lock()
            .expect("cache lock")
            .get(&feed_url)
            .expect("row must still exist");
        assert_eq!(
            updated.node_answer, None,
            "ADR 0051 §5: a node conflict must clear an earlier answer"
        );
    }

    /// Proves ADR 0050 §3 (`stophammer` repository), the `304` null-answer
    /// row: a row the node has never answered still needs an ingest.
    #[tokio::test]
    async fn stub_304_with_null_answer_ingests_the_kept_body() {
        let (feed_addr, feed_handle) =
            spawn_stub(stub_response("HTTP/1.1 304 Not Modified", &[], b"")).await;
        let feed_url = format!("http://{feed_addr}/feed.xml");

        let cache = test_cache();
        // No `record_node_answer` call: the row's answer stays null.
        seed_cached_row(&cache, &feed_url, None);

        let (ingest_addr, ingest_handle) = spawn_stub(stub_response(
            "HTTP/1.1 200 OK",
            &[("content-type", "application/json")],
            br#"{"accepted":true}"#,
        ))
        .await;

        let client = reqwest::Client::new();
        let config = test_config(format!("http://{ingest_addr}/ingest/feed"));

        let report = crawl_feed_report(&client, &feed_url, None, &config, Some(&cache)).await;

        let ingest_request = ingest_handle.await.expect("ingest stub task");
        feed_handle.await.expect("feed stub task");

        assert_eq!(
            ingest_request.method, "POST",
            "a null node answer means the node may not hold this content yet"
        );
        assert_eq!(report.fetch_http_status, Some(304));
    }

    /// Proves ADR 0050 §3 (`stophammer` repository), the `429` row: any
    /// other status behaves as it did before ADR 0050, and writes no row.
    #[tokio::test]
    async fn stub_429_is_a_retryable_fetch_error_and_the_row_is_unchanged() {
        let (feed_addr, feed_handle) = spawn_stub(stub_response(
            "HTTP/1.1 429 Too Many Requests",
            &[],
            b"slow down",
        ))
        .await;
        let feed_url = format!("http://{feed_addr}/feed.xml");

        let cache = test_cache();
        let client = reqwest::Client::new();
        let config = test_config(String::new());

        let report = crawl_feed_report(&client, &feed_url, None, &config, Some(&cache)).await;
        feed_handle.await.expect("feed stub task");

        match &report.outcome {
            CrawlOutcome::FetchError { retryable, .. } => {
                assert!(*retryable, "a 429 must be a retryable fetch error");
            }
            other => panic!("expected a retryable FetchError, got {other:?}"),
        }
        assert_eq!(report.fetch_http_status, Some(429));
        assert_eq!(
            cache.lock().expect("cache lock").get(&feed_url),
            None,
            "a non-200/304 status must not write a cache row"
        );
    }

    /// With no cache, a `304` stays a fetch error, as before ADR 0050
    /// (`stophammer` repository). The crawler sends no second GET: the stub
    /// accepts one connection, and a second one would fail and give a
    /// report with no status.
    #[tokio::test]
    async fn stub_304_with_no_cache_is_a_fetch_error_and_sends_one_request() {
        let (feed_addr, feed_handle) =
            spawn_stub(stub_response("HTTP/1.1 304 Not Modified", &[], b"")).await;
        let feed_url = format!("http://{feed_addr}/feed.xml");

        let client = reqwest::Client::new();
        let config = test_config(String::new());

        let report = crawl_feed_report(&client, &feed_url, None, &config, None).await;
        let request = feed_handle.await.expect("feed stub task");

        assert!(
            !request.headers.contains_key("if-none-match"),
            "a request with no cache must carry no conditional header"
        );
        assert!(
            matches!(report.outcome, CrawlOutcome::FetchError { .. }),
            "a 304 to an unconditional request must be a FetchError, got {:?}",
            report.outcome
        );
        assert_eq!(
            report.fetch_http_status,
            Some(304),
            "the report must carry the status of the one request that was sent"
        );
    }

    /// Proves ADR 0050 §5 (`stophammer` repository): `revalidate = false`
    /// sends no conditional header, even when the cache holds a row.
    #[tokio::test]
    async fn stub_revalidate_false_sends_no_conditional_header() {
        let (feed_addr, feed_handle) =
            spawn_stub(stub_response("HTTP/1.1 304 Not Modified", &[], b"")).await;
        let feed_url = format!("http://{feed_addr}/feed.xml");

        let cache = test_cache();
        seed_cached_row(&cache, &feed_url, Some("accepted"));

        let client = reqwest::Client::new();
        let mut config = test_config(String::new());
        config.revalidate = false;

        let _report = crawl_feed_report(&client, &feed_url, None, &config, Some(&cache)).await;

        let feed_request = feed_handle.await.expect("feed stub task");
        assert_eq!(
            feed_request.header("if-none-match"),
            None,
            "revalidate = false must send no conditional header"
        );
    }
}
