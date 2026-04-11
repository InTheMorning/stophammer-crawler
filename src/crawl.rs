use std::time::Duration;

use reqwest::header::{HeaderMap, RETRY_AFTER};
use sha2::{Digest, Sha256};
use stophammer_parser::profile;
use stophammer_parser::types::IngestFeedData;

const HTTP_ERROR_PREVIEW_LIMIT: usize = 160;

/// Configuration shared by all crawl modes.
pub struct CrawlConfig {
    pub crawl_token: String,
    pub ingest_url: String,
    pub user_agent: String,
    pub fetch_timeout: Duration,
    pub ingest_timeout: Duration,
    /// When true, the ingest server skips the content-hash dedup check.
    pub force_reingest: bool,
    /// Dedicated HTTP client for ingest POSTs, built with connection pooling
    /// disabled so stale keep-alive sockets never cause spurious failures.
    ingest_client: reqwest::Client,
}

impl CrawlConfig {
    #[allow(
        dead_code,
        reason = "analysis binaries import crawl.rs via #[path] and call this"
    )]
    pub fn from_env() -> Self {
        Self::from_env_with_force(false)
    }

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
            ingest_client: Self::build_ingest_client(ingest_timeout),
        }
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
#[allow(
    dead_code,
    reason = "analysis binaries import crawl.rs via #[path] and call this shared helper"
)]
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

/// Fetch → SHA-256 → parse → POST. Never panics.
pub async fn crawl_feed_report(
    client: &reqwest::Client,
    url: &str,
    fallback_guid: Option<&str>,
    config: &CrawlConfig,
) -> CrawlReport {
    // 1. Fetch
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

    // 2. SHA-256 hash of raw bytes
    let hash = hex::encode(Sha256::digest(&body));
    let xml = String::from_utf8_lossy(&body);

    ingest_cached_feed_report(
        url,
        final_url.as_deref().unwrap_or(url),
        status,
        &xml,
        Some(&hash),
        fallback_guid,
        config,
    )
    .await
}

/// Fetch → SHA-256 → parse → POST. Never panics.
pub async fn crawl_feed(
    client: &reqwest::Client,
    url: &str,
    fallback_guid: Option<&str>,
    config: &CrawlConfig,
) -> CrawlOutcome {
    crawl_feed_report(client, url, fallback_guid, config)
        .await
        .outcome
}

#[cfg(test)]
mod tests {
    use super::{
        CrawlOutcome, body_preview, build_crawl_report, format_http_fetch_error,
        format_ingest_http_error, is_retryable_http_status, is_retryable_ingest_status,
        normalize_rejection_reason,
    };
    use reqwest::StatusCode;
    use reqwest::header::{HeaderMap, HeaderValue, RETRY_AFTER};
    use std::time::Duration;
    use stophammer_parser::types::IngestFeedData;

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
}
