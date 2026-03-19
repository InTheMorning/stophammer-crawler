use std::time::Duration;

use sha2::{Digest, Sha256};
use stophammer_parser::profile;
use stophammer_parser::types::IngestFeedData;

/// Configuration shared by all crawl modes.
pub struct CrawlConfig {
    pub crawl_token: String,
    pub ingest_url: String,
    pub user_agent: String,
    pub fetch_timeout: Duration,
    pub ingest_timeout: Duration,
}

impl CrawlConfig {
    pub fn from_env() -> Self {
        Self {
            crawl_token: std::env::var("CRAWL_TOKEN").expect("CRAWL_TOKEN is required"),
            ingest_url: std::env::var("INGEST_URL")
                .unwrap_or_else(|_| "http://localhost:8008/ingest/feed".to_string()),
            user_agent: "stophammer-crawler/0.1".to_string(),
            fetch_timeout: Duration::from_secs(20),
            ingest_timeout: Duration::from_secs(10),
        }
    }
}

/// Outcome of a single feed crawl attempt.
#[derive(Debug)]
pub enum CrawlOutcome {
    Accepted {
        warnings: Vec<String>,
    },
    Rejected {
        reason: String,
        warnings: Vec<String>,
    },
    NoChange,
    FetchError(String),
    ParseError(String),
    IngestError(String),
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
            Self::FetchError(e) => write!(f, "fetch_error: {e}"),
            Self::ParseError(e) => write!(f, "parse_error: {e}"),
            Self::IngestError(e) => write!(f, "ingest_error: {e}"),
        }
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

async fn post_ingest_payload(
    client: &reqwest::Client,
    canonical_url: &str,
    source_url: &str,
    http_status: u16,
    content_hash: &str,
    feed_data: Option<IngestFeedData>,
    config: &CrawlConfig,
) -> CrawlOutcome {
    let payload = serde_json::json!({
        "canonical_url": canonical_url,
        "source_url": source_url,
        "crawl_token": config.crawl_token,
        "http_status": http_status,
        "content_hash": content_hash,
        "feed_data": feed_data,
    });

    let ingest_resp = match client
        .post(&config.ingest_url)
        .json(&payload)
        .timeout(config.ingest_timeout)
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => return CrawlOutcome::IngestError(e.to_string()),
    };

    let resp_body = match ingest_resp.text().await {
        Ok(t) => t,
        Err(e) => return CrawlOutcome::IngestError(e.to_string()),
    };

    let parsed: IngestResponse = match serde_json::from_str(&resp_body) {
        Ok(r) => r,
        Err(_) => return CrawlOutcome::IngestError(format!("non-JSON response: {resp_body}")),
    };

    let warnings = parsed.warnings.unwrap_or_default();

    if parsed.accepted {
        CrawlOutcome::Accepted { warnings }
    } else {
        let reason = parsed.reason.unwrap_or_default();
        if reason == "no_change" {
            CrawlOutcome::NoChange
        } else {
            CrawlOutcome::Rejected { reason, warnings }
        }
    }
}

/// Parse cached XML and POST it to `/ingest/feed`. Never panics.
#[allow(
    clippy::too_many_arguments,
    reason = "replay/import paths pass through source URL, canonical URL, status, raw XML, optional hash, fallback GUID, and config"
)]
pub async fn ingest_cached_feed(
    client: &reqwest::Client,
    source_url: &str,
    canonical_url: &str,
    http_status: u16,
    raw_xml: &str,
    content_hash: Option<&str>,
    fallback_guid: Option<&str>,
    config: &CrawlConfig,
) -> CrawlOutcome {
    let content_hash = content_hash.map_or_else(
        || hex::encode(Sha256::digest(raw_xml.as_bytes())),
        ToOwned::to_owned,
    );

    let feed_data = match parse_feed_xml(raw_xml, fallback_guid) {
        Ok(data) => data,
        Err(e) => return CrawlOutcome::ParseError(e),
    };

    post_ingest_payload(
        client,
        canonical_url,
        source_url,
        http_status,
        &content_hash,
        feed_data,
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
    // 1. Fetch
    let resp = match client
        .get(url)
        .header("User-Agent", &config.user_agent)
        .timeout(config.fetch_timeout)
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => return CrawlOutcome::FetchError(e.to_string()),
    };

    let status = resp.status().as_u16();

    let body = match resp.bytes().await {
        Ok(b) => b,
        Err(e) => return CrawlOutcome::FetchError(e.to_string()),
    };

    // 2. SHA-256 hash of raw bytes
    let hash = hex::encode(Sha256::digest(&body));
    let xml = String::from_utf8_lossy(&body);

    ingest_cached_feed(
        client,
        url,
        url,
        status,
        &xml,
        Some(&hash),
        fallback_guid,
        config,
    )
    .await
}
