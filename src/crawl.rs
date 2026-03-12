use std::time::Duration;

use sha2::{Digest, Sha256};
use stophammer_parser::profile;

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

    // 3. Parse (direct library call — no subprocess)
    let xml = String::from_utf8_lossy(&body);
    let parser = match fallback_guid {
        Some(guid) => profile::stophammer_with_fallback(guid.to_string()),
        None => profile::stophammer(),
    };
    let feed_data = match parser.parse(&xml) {
        Ok(data) => Some(data),
        Err(e) => {
            if e.is_xml() {
                return CrawlOutcome::ParseError(e.to_string());
            }
            // Missing fields (no title, no guid) → still POST with feed_data: null
            // so the server can record the crawl attempt
            None
        }
    };

    // 4. POST to /ingest/feed
    let payload = serde_json::json!({
        "canonical_url": url,
        "source_url": url,
        "crawl_token": config.crawl_token,
        "http_status": status,
        "content_hash": hash,
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
