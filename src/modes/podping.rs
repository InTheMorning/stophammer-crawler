use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use tokio::sync::{Mutex, Semaphore};

use crate::crawl::{CrawlConfig, crawl_feed};
use crate::dedup::Dedup;

// ── Podping wire types ────────────────────────────────────────────────────────

#[derive(serde::Deserialize)]
struct PodpingBlock {
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
}

impl PodpingMessage {
    fn all_urls(&self) -> Vec<&str> {
        self.iris
            .as_deref()
            .or(self.urls.as_deref())
            .unwrap_or_default()
            .iter()
            .map(String::as_str)
            .collect()
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

pub async fn run(concurrency: usize) {
    let ws_url = std::env::var("PODPING_WS_URL")
        .unwrap_or_else(|_| "wss://api.livewire.io/ws/podping".to_string());

    let config = Arc::new(CrawlConfig::from_env());
    let client = Arc::new(reqwest::Client::new());
    let sem = Arc::new(Semaphore::new(concurrency));
    let dedup = Arc::new(Mutex::new(Dedup::new()));

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

    loop {
        eprintln!("podping: connecting to {ws_url}");

        let ws_result = tokio_tungstenite::connect_async(&ws_url).await;

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

            for entry in &block.p {
                if !should_accept(&entry.p) {
                    continue;
                }

                for url in entry.p.all_urls() {
                    let should_crawl = dedup.lock().await.should_process(url);
                    if !should_crawl {
                        continue;
                    }

                    let url = url.to_string();
                    let client = Arc::clone(&client);
                    let config = Arc::clone(&config);
                    let sem = Arc::clone(&sem);

                    tokio::spawn(async move {
                        let _permit = sem.acquire().await.expect("semaphore closed");
                        let outcome = crawl_feed(&client, &url, None, &config).await;
                        eprintln!("  {outcome}: {url}");
                    });
                }
            }
        }

        eprintln!("podping: disconnected, retrying in {backoff:?}");
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(max_backoff);
    }
}
