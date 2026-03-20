use std::io::IsTerminal;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::crawl::{CrawlConfig, crawl_feed};
use crate::pool::run_pool;
use crate::url_queue::{host_key, interleave_by_host};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

const CRAWL_ATTEMPTS: u32 = 3;

/// Load URLs from: positional args → `FEED_URLS` env → stdin (one per line).
fn load_urls(args: &[String]) -> Vec<String> {
    // 1. Positional args: if first arg is a file, read it; else treat all as URLs
    if !args.is_empty() {
        let first = &args[0];
        if args.len() == 1 && std::path::Path::new(first).is_file() {
            let content = std::fs::read_to_string(first).expect("failed to read URL file");
            return content
                .lines()
                .map(str::trim)
                .filter(|l| !l.is_empty() && !l.starts_with('#'))
                .map(String::from)
                .collect();
        }
        return args.to_vec();
    }

    // 2. FEED_URLS env
    if let Ok(env_urls) = std::env::var("FEED_URLS") {
        let urls: Vec<String> = env_urls
            .lines()
            .flat_map(|l| l.split(','))
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .map(String::from)
            .collect();
        if !urls.is_empty() {
            return urls;
        }
    }

    // 3. stdin (only if not a tty)
    if !std::io::stdin().is_terminal() {
        use std::io::BufRead;
        return std::io::stdin()
            .lock()
            .lines()
            .map_while(Result::ok)
            .map(|l| l.trim().to_string())
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .collect();
    }

    Vec::new()
}

fn write_failed_feeds(path: &str, urls: &[String]) {
    if urls.is_empty() {
        return;
    }

    let path = std::path::Path::new(path);
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent).expect("failed to create failed feed directory");
    }

    let content = format!("{}\n", urls.join("\n"));
    std::fs::write(path, content).expect("failed to write failed feed file");
}

struct HostThrottle {
    slots: Mutex<std::collections::HashMap<String, Arc<HostSlot>>>,
    host_delay: Duration,
}

struct HostSlot {
    semaphore: Arc<Semaphore>,
    next_allowed_at: Mutex<Instant>,
}

struct HostLease {
    slot: Option<Arc<HostSlot>>,
    _permit: Option<OwnedSemaphorePermit>,
}

impl HostThrottle {
    fn new(host_delay: Duration) -> Self {
        Self {
            slots: Mutex::new(std::collections::HashMap::new()),
            host_delay,
        }
    }

    async fn acquire(&self, url: &str) -> HostLease {
        let Some(host) = host_key(url) else {
            return HostLease {
                slot: None,
                _permit: None,
            };
        };

        let slot = {
            let mut slots = self.slots.lock().await;
            Arc::clone(slots.entry(host).or_insert_with(|| {
                Arc::new(HostSlot {
                    semaphore: Arc::new(Semaphore::new(1)),
                    next_allowed_at: Mutex::new(Instant::now()),
                })
            }))
        };

        let permit = Arc::clone(&slot.semaphore)
            .acquire_owned()
            .await
            .expect("host semaphore closed");

        let wait = {
            let next_allowed_at = slot.next_allowed_at.lock().await;
            (*next_allowed_at).saturating_duration_since(Instant::now())
        };
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }

        HostLease {
            slot: Some(slot),
            _permit: Some(permit),
        }
    }

    async fn release(&self, lease: &HostLease, delay: Duration) {
        let Some(slot) = &lease.slot else {
            return;
        };

        let mut next_allowed_at = slot.next_allowed_at.lock().await;
        *next_allowed_at = Instant::now() + delay.max(self.host_delay);
    }
}

async fn crawl_feed_with_retries(
    client: &reqwest::Client,
    url: &str,
    config: &CrawlConfig,
    host_throttle: &HostThrottle,
) -> crate::crawl::CrawlOutcome {
    let mut attempt = 1;

    loop {
        let lease = host_throttle.acquire(url).await;
        let outcome = crawl_feed(client, url, None, config).await;
        let delay = outcome
            .retry_delay(attempt)
            .unwrap_or(host_throttle.host_delay);
        host_throttle.release(&lease, delay).await;
        if outcome.is_retryable() && attempt < CRAWL_ATTEMPTS {
            eprintln!(
                "  crawl: retrying after attempt {attempt}/{CRAWL_ATTEMPTS} for {url}: {outcome}"
            );
            attempt += 1;
            continue;
        }
        return outcome;
    }
}

pub async fn run(
    urls_arg: Vec<String>,
    concurrency: usize,
    host_delay_ms: u64,
    failed_feeds_output: String,
) {
    let urls = interleave_by_host(load_urls(&urls_arg), |url| host_key(url));

    if urls.is_empty() {
        eprintln!("no URLs provided (pass as args, set FEED_URLS, or pipe to stdin)");
        std::process::exit(1);
    }

    eprintln!(
        "crawl: {} URLs, concurrency={concurrency}, host_delay={}ms",
        urls.len(),
        host_delay_ms
    );

    let config = Arc::new(CrawlConfig::from_env());
    let client = Arc::new(reqwest::Client::new());
    let failed_feeds = Arc::new(std::sync::Mutex::new(Vec::new()));
    let host_throttle = Arc::new(HostThrottle::new(Duration::from_millis(host_delay_ms)));

    let tasks: Vec<_> = urls
        .into_iter()
        .map(|url| {
            let client = Arc::clone(&client);
            let config = Arc::clone(&config);
            let failed_feeds = Arc::clone(&failed_feeds);
            let host_throttle = Arc::clone(&host_throttle);
            move || async move {
                let outcome = crawl_feed_with_retries(&client, &url, &config, &host_throttle).await;
                if outcome.is_retryable() {
                    failed_feeds
                        .lock()
                        .expect("failed feed retry list mutex poisoned")
                        .push(url.clone());
                }
                eprintln!("  {outcome}: {url}");
            }
        })
        .collect();

    run_pool(tasks, concurrency).await;
    let mut failed_urls = failed_feeds
        .lock()
        .expect("failed feed retry list mutex poisoned");
    failed_urls.sort();
    failed_urls.dedup();
    write_failed_feeds(&failed_feeds_output, &failed_urls);

    eprintln!("crawl: done");
}
