use std::collections::HashSet;
use std::io::IsTerminal;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::crawl::{CrawlConfig, CrawlOutcome, CrawlReport, crawl_feed_report};
use crate::follow::follow_urls;
use crate::pool::run_pool;
use crate::url_queue::{host_key, interleave_by_host};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

const CRAWL_ATTEMPTS: u32 = 3;

/// A wave's collected follow URLs, indexed by each task's position in the
/// URL list it was given. The index lets [`run_wave`] put the URLs back in
/// submission order once every task has finished, since tasks themselves
/// finish in whatever order the runtime schedules them.
type IndexedFollowUrls = Vec<(usize, Vec<String>)>;

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
) -> CrawlReport {
    let mut attempt = 1;

    loop {
        let lease = host_throttle.acquire(url).await;
        let report = crawl_feed_report(client, url, None, config).await;
        let delay = report
            .outcome
            .retry_delay(attempt)
            .unwrap_or(host_throttle.host_delay);
        host_throttle.release(&lease, delay).await;
        if report.is_retryable() && attempt < CRAWL_ATTEMPTS {
            eprintln!(
                "  crawl: retrying after attempt {attempt}/{CRAWL_ATTEMPTS} for {url}: {}",
                report.outcome
            );
            attempt += 1;
            continue;
        }
        return report;
    }
}

/// Gives the follow URLs (ADR 0049 §2, `stophammer` repository) of `report`.
///
/// Only an `Accepted` or `NoChange` report that carries a parsed feed gives
/// anything; any other outcome, or a report with no parsed feed, gives
/// nothing. This is the only place that reads `report.parsed_feed`, so the
/// report can be dropped right after the call, and a wave never has to hold
/// more than [`run_pool`]'s in-flight reports (bounded by `concurrency`) at
/// one time.
fn report_follow_urls(report: &CrawlReport) -> Vec<String> {
    if !matches!(
        report.outcome,
        CrawlOutcome::Accepted { .. } | CrawlOutcome::NoChange
    ) {
        return Vec::new();
    }
    let Some(feed) = &report.parsed_feed else {
        return Vec::new();
    };
    follow_urls(feed)
}

/// Runs `urls` through `step` with bounded `concurrency`.
///
/// A report whose outcome is retryable adds its URL to `failed_feeds`.
/// Prints one line per URL, in the style [`run_urls`] already uses.
///
/// When `collect_follow_urls` is true, each task reads its own report's
/// follow URLs through [`report_follow_urls`] as soon as the report
/// arrives, keeps only that small `Vec<String>`, and drops the report. The
/// wave never collects a `CrawlReport` itself: a `CrawlReport` carries
/// `raw_xml` and a full `parsed_feed`, and a `refresh` pass over the whole
/// index must not hold every one of those in memory at once. The returned
/// list is in the order `urls` was given, not completion order, because
/// tasks finish concurrently and completion order is not reproducible.
/// When `collect_follow_urls` is false, no task reads a follow URL at all,
/// and the returned list is empty.
async fn run_wave<S, Fut>(
    urls: Vec<String>,
    concurrency: usize,
    failed_feeds: &Arc<std::sync::Mutex<Vec<String>>>,
    step: &Arc<S>,
    collect_follow_urls: bool,
) -> Vec<String>
where
    S: Fn(String) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = CrawlReport> + Send + 'static,
{
    let follow_by_index: Arc<std::sync::Mutex<IndexedFollowUrls>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));

    let tasks: Vec<_> = urls
        .into_iter()
        .enumerate()
        .map(|(index, url)| {
            let step = Arc::clone(step);
            let failed_feeds = Arc::clone(failed_feeds);
            let follow_by_index = Arc::clone(&follow_by_index);
            move || async move {
                let report = step(url.clone()).await;
                if report.is_retryable() {
                    failed_feeds
                        .lock()
                        .expect("failed feed retry list mutex poisoned")
                        .push(url.clone());
                }
                eprintln!("  {}: {url}", report.outcome);

                if collect_follow_urls {
                    let follow = report_follow_urls(&report);
                    if !follow.is_empty() {
                        follow_by_index
                            .lock()
                            .expect("follow url list mutex poisoned")
                            .push((index, follow));
                    }
                }
                // `report` drops here, at the end of this one task, not at
                // the end of the wave.
            }
        })
        .collect();

    run_pool(tasks, concurrency).await;

    let mut follow_by_index = Arc::into_inner(follow_by_index)
        .expect("all wave tasks finished before run_pool returned")
        .into_inner()
        .expect("follow url list mutex poisoned");
    follow_by_index.sort_by_key(|(index, _)| *index);
    follow_by_index
        .into_iter()
        .flat_map(|(_, follow)| follow)
        .collect()
}

/// Excludes each URL already in `wave1_urls`, by exact string, from
/// `collected` — the follow URLs that wave 1 gave, in wave-1 order. Removes
/// duplicates and keeps the first position.
fn wave2_follow_urls(collected: &[String], wave1_urls: &HashSet<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut follow = Vec::new();

    for url in collected {
        if wave1_urls.contains(url) {
            continue;
        }
        if seen.insert(url.clone()) {
            follow.push(url.clone());
        }
    }

    follow
}

/// Runs `wave1_urls`, then crawls a second wave: the feeds that an accepted
/// or unchanged wave-1 feed names through a publisher link (ADR 0049 §2,
/// `stophammer` repository). The walk stops there. A feed that wave 2
/// crawls never starts a wave 3, even when it names a further link, because
/// wave 2 runs with `collect_follow_urls: false`.
///
/// `step` fetches and ingests one URL and gives its crawl report. A test
/// gives a stub `step` and needs no network.
async fn run_waves<S, Fut>(
    wave1_urls: Vec<String>,
    concurrency: usize,
    host_delay_ms: u64,
    failed_feeds: &Arc<std::sync::Mutex<Vec<String>>>,
    step: &Arc<S>,
) where
    S: Fn(String) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = CrawlReport> + Send + 'static,
{
    let wave1_seen: HashSet<String> = wave1_urls.iter().cloned().collect();

    let collected = run_wave(wave1_urls, concurrency, failed_feeds, step, true).await;

    let wave2_urls = wave2_follow_urls(&collected, &wave1_seen);
    let wave2_urls = interleave_by_host(wave2_urls, |url| host_key(url));

    if wave2_urls.is_empty() {
        return;
    }

    eprintln!(
        "crawl: wave 2 is {} URLs, concurrency={concurrency}, host_delay={host_delay_ms}ms",
        wave2_urls.len()
    );

    run_wave(wave2_urls, concurrency, failed_feeds, step, false).await;
}

/// Resolve URLs from args, `FEED_URLS`, or stdin, then hand them to
/// [`run_urls`]. This is the `feed` mode entry point.
pub async fn run(
    urls_arg: Vec<String>,
    concurrency: usize,
    host_delay_ms: u64,
    failed_feeds_output: String,
    force: bool,
) {
    let urls = load_urls(&urls_arg);

    if urls.is_empty() {
        eprintln!("no URLs provided (pass as args, set FEED_URLS, or pipe to stdin)");
        std::process::exit(1);
    }

    run_urls(urls, concurrency, host_delay_ms, failed_feeds_output, force).await;
}

/// Run the batch pipeline over an already-resolved URL list: interleave by
/// host, then fetch and ingest through the concurrency pool.
///
/// A caller that already holds a list calls this directly instead of `run`, so
/// the host interleave and the rest of the pipeline still apply to it.
pub async fn run_urls(
    urls: Vec<String>,
    concurrency: usize,
    host_delay_ms: u64,
    failed_feeds_output: String,
    force: bool,
) {
    let urls = interleave_by_host(urls, |url| host_key(url));

    eprintln!(
        "crawl: {} URLs, concurrency={concurrency}, host_delay={}ms",
        urls.len(),
        host_delay_ms
    );

    let config = Arc::new(CrawlConfig::from_env_with_force(force));
    let client = Arc::new(reqwest::Client::new());
    let failed_feeds = Arc::new(std::sync::Mutex::new(Vec::new()));
    let host_throttle = Arc::new(HostThrottle::new(Duration::from_millis(host_delay_ms)));

    let step: Arc<_> = Arc::new(move |url: String| {
        let client = Arc::clone(&client);
        let config = Arc::clone(&config);
        let host_throttle = Arc::clone(&host_throttle);
        async move { crawl_feed_with_retries(&client, &url, &config, &host_throttle).await }
    });

    run_waves(urls, concurrency, host_delay_ms, &failed_feeds, &step).await;

    let mut failed_urls = failed_feeds
        .lock()
        .expect("failed feed retry list mutex poisoned");
    failed_urls.sort();
    failed_urls.dedup();
    write_failed_feeds(&failed_feeds_output, &failed_urls);

    eprintln!("crawl: done");
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use stophammer_parser::types::{IngestFeedData, IngestRemoteFeedRef};

    use super::{
        CrawlOutcome, CrawlReport, report_follow_urls, run_wave, run_waves, wave2_follow_urls,
    };

    fn remote_item(position: i64, medium: &str, url: &str) -> IngestRemoteFeedRef {
        IngestRemoteFeedRef {
            position,
            medium: Some(medium.to_string()),
            remote_feed_guid: format!("guid-{position}"),
            remote_feed_url: Some(url.to_string()),
            rel: None,
        }
    }

    fn feed(raw_medium: &str, remote_items: Vec<IngestRemoteFeedRef>) -> IngestFeedData {
        IngestFeedData {
            feed_guid: "feed-guid".to_string(),
            title: "Feed".to_string(),
            description: None,
            image_url: None,
            language: None,
            explicit: false,
            itunes_type: None,
            raw_medium: Some(raw_medium.to_string()),
            author_name: None,
            owner_name: None,
            pub_date: None,
            last_build_date: None,
            remote_items,
            persons: Vec::new(),
            entity_ids: Vec::new(),
            links: Vec::new(),
            podcast_namespace: None,
            feed_payment_routes: Vec::new(),
            live_items: Vec::new(),
            tracks: Vec::new(),
        }
    }

    fn accepted_report(feed: IngestFeedData) -> CrawlReport {
        CrawlReport {
            outcome: CrawlOutcome::Accepted {
                warnings: Vec::new(),
            },
            fetch_http_status: Some(200),
            raw_medium: feed.raw_medium.clone(),
            parsed_feed_guid: Some(feed.feed_guid.clone()),
            final_url: None,
            content_sha256: None,
            raw_xml: None,
            parsed_feed: Some(feed),
        }
    }

    fn no_change_report(feed: IngestFeedData) -> CrawlReport {
        CrawlReport {
            outcome: CrawlOutcome::NoChange,
            ..accepted_report(feed)
        }
    }

    fn rejected_report(feed: IngestFeedData) -> CrawlReport {
        CrawlReport {
            outcome: CrawlOutcome::Rejected {
                reason: "[medium_music] not a music feed".to_string(),
                warnings: Vec::new(),
            },
            ..accepted_report(feed)
        }
    }

    #[test]
    fn wave_2_is_the_follow_urls_less_wave_1_with_no_duplicates() {
        let music_feed = feed(
            "music",
            vec![remote_item(
                0,
                "publisher",
                "https://publisher.example/feed.xml",
            )],
        );
        let publisher_feed = feed(
            "publisher",
            vec![
                // Names a feed already in wave 1: must be excluded.
                remote_item(0, "music", "https://a.example/feed.xml"),
                // Names the same URL as the music feed above: deduplicated.
                remote_item(1, "music", "https://publisher.example/feed.xml"),
            ],
        );

        // What `run_wave` would collect: each report's own follow URLs, in
        // submission order.
        let collected: Vec<String> = report_follow_urls(&accepted_report(music_feed))
            .into_iter()
            .chain(report_follow_urls(&no_change_report(publisher_feed)))
            .collect();
        let wave1_urls: HashSet<String> = ["https://a.example/feed.xml".to_string()]
            .into_iter()
            .collect();

        let wave2 = wave2_follow_urls(&collected, &wave1_urls);

        assert_eq!(
            wave2,
            vec!["https://publisher.example/feed.xml".to_string()],
            "wave 2 must hold the follow URLs, less the wave-1 URLs, with no duplicates"
        );
    }

    #[test]
    fn a_rejected_report_gives_no_follow_url() {
        let music_feed = feed(
            "music",
            vec![remote_item(
                0,
                "publisher",
                "https://publisher.example/feed.xml",
            )],
        );

        assert_eq!(
            report_follow_urls(&rejected_report(music_feed)),
            Vec::<String>::new(),
            "a rejected report must not contribute a follow URL"
        );
    }

    #[test]
    fn a_report_with_no_parsed_feed_gives_no_follow_url() {
        let report = CrawlReport {
            outcome: CrawlOutcome::Accepted {
                warnings: Vec::new(),
            },
            fetch_http_status: Some(200),
            raw_medium: None,
            parsed_feed_guid: None,
            final_url: None,
            content_sha256: None,
            raw_xml: None,
            parsed_feed: None,
        };

        assert_eq!(
            report_follow_urls(&report),
            Vec::<String>::new(),
            "a report with no parsed feed must not contribute a follow URL"
        );
    }

    #[tokio::test]
    async fn run_wave_returns_follow_urls_not_reports() {
        let music_feed = feed(
            "music",
            vec![remote_item(
                0,
                "publisher",
                "https://publisher.example/feed.xml",
            )],
        );

        let step: Arc<_> = Arc::new(move |_url: String| {
            let report = accepted_report(music_feed.clone());
            async move { report }
        });

        let failed_feeds = Arc::new(std::sync::Mutex::new(Vec::new()));

        // The binding's type is `Vec<String>`, not `Vec<CrawlReport>`. A
        // regression that made `run_wave` collect reports again would fail
        // to compile here, not only fail at run time: proof that a wave
        // never collects a `CrawlReport`.
        let collected: Vec<String> = run_wave(
            vec!["https://music.example/feed.xml".to_string()],
            1,
            &failed_feeds,
            &step,
            true,
        )
        .await;

        assert_eq!(
            collected,
            vec!["https://publisher.example/feed.xml".to_string()],
            "run_wave must return the collected follow URLs, not the reports"
        );
    }

    #[tokio::test]
    async fn wave_2_is_not_itself_a_source_of_a_further_wave() {
        let music_feed = feed(
            "music",
            vec![remote_item(
                0,
                "publisher",
                "https://publisher.example/feed.xml",
            )],
        );
        // If wave 2 were followed, this feed would name a wave-3 candidate.
        let publisher_feed = feed(
            "publisher",
            vec![remote_item(0, "music", "https://third.example/feed.xml")],
        );

        let calls: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
        let calls_for_step = Arc::clone(&calls);
        let music_feed_for_step = music_feed.clone();
        let publisher_feed_for_step = publisher_feed.clone();
        let step: Arc<_> = Arc::new(move |url: String| {
            calls_for_step
                .lock()
                .expect("call list mutex poisoned")
                .push(url.clone());
            let report = match url.as_str() {
                "https://music.example/feed.xml" => accepted_report(music_feed_for_step.clone()),
                "https://publisher.example/feed.xml" => {
                    accepted_report(publisher_feed_for_step.clone())
                }
                other => panic!("unexpected fetch of {other}"),
            };
            async move { report }
        });

        let failed_feeds = Arc::new(std::sync::Mutex::new(Vec::new()));

        run_waves(
            vec!["https://music.example/feed.xml".to_string()],
            2,
            0,
            &failed_feeds,
            &step,
        )
        .await;

        let called = calls.lock().expect("call list mutex poisoned").clone();
        assert_eq!(
            called,
            vec![
                "https://music.example/feed.xml".to_string(),
                "https://publisher.example/feed.xml".to_string(),
            ],
            "wave 2 must run, but must never itself become a source of a further wave"
        );
    }

    #[tokio::test]
    async fn a_retryable_wave_2_failure_goes_to_the_same_failed_feeds_output_as_wave_1() {
        let music_feed = feed(
            "music",
            vec![remote_item(
                0,
                "publisher",
                "https://publisher.example/feed.xml",
            )],
        );

        let step: Arc<_> = Arc::new(move |url: String| {
            let report = match url.as_str() {
                "https://music.example/feed.xml" => accepted_report(music_feed.clone()),
                "https://publisher.example/feed.xml" => CrawlReport {
                    outcome: CrawlOutcome::FetchError {
                        reason: "http 503 Service Unavailable".to_string(),
                        retryable: true,
                        retry_after_secs: None,
                    },
                    fetch_http_status: Some(503),
                    raw_medium: None,
                    parsed_feed_guid: None,
                    final_url: None,
                    content_sha256: None,
                    raw_xml: None,
                    parsed_feed: None,
                },
                other => panic!("unexpected fetch of {other}"),
            };
            async move { report }
        });

        let failed_feeds = Arc::new(std::sync::Mutex::new(Vec::new()));

        run_waves(
            vec!["https://music.example/feed.xml".to_string()],
            2,
            0,
            &failed_feeds,
            &step,
        )
        .await;

        assert_eq!(
            failed_feeds.lock().expect("mutex poisoned").clone(),
            vec!["https://publisher.example/feed.xml".to_string()],
            "a retryable wave-2 failure must land in the same failed-feeds list as wave 1"
        );
    }
}
