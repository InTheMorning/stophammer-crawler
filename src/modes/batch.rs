use std::collections::HashSet;
use std::io::IsTerminal;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::crawl::{CrawlConfig, CrawlOutcome, CrawlReport, FeedCache, crawl_feed_report};
use crate::feed_cache::FeedCacheDb;
use crate::follow::{FollowLevel, follow_urls_at_level};
use crate::pool::run_pool;
use crate::url_queue::{FollowQueueLimit, MAX_FOLLOW_QUEUE_URLS, host_key, interleave_by_host};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

const CRAWL_ATTEMPTS: u32 = 3;

/// Builds a client for a feed fetch (`stophammer` ADR 0052 §2). `import`
/// uses this client too.
///
/// Redirects are off. The crawler follows a redirect chain itself, through
/// [`crate::crawl::crawl_feed_report`], so it can put a conditional header
/// on every hop, and record each hop.
///
/// The DNS resolver accepts only a public answer (`stophammer` ADR 0054
/// §1). It pins the client to the address it resolved.
pub(crate) fn build_feed_fetch_client() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .dns_resolver(Arc::new(crate::fetch_guard::PublicOnlyResolver))
        .build()
        .expect("failed to build feed fetch HTTP client")
}

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

pub(crate) struct HostThrottle {
    slots: Mutex<std::collections::HashMap<String, Arc<HostSlot>>>,
    host_delay: Duration,
}

struct HostSlot {
    semaphore: Arc<Semaphore>,
    next_allowed_at: Mutex<Instant>,
}

pub(crate) struct HostLease {
    slot: Option<Arc<HostSlot>>,
    _permit: Option<OwnedSemaphorePermit>,
}

impl HostThrottle {
    pub(crate) fn new(host_delay: Duration) -> Self {
        Self {
            slots: Mutex::new(std::collections::HashMap::new()),
            host_delay,
        }
    }

    pub(crate) async fn acquire(&self, url: &str) -> HostLease {
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

    pub(crate) async fn release(&self, lease: &HostLease, delay: Duration) {
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
    cache: Option<&FeedCache>,
) -> CrawlReport {
    let mut attempt = 1;

    loop {
        let lease = host_throttle.acquire(url).await;
        let report = crawl_feed_report(client, url, None, config, cache).await;
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

/// Gives the follow URLs (ADR 0049 §2, `stophammer` repository) of `report`,
/// filtered by the level `report`'s feed was fetched at.
///
/// Only an `Accepted` or `NoChange` report that carries a parsed feed gives
/// anything; any other outcome, or a report with no parsed feed, gives
/// nothing. This is the only place that reads `report.parsed_feed`, so the
/// report can be dropped right after the call, and a wave never has to hold
/// more than [`run_pool`]'s in-flight reports (bounded by `concurrency`) at
/// one time.
pub(crate) fn report_follow_urls(report: &CrawlReport, level: FollowLevel) -> Vec<String> {
    if !matches!(
        report.outcome,
        CrawlOutcome::Accepted { .. } | CrawlOutcome::NoChange
    ) {
        return Vec::new();
    }
    let Some(feed) = &report.parsed_feed else {
        return Vec::new();
    };
    // ADR 0052 §2 (`stophammer` repository): `follow_urls` needs the
    // fetched URL, to skip a `new_feed_url` that names it.
    let fetched_url = report.final_url.as_deref().unwrap_or_default();
    follow_urls_at_level(feed, level, fetched_url)
}

/// The fetch counts of one batch pass, added up over every wave (ADR 0050
/// §6, `stophammer` repository).
///
/// `crawl_feed_with_retries` retries inside one call and gives back one
/// final report for a URL. A wave task sees only that report, so
/// [`FetchCounts::note`] reads one `fetch_http_status` for each URL in each
/// wave. A retry never adds a second count.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct FetchCounts {
    ok: u32,
    not_modified: u32,
    rate_limited: u32,
    other: u32,
}

impl FetchCounts {
    /// Add one fetch to its bucket, by the final HTTP status of the fetch.
    ///
    /// `Some(200)` counts as `ok`. `Some(304)` counts as `not_modified`.
    /// `Some(429)` counts as `rate_limited`. Any other status, and `None`,
    /// count as `other`.
    fn note(&mut self, status: Option<u16>) {
        match status {
            Some(200) => self.ok += 1,
            Some(304) => self.not_modified += 1,
            Some(429) => self.rate_limited += 1,
            _ => self.other += 1,
        }
    }
}

impl std::fmt::Display for FetchCounts {
    /// Print the line ADR 0050 §6 asks for.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "fetch: ok={} not_modified={} rate_limited={} other={}",
            self.ok, self.not_modified, self.rate_limited, self.other
        )
    }
}

/// Runs `urls` through `step` with bounded `concurrency`.
///
/// A report whose outcome is retryable adds its URL to `failed_feeds`. Each
/// report also notes its `fetch_http_status` into `fetch_counts`, under the
/// same rule that keeps only the follow URLs: the count is read from the
/// report before the task drops it. Prints one line per URL, in the style
/// [`run_urls`] already uses.
///
/// When `follow_level` is `Some`, each task reads its own report's follow
/// URLs through [`report_follow_urls`], at that level, as soon as the
/// report arrives. It keeps only that small `Vec<String>` and drops the
/// report. The wave never collects a `CrawlReport` itself: a `CrawlReport`
/// carries `raw_xml` and a full `parsed_feed`, and a `refresh` pass over the
/// whole index must not hold every one of those in memory at once. The
/// returned list is in the order `urls` was given, not completion order,
/// because tasks finish concurrently and completion order is not
/// reproducible. When `follow_level` is `None`, no task reads a follow URL
/// at all, and the returned list is empty.
async fn run_wave<S, Fut>(
    urls: Vec<String>,
    concurrency: usize,
    failed_feeds: &Arc<std::sync::Mutex<Vec<String>>>,
    fetch_counts: &Arc<std::sync::Mutex<FetchCounts>>,
    step: &Arc<S>,
    follow_level: Option<FollowLevel>,
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
            let fetch_counts = Arc::clone(fetch_counts);
            let follow_by_index = Arc::clone(&follow_by_index);
            move || async move {
                let report = step(url.clone()).await;
                if report.is_retryable() {
                    failed_feeds
                        .lock()
                        .expect("failed feed retry list mutex poisoned")
                        .push(url.clone());
                }
                fetch_counts
                    .lock()
                    .expect("fetch counts mutex poisoned")
                    .note(report.fetch_http_status);
                eprintln!("  {}: {url}", report.outcome);

                if let Some(level) = follow_level {
                    let follow = report_follow_urls(&report, level);
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

/// Excludes each URL already in `seen`, by exact string, from `collected` —
/// the follow URLs a wave gave, in that wave's order. Removes duplicates and
/// keeps the first position.
fn exclude_seen_urls(collected: &[String], seen: &HashSet<String>) -> Vec<String> {
    let mut deduped = HashSet::new();
    let mut follow = Vec::new();

    for url in collected {
        if seen.contains(url) {
            continue;
        }
        if deduped.insert(url.clone()) {
            follow.push(url.clone());
        }
    }

    follow
}

/// Filters `urls` through `queue`, in order, keeping only the URLs the queue
/// still has room for (`stophammer` ADR 0054 §3). A URL the queue refuses is
/// dropped here; `queue` itself counts the drop, for the pass to log at its
/// end.
fn admit_follow_urls(urls: Vec<String>, queue: &FollowQueueLimit) -> Vec<String> {
    urls.into_iter().filter(|_| queue.try_admit()).collect()
}

/// Runs `wave1_urls` first. Then it crawls the publisher feeds that an
/// accepted or unchanged wave-1 feed names (wave 2). Then it crawls the
/// album feeds that an accepted or unchanged wave-2 publisher feed lists
/// (wave 3). ADR 0049 §2, in the `stophammer` repository, owns this
/// sequence.
///
/// A wave-3 feed can name a further link. Wave 3 never starts a wave 4 for
/// it, because wave 3 runs with `follow_level: None`. No task in wave 3
/// reads a follow URL.
///
/// `fetch_counts` is the same shared counter across all three waves, so its
/// value after this call is the total over the whole pass. This is the
/// smaller change: `run_wave` already takes `failed_feeds` the same way.
///
/// `follow_queue` caps the follow URLs wave 2 and wave 3 add, in total, for
/// this one pass (`stophammer` ADR 0054 §3). A URL past the cap is dropped
/// before it is ever fetched.
///
/// `step` fetches and ingests one URL and gives its crawl report. A test
/// gives a stub `step` and needs no network.
async fn run_waves<S, Fut>(
    wave1_urls: Vec<String>,
    concurrency: usize,
    host_delay_ms: u64,
    failed_feeds: &Arc<std::sync::Mutex<Vec<String>>>,
    fetch_counts: &Arc<std::sync::Mutex<FetchCounts>>,
    follow_queue: &FollowQueueLimit,
    step: &Arc<S>,
) where
    S: Fn(String) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = CrawlReport> + Send + 'static,
{
    let wave1_seen: HashSet<String> = wave1_urls.iter().cloned().collect();

    let collected1 = run_wave(
        wave1_urls,
        concurrency,
        failed_feeds,
        fetch_counts,
        step,
        Some(FollowLevel::Input),
    )
    .await;

    let wave2_urls = exclude_seen_urls(&collected1, &wave1_seen);
    let wave2_urls = admit_follow_urls(wave2_urls, follow_queue);
    let wave2_urls = interleave_by_host(wave2_urls, |url| host_key(url));

    if wave2_urls.is_empty() {
        return;
    }

    eprintln!(
        "crawl: wave 2 is {} URLs, concurrency={concurrency}, host_delay={host_delay_ms}ms",
        wave2_urls.len()
    );

    let wave1_and_2_seen: HashSet<String> = wave1_seen
        .iter()
        .cloned()
        .chain(wave2_urls.iter().cloned())
        .collect();

    let collected2 = run_wave(
        wave2_urls,
        concurrency,
        failed_feeds,
        fetch_counts,
        step,
        Some(FollowLevel::Publisher),
    )
    .await;

    let wave3_urls = exclude_seen_urls(&collected2, &wave1_and_2_seen);
    let wave3_urls = admit_follow_urls(wave3_urls, follow_queue);
    let wave3_urls = interleave_by_host(wave3_urls, |url| host_key(url));

    if wave3_urls.is_empty() {
        return;
    }

    eprintln!(
        "crawl: wave 3 is {} URLs, concurrency={concurrency}, host_delay={host_delay_ms}ms",
        wave3_urls.len()
    );

    run_wave(
        wave3_urls,
        concurrency,
        failed_feeds,
        fetch_counts,
        step,
        None,
    )
    .await;
}

/// Resolve URLs from args, `FEED_URLS`, or stdin, then hand them to
/// [`run_urls`]. This is the `feed` mode entry point.
pub async fn run(
    urls_arg: Vec<String>,
    concurrency: usize,
    host_delay_ms: u64,
    failed_feeds_output: String,
    force: bool,
    feed_cache: String,
    revalidate: bool,
) {
    let urls = load_urls(&urls_arg);

    if urls.is_empty() {
        eprintln!("no URLs provided (pass as args, set FEED_URLS, or pipe to stdin)");
        std::process::exit(1);
    }

    run_urls(
        urls,
        concurrency,
        host_delay_ms,
        failed_feeds_output,
        force,
        feed_cache,
        revalidate,
    )
    .await;
}

/// Run the batch pipeline over an already-resolved URL list: interleave by
/// host, then fetch and ingest through the concurrency pool.
///
/// A caller that already holds a list calls this directly instead of `run`, so
/// the host interleave and the rest of the pipeline still apply to it.
///
/// `feed_cache` names the shared fetch cache (ADR 0050 §1, `stophammer`
/// repository). This is the route the cache takes to reach the `step`
/// closure: opened once here, then cloned into the closure the same way
/// `client`, `config`, and `host_throttle` already are.
pub async fn run_urls(
    urls: Vec<String>,
    concurrency: usize,
    host_delay_ms: u64,
    failed_feeds_output: String,
    force: bool,
    feed_cache: String,
    revalidate: bool,
) {
    let urls = interleave_by_host(urls, |url| host_key(url));

    eprintln!(
        "crawl: {} URLs, concurrency={concurrency}, host_delay={}ms",
        urls.len(),
        host_delay_ms
    );

    let config = Arc::new(CrawlConfig::from_env_with_force(force).with_revalidate(revalidate));
    let client = Arc::new(build_feed_fetch_client());
    let failed_feeds = Arc::new(std::sync::Mutex::new(Vec::new()));
    let fetch_counts = Arc::new(std::sync::Mutex::new(FetchCounts::default()));
    let follow_queue = FollowQueueLimit::new(MAX_FOLLOW_QUEUE_URLS);
    let host_throttle = Arc::new(HostThrottle::new(Duration::from_millis(host_delay_ms)));
    let cache: FeedCache = Arc::new(std::sync::Mutex::new(FeedCacheDb::open(&feed_cache)));

    let step: Arc<_> = Arc::new(move |url: String| {
        let client = Arc::clone(&client);
        let config = Arc::clone(&config);
        let host_throttle = Arc::clone(&host_throttle);
        let cache = Arc::clone(&cache);
        async move {
            crawl_feed_with_retries(&client, &url, &config, &host_throttle, Some(&cache)).await
        }
    });

    run_waves(
        urls,
        concurrency,
        host_delay_ms,
        &failed_feeds,
        &fetch_counts,
        &follow_queue,
        &step,
    )
    .await;

    // ADR 0050 §6 (`stophammer` repository): one line, printed once, after
    // every wave has run. Always printed, even when every count is zero.
    let counts = *fetch_counts.lock().expect("fetch counts mutex poisoned");
    eprintln!("{counts}");

    // ADR 0054 §3 (`stophammer` repository): one line, printed once, at the
    // end of the pass. Always printed, even when nothing was dropped.
    eprintln!(
        "crawl: follow queue: dropped {} URL(s), limit {MAX_FOLLOW_QUEUE_URLS}",
        follow_queue.dropped()
    );

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
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use std::time::Duration;

    use stophammer_parser::types::{IngestFeedData, IngestRemoteFeedRef};
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{TcpListener, TcpStream};

    use super::{
        CrawlConfig, CrawlOutcome, CrawlReport, FeedCache, FeedCacheDb, FetchCounts, FollowLevel,
        HostThrottle, crawl_feed_with_retries, exclude_seen_urls, report_follow_urls, run_wave,
        run_waves,
    };
    use crate::url_queue::{FollowQueueLimit, MAX_FOLLOW_QUEUE_URLS};

    fn remote_item(position: i64, medium: &str, url: &str) -> IngestRemoteFeedRef {
        IngestRemoteFeedRef {
            position,
            medium: Some(medium.to_string()),
            remote_feed_guid: format!("guid-{position}"),
            remote_feed_url: Some(url.to_string()),
            rel: None,
            item_guid: None,
            item_title: None,
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
            new_feed_url: None,
            locked: None,
            locked_owner: None,
            remote_items,
            persons: Vec::new(),
            entity_ids: Vec::new(),
            links: Vec::new(),
            blocks: Vec::new(),
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
            redirects: Vec::new(),
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
    fn fetch_counts_note_200_counts_as_ok() {
        let mut counts = FetchCounts::default();
        counts.note(Some(200));
        assert_eq!(
            counts,
            FetchCounts {
                ok: 1,
                ..FetchCounts::default()
            },
            "status 200 must count as ok"
        );
    }

    #[test]
    fn fetch_counts_note_304_counts_as_not_modified() {
        let mut counts = FetchCounts::default();
        counts.note(Some(304));
        assert_eq!(
            counts,
            FetchCounts {
                not_modified: 1,
                ..FetchCounts::default()
            },
            "status 304 must count as not_modified"
        );
    }

    #[test]
    fn fetch_counts_note_429_counts_as_rate_limited() {
        let mut counts = FetchCounts::default();
        counts.note(Some(429));
        assert_eq!(
            counts,
            FetchCounts {
                rate_limited: 1,
                ..FetchCounts::default()
            },
            "status 429 must count as rate_limited"
        );
    }

    #[test]
    fn fetch_counts_note_an_unlisted_status_and_no_status_count_as_other() {
        let mut counts = FetchCounts::default();
        counts.note(Some(500));
        counts.note(None);
        assert_eq!(
            counts,
            FetchCounts {
                other: 2,
                ..FetchCounts::default()
            },
            "a status outside 200, 304 and 429, and a fetch with no status, must count as other"
        );
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
        // submission order. Wave 1 runs at `FollowLevel::Input`.
        let collected: Vec<String> =
            report_follow_urls(&accepted_report(music_feed), FollowLevel::Input)
                .into_iter()
                .chain(report_follow_urls(
                    &no_change_report(publisher_feed),
                    FollowLevel::Input,
                ))
                .collect();
        let wave1_urls: HashSet<String> = ["https://a.example/feed.xml".to_string()]
            .into_iter()
            .collect();

        let wave2 = exclude_seen_urls(&collected, &wave1_urls);

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
            report_follow_urls(&rejected_report(music_feed), FollowLevel::Input),
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
            redirects: Vec::new(),
        };

        assert_eq!(
            report_follow_urls(&report, FollowLevel::Input),
            Vec::<String>::new(),
            "a report with no parsed feed must not contribute a follow URL"
        );
    }

    #[test]
    fn a_music_feed_report_gives_no_follow_url_at_publisher_level() {
        // The parsed feed does not parse as a publisher feed. At
        // `FollowLevel::Publisher`, it must give nothing (ADR 0049 §2
        // task 010b), even though the feed carries a link of its own.
        let music_feed = feed(
            "music",
            vec![remote_item(
                0,
                "publisher",
                "https://publisher.example/feed.xml",
            )],
        );

        assert_eq!(
            report_follow_urls(&accepted_report(music_feed), FollowLevel::Publisher),
            Vec::<String>::new(),
            "a music feed in wave 2 must give no link"
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
        let fetch_counts = Arc::new(std::sync::Mutex::new(FetchCounts::default()));

        // The binding's type is `Vec<String>`, not `Vec<CrawlReport>`. A
        // regression that made `run_wave` collect reports again would fail
        // to compile here, not only fail at run time: proof that a wave
        // never collects a `CrawlReport`.
        let collected: Vec<String> = run_wave(
            vec!["https://music.example/feed.xml".to_string()],
            1,
            &failed_feeds,
            &fetch_counts,
            &step,
            Some(FollowLevel::Input),
        )
        .await;

        assert_eq!(
            collected,
            vec!["https://publisher.example/feed.xml".to_string()],
            "run_wave must return the collected follow URLs, not the reports"
        );
    }

    /// Corrects `wave_2_is_not_itself_a_source_of_a_further_wave`, which
    /// asserted that wave 2 never becomes a source of a further wave. Task
    /// 010b (ADR 0049 §2) adds wave 3: a wave-2 report that parses as a
    /// publisher feed does give its `medium="music"` links.
    ///
    /// Old expectation: only `music.example` and `publisher.example` are
    /// fetched. The wave-2 feed's own further link is never followed.
    /// New expectation: wave 3 runs and fetches each album the wave-2
    /// publisher feed lists (`a1.example`, `a2.example`). One of those
    /// albums names a further link (`q.example`), but wave 3 runs with
    /// `follow_level: None`, so that link is never fetched.
    #[tokio::test]
    async fn wave_3_is_the_links_of_wave_2_and_no_wave_4_runs() {
        // An album in wave 1 names publisher P.
        let album = feed(
            "music",
            vec![remote_item(
                0,
                "publisher",
                "https://publisher.example/feed.xml",
            )],
        );
        // P, in wave 2, lists albums A1 and A2. It also re-lists the
        // wave-1 URL, to prove a URL of wave 1 does not reappear in wave 3.
        let publisher = feed(
            "publisher",
            vec![
                remote_item(0, "music", "https://a1.example/feed.xml"),
                remote_item(1, "music", "https://a2.example/feed.xml"),
                remote_item(2, "music", "https://music.example/feed.xml"),
            ],
        );
        // A1, in wave 3, names publisher Q. Wave 3 must give no link, so Q
        // must never be fetched.
        let a1 = feed(
            "music",
            vec![remote_item(0, "publisher", "https://q.example/feed.xml")],
        );
        let a2 = feed("music", Vec::new());

        let calls: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
        let calls_for_step = Arc::clone(&calls);
        let step: Arc<_> = Arc::new(move |url: String| {
            calls_for_step
                .lock()
                .expect("call list mutex poisoned")
                .push(url.clone());
            let report = match url.as_str() {
                "https://music.example/feed.xml" => accepted_report(album.clone()),
                "https://publisher.example/feed.xml" => accepted_report(publisher.clone()),
                "https://a1.example/feed.xml" => accepted_report(a1.clone()),
                "https://a2.example/feed.xml" => accepted_report(a2.clone()),
                other => panic!("unexpected fetch of {other}"),
            };
            async move { report }
        });

        let failed_feeds = Arc::new(std::sync::Mutex::new(Vec::new()));
        let fetch_counts = Arc::new(std::sync::Mutex::new(FetchCounts::default()));
        let follow_queue = FollowQueueLimit::new(MAX_FOLLOW_QUEUE_URLS);

        run_waves(
            vec!["https://music.example/feed.xml".to_string()],
            2,
            0,
            &failed_feeds,
            &fetch_counts,
            &follow_queue,
            &step,
        )
        .await;

        let mut called = calls.lock().expect("call list mutex poisoned").clone();
        called.sort();
        assert_eq!(
            called,
            vec![
                "https://a1.example/feed.xml".to_string(),
                "https://a2.example/feed.xml".to_string(),
                "https://music.example/feed.xml".to_string(),
                "https://publisher.example/feed.xml".to_string(),
            ],
            "wave 3 must fetch each album wave 2's publisher feed lists, exactly once, \
             and no wave 4 must run"
        );
    }

    /// Proves `stophammer` ADR 0054 §3: a follow queue past its limit drops
    /// the next URL, and never fetches it. A small limit stands in for the
    /// production limit of 50,000, since the test does not need to reach
    /// that count to prove the cap works.
    #[tokio::test]
    async fn a_small_follow_queue_limit_drops_the_url_past_it() {
        // Wave 1's one input feed names three publisher links. With a
        // follow queue limit of 2, wave 2 must run for only the first two.
        let music_feed = feed(
            "music",
            vec![
                remote_item(0, "publisher", "https://p1.example/feed.xml"),
                remote_item(1, "publisher", "https://p2.example/feed.xml"),
                remote_item(2, "publisher", "https://p3.example/feed.xml"),
            ],
        );

        let calls: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
        let calls_for_step = Arc::clone(&calls);
        let step: Arc<_> = Arc::new(move |url: String| {
            calls_for_step
                .lock()
                .expect("call list mutex poisoned")
                .push(url.clone());
            let report = match url.as_str() {
                "https://music.example/feed.xml" => accepted_report(music_feed.clone()),
                "https://p1.example/feed.xml" | "https://p2.example/feed.xml" => {
                    accepted_report(feed("publisher", Vec::new()))
                }
                other => panic!(
                    "unexpected fetch of {other}: the follow queue limit of 2 must drop \
                     the third publisher link before it is ever fetched"
                ),
            };
            async move { report }
        });

        let failed_feeds = Arc::new(std::sync::Mutex::new(Vec::new()));
        let fetch_counts = Arc::new(std::sync::Mutex::new(FetchCounts::default()));
        let follow_queue = FollowQueueLimit::new(2);

        run_waves(
            vec!["https://music.example/feed.xml".to_string()],
            2,
            0,
            &failed_feeds,
            &fetch_counts,
            &follow_queue,
            &step,
        )
        .await;

        let mut called = calls.lock().expect("call list mutex poisoned").clone();
        called.sort();
        assert_eq!(
            called,
            vec![
                "https://music.example/feed.xml".to_string(),
                "https://p1.example/feed.xml".to_string(),
                "https://p2.example/feed.xml".to_string(),
            ],
            "a follow queue limit of 2 must fetch only the first two publisher links"
        );
        assert_eq!(
            follow_queue.dropped(),
            1,
            "the pass must count the one follow URL the limit dropped"
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
                    redirects: Vec::new(),
                },
                other => panic!("unexpected fetch of {other}"),
            };
            async move { report }
        });

        let failed_feeds = Arc::new(std::sync::Mutex::new(Vec::new()));
        let fetch_counts = Arc::new(std::sync::Mutex::new(FetchCounts::default()));
        let follow_queue = FollowQueueLimit::new(MAX_FOLLOW_QUEUE_URLS);

        run_waves(
            vec!["https://music.example/feed.xml".to_string()],
            2,
            0,
            &failed_feeds,
            &fetch_counts,
            &follow_queue,
            &step,
        )
        .await;

        assert_eq!(
            failed_feeds.lock().expect("mutex poisoned").clone(),
            vec!["https://publisher.example/feed.xml".to_string()],
            "a retryable wave-2 failure must land in the same failed-feeds list as wave 1"
        );
    }

    /// Proves ADR 0050 §6 (`stophammer` repository): the fetch counts add up
    /// over every wave, through the same `fetch_counts` argument `run_wave`
    /// takes alongside `failed_feeds`.
    ///
    /// Wave 1 holds two feeds, each naming its own publisher, so wave 2 runs
    /// for both. Wave 1 gives the `200` and the `304`. Wave 2 gives the
    /// `429` and the fetch with no status at all. Neither wave-2 report
    /// parses as an accepted or unchanged feed, so wave 3 never starts, and
    /// this proves the total over exactly two waves.
    #[tokio::test]
    async fn fetch_counts_add_up_over_both_waves() {
        let feed_a = feed(
            "music",
            vec![remote_item(0, "publisher", "https://pub1.example/feed.xml")],
        );
        let feed_b = feed(
            "music",
            vec![remote_item(0, "publisher", "https://pub2.example/feed.xml")],
        );

        let step: Arc<_> = Arc::new(move |url: String| {
            let report = match url.as_str() {
                "https://a.example/feed.xml" => accepted_report(feed_a.clone()),
                "https://b.example/feed.xml" => CrawlReport {
                    fetch_http_status: Some(304),
                    ..no_change_report(feed_b.clone())
                },
                "https://pub1.example/feed.xml" => CrawlReport {
                    outcome: CrawlOutcome::FetchError {
                        reason: "http 429 Too Many Requests".to_string(),
                        retryable: false,
                        retry_after_secs: None,
                    },
                    fetch_http_status: Some(429),
                    raw_medium: None,
                    parsed_feed_guid: None,
                    final_url: None,
                    content_sha256: None,
                    raw_xml: None,
                    parsed_feed: None,
                    redirects: Vec::new(),
                },
                "https://pub2.example/feed.xml" => CrawlReport {
                    outcome: CrawlOutcome::FetchError {
                        reason: "connection reset".to_string(),
                        retryable: false,
                        retry_after_secs: None,
                    },
                    fetch_http_status: None,
                    raw_medium: None,
                    parsed_feed_guid: None,
                    final_url: None,
                    content_sha256: None,
                    raw_xml: None,
                    parsed_feed: None,
                    redirects: Vec::new(),
                },
                other => panic!("unexpected fetch of {other}"),
            };
            async move { report }
        });

        let failed_feeds = Arc::new(std::sync::Mutex::new(Vec::new()));
        let fetch_counts = Arc::new(std::sync::Mutex::new(FetchCounts::default()));
        let follow_queue = FollowQueueLimit::new(MAX_FOLLOW_QUEUE_URLS);

        run_waves(
            vec![
                "https://a.example/feed.xml".to_string(),
                "https://b.example/feed.xml".to_string(),
            ],
            2,
            0,
            &failed_feeds,
            &fetch_counts,
            &follow_queue,
            &step,
        )
        .await;

        assert_eq!(
            *fetch_counts.lock().expect("fetch counts mutex poisoned"),
            FetchCounts {
                ok: 1,
                not_modified: 1,
                rate_limited: 1,
                other: 1,
            },
            "the fetch counts must add up over both waves: one 200, one 304, \
             one 429 and one fetch with no status"
        );
    }

    // ---- stub-server test: the cache reaches `crawl_feed_with_retries` ----
    //
    // `run_wave` and `run_waves` take a stub `step` closure, so a test at
    // that level never sees the cache `run_urls` opens. This test proves
    // the fact one level down, at `crawl_feed_with_retries` itself. The
    // stub helper shape is copied from the tests at the end of
    // `src/crawl.rs`. No test here sends a request to an external host.

    /// One HTTP/1.1 request, captured from a stub connection. This test
    /// reads only the method; `read_stub_request` still reads the headers,
    /// to find the body's `Content-Length`.
    struct StubRequest {
        method: String,
    }

    /// Read one HTTP/1.1 request head and body from `stream`, and discard
    /// the body. This test reads only the method and the headers.
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

        StubRequest { method }
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
        Arc::new(std::sync::Mutex::new(FeedCacheDb::open(&path)))
    }

    /// Proves ADR 0050 §1 (`stophammer` repository): `crawl_feed_with_retries`
    /// passes its `cache` argument through to `crawl_feed_report`, so a
    /// `200` fetch writes a cache row. `run_urls` opens the cache and
    /// clones it into the same closure that calls this function, so this
    /// proves the cache reaches the step `run_urls` builds.
    #[tokio::test]
    async fn crawl_feed_with_retries_passes_the_cache_through_to_crawl_feed_report() {
        let body = b"<rss><channel><title>Feed</title></channel></rss>";
        let (feed_addr, feed_handle) = spawn_stub(stub_response(
            "HTTP/1.1 200 OK",
            &[("etag", "\"v1\"")],
            body,
        ))
        .await;
        let feed_url = format!("http://{feed_addr}/feed.xml");

        let (ingest_addr, ingest_handle) = spawn_stub(stub_response(
            "HTTP/1.1 200 OK",
            &[("content-type", "application/json")],
            br#"{"accepted":true}"#,
        ))
        .await;

        let client = reqwest::Client::new();
        let mut config =
            CrawlConfig::dry_run("stophammer-crawler-test/1.0", Duration::from_secs(5));
        config.ingest_url = format!("http://{ingest_addr}/ingest/feed");
        // This test fetches its own stub server on a loopback address (ADR
        // 0054 §1, `stophammer` repository). Production code must never
        // set this.
        config.allow_private_targets = true;
        let cache = test_cache();
        let host_throttle = HostThrottle::new(Duration::from_millis(0));

        let _report =
            crawl_feed_with_retries(&client, &feed_url, &config, &host_throttle, Some(&cache))
                .await;

        let feed_request = feed_handle.await.expect("feed stub task");
        let ingest_request = ingest_handle.await.expect("ingest stub task");
        assert_eq!(feed_request.method, "GET");
        assert_eq!(ingest_request.method, "POST");

        let cached = cache
            .lock()
            .expect("cache lock")
            .get(&feed_url)
            .expect("crawl_feed_with_retries must pass its cache to crawl_feed_report");
        assert_eq!(cached.etag.as_deref(), Some("\"v1\""));
    }
}
