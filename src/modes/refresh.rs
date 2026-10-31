//! `refresh` mode: read the node's own feed list, then run the crawl
//! pipeline over it.
//!
//! ADR 0047 (in the `stophammer` repository) owns this decision. The corpus
//! is `GET /v1/feeds/recent` on the node named by `INGEST_URL`, and nothing
//! else. It never reads `feed_skip.db` or `import_feed_memory`: those
//! record what the crawler attempted, not what the index holds. A corpus
//! built from crawler-local records cannot promise completeness; a corpus
//! read from the index can.
//!
//! A page that fails stops the whole pass. [`run_corrective_pass`] never
//! calls its pipeline callback with a partial corpus.

use std::time::Duration;

use serde::Deserialize;

use crate::modes::batch;

/// Default `INGEST_URL`, matching [`crate::crawl::CrawlConfig`].
const DEFAULT_INGEST_URL: &str = "http://localhost:8008/ingest/feed";
const FEEDS_RECENT_PATH: &str = "/v1/feeds/recent";
/// stophammer ADR 0049 §8: the corrective pass reports the unresolved
/// publisher back-links from this route, at the end of the pass.
const PUBLISHER_LINKS_STATS_PATH: &str = "/v1/publisher-links/stats";
/// The route's page-size cap.
const PAGE_LIMIT: u32 = 100;
/// The feed list filters to the music medium by default. The index also holds
/// publisher and `musicL` feeds, so a corrective pass that takes the default
/// covers one medium and omits the rest without saying so. stophammer ADR 0047
/// owns this.
const ALL_MEDIUMS: &str = "all";

/// One feed of `GET /v1/feeds/recent`. The mode `podcastindex` also reads it
/// (`stophammer` ADR 0081 §4).
#[derive(Debug, Deserialize)]
pub(crate) struct FeedsRecentItem {
    pub(crate) feed_guid: String,
    pub(crate) feed_url: String,
}

/// The pagination block of `GET /v1/feeds/recent`.
#[derive(Debug, Deserialize)]
pub(crate) struct FeedsRecentPagination {
    pub(crate) cursor: Option<String>,
    pub(crate) has_more: bool,
}

/// One page of `GET /v1/feeds/recent`.
#[derive(Debug, Deserialize)]
pub(crate) struct FeedsRecentPage {
    pub(crate) data: Vec<FeedsRecentItem>,
    pub(crate) pagination: FeedsRecentPagination,
}

/// One count from the `data` object of `/v1/publisher-links/stats`.
///
/// A missing count is an error. The route always reports all four counts,
/// so a partial body is never valid.
#[derive(Debug, Deserialize)]
struct PublisherLinkStats {
    listed_links: u64,
    resolved_by_guid: u64,
    resolved_by_feed_url: u64,
    unresolved: u64,
}

/// The envelope of `/v1/publisher-links/stats`. The real body also carries
/// `pagination` and `meta`. Those fields are not read here.
#[derive(Debug, Deserialize)]
struct PublisherLinkStatsResponse {
    data: PublisherLinkStats,
}

/// Read `INGEST_URL`, or give the default.
pub(crate) fn ingest_url_from_env() -> String {
    std::env::var("INGEST_URL").unwrap_or_else(|_| DEFAULT_INGEST_URL.to_string())
}

/// Strip the ingest path from `INGEST_URL` to get the node's origin, e.g.
/// `http://host:8008/ingest/feed` becomes `http://host:8008`. Needs no
/// network; parses the URL and reads its origin.
pub(crate) fn query_origin_from_ingest_url(ingest_url: &str) -> Result<String, String> {
    let url = reqwest::Url::parse(ingest_url)
        .map_err(|err| format!("INGEST_URL {ingest_url:?} is not a valid URL: {err}"))?;
    let origin = url.origin().ascii_serialization();
    if origin == "null" {
        return Err(format!(
            "INGEST_URL {ingest_url:?} has no usable origin (scheme+host+port)"
        ));
    }
    Ok(origin)
}

/// The most `429` answers one page read waits through.
const MAX_429_ATTEMPTS: u32 = 5;
/// The longest wait after one `429`, in seconds.
const MAX_429_WAIT_SECS: u64 = 30;

/// Give the wait before the next try after a `429`, or `None` to stop.
///
/// `attempt` counts from 1 and names the try that got the `429`. The result
/// is `None` when `attempt` is 5 or more. Else it is the `Retry-After`
/// seconds, at most 30, when the header is an integer. With no usable header
/// it is 1 second. A date form of `Retry-After` is not usable.
///
/// `stophammer` ADR 0081 §4 owns this rule. On 2026-10-31, a `--dry-run` pass
/// of the `podcastindex` mode read 106 pages in less than one second. A second
/// pass 2 seconds later got a `429` and stopped. `refresh` uses the same
/// reader.
pub(crate) fn wait_after_429(retry_after: Option<&str>, attempt: u32) -> Option<Duration> {
    if attempt >= MAX_429_ATTEMPTS {
        return None;
    }
    let secs = retry_after
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map_or(1, |secs| secs.min(MAX_429_WAIT_SECS));
    Some(Duration::from_secs(secs))
}

/// Fetch one page of `GET /v1/feeds/recent` at `limit=100` across every
/// medium, honoring the pagination cursor.
///
/// A `429` makes the call wait and send the page again, at most 5 times
/// (see [`wait_after_429`]). Each other status is final.
pub(crate) async fn fetch_feeds_recent_page(
    client: &reqwest::Client,
    origin: &str,
    cursor: Option<&str>,
) -> Result<FeedsRecentPage, String> {
    let mut attempt: u32 = 1;
    loop {
        let mut request = client
            .get(format!("{origin}{FEEDS_RECENT_PATH}"))
            .query(&[("limit", PAGE_LIMIT.to_string())])
            .query(&[("medium", ALL_MEDIUMS)]);
        if let Some(cursor) = cursor {
            request = request.query(&[("cursor", cursor)]);
        }

        let response = request
            .send()
            .await
            .map_err(|err| format!("GET {FEEDS_RECENT_PATH} failed: {err}"))?;

        let status = response.status();
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let retry_after = response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .map(ToString::to_string);
            if let Some(wait) = wait_after_429(retry_after.as_deref(), attempt) {
                eprintln!(
                    "feeds/recent: 429, waiting {} s before try {}",
                    wait.as_secs(),
                    attempt + 1
                );
                tokio::time::sleep(wait).await;
                attempt += 1;
                continue;
            }
        }
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(format!("GET {FEEDS_RECENT_PATH} returned {status}: {body}"));
        }

        return response
            .json::<FeedsRecentPage>()
            .await
            .map_err(|err| format!("GET {FEEDS_RECENT_PATH} returned an unreadable body: {err}"));
    }
}

/// Decode the body of `GET /v1/publisher-links/stats` into the printed
/// line. Needs no network, so a test can check it against a fixed body.
///
/// # Errors
///
/// Returns an error string when the body is not valid JSON, or when a
/// count field is missing.
fn format_link_stats(body: &str) -> Result<String, String> {
    let response: PublisherLinkStatsResponse = serde_json::from_str(body).map_err(|err| {
        format!("{PUBLISHER_LINKS_STATS_PATH} returned an unreadable body: {err}")
    })?;
    let stats = response.data;
    Ok(format!(
        "publisher links: listed={} guid={} feed_url={} unresolved={}",
        stats.listed_links, stats.resolved_by_guid, stats.resolved_by_feed_url, stats.unresolved
    ))
}

/// Fetch `GET /v1/publisher-links/stats` from `origin` and decode it into
/// the printed line.
async fn fetch_publisher_link_stats(
    client: &reqwest::Client,
    origin: &str,
) -> Result<String, String> {
    let response = client
        .get(format!("{origin}{PUBLISHER_LINKS_STATS_PATH}"))
        .send()
        .await
        .map_err(|err| format!("GET {PUBLISHER_LINKS_STATS_PATH} failed: {err}"))?;

    let status = response.status();
    let body = response.text().await.map_err(|err| {
        format!("GET {PUBLISHER_LINKS_STATS_PATH} returned an unreadable body: {err}")
    })?;

    if !status.is_success() {
        return Err(format!(
            "GET {PUBLISHER_LINKS_STATS_PATH} returned {status}: {body}"
        ));
    }

    format_link_stats(&body)
}

/// Read the publisher link counts and print them, or print one warning.
///
/// The corrective pass is already complete when this call runs, so a
/// failed read here never changes the exit status.
async fn report_publisher_link_stats(client: &reqwest::Client, origin: &str) {
    match fetch_publisher_link_stats(client, origin).await {
        Ok(line) => eprintln!("refresh: {line}"),
        Err(err) => eprintln!("refresh: could not read publisher link counts: {err}"),
    }
}

/// Page through `fetch_page` while `has_more` is true, collecting every
/// feed. Stops at the first page that fails and returns the error, never a
/// partial list.
pub(crate) async fn collect_feeds<F, Fut>(
    fetch_page: &mut F,
) -> Result<Vec<FeedsRecentItem>, String>
where
    F: FnMut(Option<String>) -> Fut,
    Fut: std::future::Future<Output = Result<FeedsRecentPage, String>>,
{
    let mut feeds = Vec::new();
    let mut cursor: Option<String> = None;

    loop {
        let page = fetch_page(cursor.take()).await?;
        feeds.extend(page.data);

        if !page.pagination.has_more {
            break;
        }

        let Some(next_cursor) = page.pagination.cursor else {
            return Err(format!(
                "{FEEDS_RECENT_PATH} reported has_more=true but returned no cursor"
            ));
        };
        cursor = Some(next_cursor);
    }

    Ok(feeds)
}

/// Page through `fetch_page` and collect every `feed_url`. It has the
/// behavior of [`collect_feeds`].
async fn collect_feed_urls<F, Fut>(fetch_page: &mut F) -> Result<Vec<String>, String>
where
    F: FnMut(Option<String>) -> Fut,
    Fut: std::future::Future<Output = Result<FeedsRecentPage, String>>,
{
    let feeds = collect_feeds(fetch_page).await?;
    Ok(feeds.into_iter().map(|item| item.feed_url).collect())
}

/// Page the corpus through `fetch_page`, then hand it to `run_pipeline`.
/// A failed page returns `Err` and never calls `run_pipeline`. An empty
/// corpus reports zero and never calls `run_pipeline` either, so a pass
/// over an empty index performs no feed fetch.
async fn run_corrective_pass<F, Fut, P, PFut>(
    mut fetch_page: F,
    run_pipeline: P,
) -> Result<(), String>
where
    F: FnMut(Option<String>) -> Fut,
    Fut: std::future::Future<Output = Result<FeedsRecentPage, String>>,
    P: FnOnce(Vec<String>) -> PFut,
    PFut: std::future::Future<Output = ()>,
{
    let urls = collect_feed_urls(&mut fetch_page).await?;
    eprintln!("refresh: corpus is {} feeds", urls.len());

    if urls.is_empty() {
        eprintln!("refresh: no feeds to refresh");
        return Ok(());
    }

    run_pipeline(urls).await;
    Ok(())
}

/// Read the node's feed list from `GET /v1/feeds/recent`, then run the
/// existing crawl pipeline over it through [`batch::run_urls`]. This is the
/// `refresh` mode entry point.
///
/// `feed_cache` and `revalidate` pass straight through to
/// [`batch::run_urls`], which opens the shared fetch cache (ADR 0050 §1,
/// `stophammer` repository) at that path.
pub async fn run(
    concurrency: usize,
    host_delay_ms: u64,
    failed_feeds_output: String,
    force: bool,
    feed_cache: String,
    revalidate: bool,
) {
    let ingest_url = ingest_url_from_env();
    let origin = match query_origin_from_ingest_url(&ingest_url) {
        Ok(origin) => origin,
        Err(err) => {
            eprintln!("refresh: {err}");
            std::process::exit(1);
        }
    };

    eprintln!("refresh: reading feed list from {origin}{FEEDS_RECENT_PATH}");

    let client = reqwest::Client::new();
    let link_stats_client = client.clone();
    let link_stats_origin = origin.clone();
    let fetch_page = move |cursor: Option<String>| {
        let client = client.clone();
        let origin = origin.clone();
        async move { fetch_feeds_recent_page(&client, &origin, cursor.as_deref()).await }
    };

    let run_pipeline = move |urls: Vec<String>| {
        batch::run_urls(
            urls,
            concurrency,
            host_delay_ms,
            failed_feeds_output,
            force,
            feed_cache,
            revalidate,
        )
    };

    if let Err(err) = run_corrective_pass(fetch_page, run_pipeline).await {
        eprintln!("refresh: failed to read the node's feed list: {err}");
        std::process::exit(1);
    }

    report_publisher_link_stats(&link_stats_client, &link_stats_origin).await;
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use super::{
        FeedsRecentItem, FeedsRecentPage, FeedsRecentPagination, collect_feeds,
        fetch_feeds_recent_page, format_link_stats, query_origin_from_ingest_url,
        run_corrective_pass, wait_after_429,
    };
    use std::time::Duration;

    #[test]
    fn query_origin_strips_the_ingest_path() {
        assert_eq!(
            query_origin_from_ingest_url("http://host:8008/ingest/feed").unwrap(),
            "http://host:8008"
        );
    }

    #[test]
    fn query_origin_works_for_the_default_ingest_url() {
        assert_eq!(
            query_origin_from_ingest_url("http://localhost:8008/ingest/feed").unwrap(),
            "http://localhost:8008"
        );
    }

    #[test]
    fn query_origin_keeps_scheme_and_drops_any_path() {
        assert_eq!(
            query_origin_from_ingest_url("https://api.musicindex.org/ingest/feed").unwrap(),
            "https://api.musicindex.org"
        );
    }

    #[test]
    fn query_origin_rejects_an_unparseable_url() {
        assert!(query_origin_from_ingest_url("not a url").is_err());
    }

    #[test]
    fn format_link_stats_decodes_the_real_response_body() {
        let body = r#"{"data":{"listed_links":709,"resolved_by_guid":693,"resolved_by_feed_url":1,"unresolved":15},"pagination":{"cursor":null,"has_more":false},"meta":{"api_version":"v1","node_pubkey":"fae4b4c8de468abcb430b05efa1d8cfd8b85a3e90d62ca5a5c2c7ed4a56167bd"}}"#;
        assert_eq!(
            format_link_stats(body).unwrap(),
            "publisher links: listed=709 guid=693 feed_url=1 unresolved=15"
        );
    }

    #[test]
    fn format_link_stats_decodes_a_synthetic_response_body() {
        let body = r#"{"data":{"listed_links":10,"resolved_by_guid":4,"resolved_by_feed_url":5,"unresolved":1}}"#;
        assert_eq!(
            format_link_stats(body).unwrap(),
            "publisher links: listed=10 guid=4 feed_url=5 unresolved=1"
        );
    }

    #[test]
    fn format_link_stats_rejects_a_missing_count_field() {
        let body = r#"{"data":{"listed_links":10,"resolved_by_guid":4,"resolved_by_feed_url":5}}"#;
        assert!(
            format_link_stats(body).is_err(),
            "a missing count must be an error, not a zero"
        );
    }

    #[test]
    fn format_link_stats_rejects_a_body_that_is_not_json() {
        assert!(
            format_link_stats("not json").is_err(),
            "a non-JSON body must be an error"
        );
    }

    fn page(urls: &[&str], cursor: Option<&str>, has_more: bool) -> FeedsRecentPage {
        FeedsRecentPage {
            data: urls
                .iter()
                .map(|url| FeedsRecentItem {
                    feed_guid: format!("guid-of-{url}"),
                    feed_url: (*url).to_string(),
                })
                .collect(),
            pagination: FeedsRecentPagination {
                cursor: cursor.map(ToString::to_string),
                has_more,
            },
        }
    }

    #[tokio::test]
    async fn run_corrective_pass_collects_every_page_before_running_the_pipeline() {
        let pages = Arc::new(Mutex::new(vec![
            Ok(page(&["https://a.example/feed.xml"], Some("c1"), true)),
            Ok(page(&["https://b.example/feed.xml"], Some("c2"), true)),
            Ok(page(&["https://c.example/feed.xml"], None, false)),
        ]));
        let fetch_calls = Arc::new(AtomicUsize::new(0));
        let fetch_calls_for_closure = Arc::clone(&fetch_calls);
        let pages_for_closure = Arc::clone(&pages);

        let fetch_page = move |_cursor: Option<String>| {
            fetch_calls_for_closure.fetch_add(1, Ordering::SeqCst);
            let pages = Arc::clone(&pages_for_closure);
            async move {
                let mut pages = pages.lock().expect("pages mutex poisoned");
                assert!(
                    !pages.is_empty(),
                    "fetch_page called past the last stub page"
                );
                pages.remove(0)
            }
        };

        let pipeline_urls: Arc<Mutex<Option<Vec<String>>>> = Arc::new(Mutex::new(None));
        let pipeline_urls_for_closure = Arc::clone(&pipeline_urls);
        let run_pipeline = move |urls: Vec<String>| {
            *pipeline_urls_for_closure.lock().expect("mutex poisoned") = Some(urls);
            async move {}
        };

        let result = run_corrective_pass(fetch_page, run_pipeline).await;

        assert!(result.is_ok(), "a fully paged corpus must not error");
        assert_eq!(
            fetch_calls.load(Ordering::SeqCst),
            3,
            "every page must be fetched"
        );
        assert_eq!(
            pipeline_urls.lock().expect("mutex poisoned").clone(),
            Some(vec![
                "https://a.example/feed.xml".to_string(),
                "https://b.example/feed.xml".to_string(),
                "https://c.example/feed.xml".to_string(),
            ]),
            "the pipeline must receive every URL from every page, in order"
        );
    }

    #[tokio::test]
    async fn a_failed_page_stops_the_pass_and_never_runs_the_pipeline() {
        let pages = Arc::new(Mutex::new(vec![
            Ok(page(&["https://a.example/feed.xml"], Some("c1"), true)),
            Err("http 500 internal server error".to_string()),
        ]));
        let fetch_calls = Arc::new(AtomicUsize::new(0));
        let fetch_calls_for_closure = Arc::clone(&fetch_calls);
        let pages_for_closure = Arc::clone(&pages);

        let fetch_page = move |_cursor: Option<String>| {
            fetch_calls_for_closure.fetch_add(1, Ordering::SeqCst);
            let pages = Arc::clone(&pages_for_closure);
            async move {
                let mut pages = pages.lock().expect("pages mutex poisoned");
                assert!(
                    !pages.is_empty(),
                    "fetch_page called again after the pass should have stopped"
                );
                pages.remove(0)
            }
        };

        let pipeline_called = Arc::new(AtomicUsize::new(0));
        let pipeline_called_for_closure = Arc::clone(&pipeline_called);
        let run_pipeline = move |_urls: Vec<String>| {
            pipeline_called_for_closure.fetch_add(1, Ordering::SeqCst);
            async move {}
        };

        let result = run_corrective_pass(fetch_page, run_pipeline).await;

        assert!(
            result.is_err(),
            "a failed page must stop the pass with an error"
        );
        assert_eq!(
            fetch_calls.load(Ordering::SeqCst),
            2,
            "paging must stop at the failed page, not continue past it"
        );
        assert_eq!(
            pipeline_called.load(Ordering::SeqCst),
            0,
            "run_urls (the pipeline) must never be called with a partial corpus"
        );
    }

    #[tokio::test]
    async fn an_empty_feed_list_reports_zero_and_runs_no_pipeline() {
        let fetch_calls = Arc::new(AtomicUsize::new(0));
        let fetch_calls_for_closure = Arc::clone(&fetch_calls);
        let fetch_page = move |_cursor: Option<String>| {
            fetch_calls_for_closure.fetch_add(1, Ordering::SeqCst);
            async move { Ok(page(&[], None, false)) }
        };

        let pipeline_called = Arc::new(AtomicUsize::new(0));
        let pipeline_called_for_closure = Arc::clone(&pipeline_called);
        let run_pipeline = move |_urls: Vec<String>| {
            pipeline_called_for_closure.fetch_add(1, Ordering::SeqCst);
            async move {}
        };

        let result = run_corrective_pass(fetch_page, run_pipeline).await;

        assert!(result.is_ok(), "an empty corpus is not a failure");
        assert_eq!(
            fetch_calls.load(Ordering::SeqCst),
            1,
            "one page fetch is enough to learn the list is empty"
        );
        assert_eq!(
            pipeline_called.load(Ordering::SeqCst),
            0,
            "an empty corpus must perform no feed fetch"
        );
    }

    #[tokio::test]
    async fn collect_feeds_gives_each_item_with_its_guid_and_url() {
        let pages = Arc::new(Mutex::new(vec![
            page(&["https://a.example/feed.xml"], Some("c1"), true),
            page(&["https://b.example/feed.xml"], None, false),
        ]));
        let mut fetch_page = move |_cursor: Option<String>| {
            let pages = Arc::clone(&pages);
            async move { Ok(pages.lock().expect("pages mutex poisoned").remove(0)) }
        };

        let feeds = collect_feeds(&mut fetch_page)
            .await
            .expect("a fully paged list must not error");

        let pairs: Vec<(&str, &str)> = feeds
            .iter()
            .map(|item| (item.feed_guid.as_str(), item.feed_url.as_str()))
            .collect();
        assert_eq!(
            pairs,
            vec![
                (
                    "guid-of-https://a.example/feed.xml",
                    "https://a.example/feed.xml"
                ),
                (
                    "guid-of-https://b.example/feed.xml",
                    "https://b.example/feed.xml"
                ),
            ],
            "each item must keep its feed_guid and feed_url, in page order"
        );
    }

    #[test]
    fn wait_after_429_follows_the_rule_of_adr_0081() {
        assert_eq!(wait_after_429(Some("3"), 1), Some(Duration::from_secs(3)));
        assert_eq!(
            wait_after_429(Some("120"), 1),
            Some(Duration::from_secs(30)),
            "the wait has a maximum of 30 seconds"
        );
        assert_eq!(
            wait_after_429(Some("Wed, 21 Oct 2026 07:28:00 GMT"), 1),
            Some(Duration::from_secs(1)),
            "a date is not a usable header"
        );
        assert_eq!(wait_after_429(None, 1), Some(Duration::from_secs(1)));
        assert_eq!(
            wait_after_429(Some("1"), 5),
            None,
            "the fifth attempt is the last"
        );
    }

    /// Serve one canned answer for each entry of `answers`, in order, then
    /// stop. Returns the origin and the count of requests seen.
    async fn stub_node(answers: Vec<String>) -> (String, Arc<AtomicUsize>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind stub");
        let origin = format!("http://{}", listener.local_addr().expect("stub address"));
        let seen = Arc::new(AtomicUsize::new(0));
        let seen_for_task = Arc::clone(&seen);
        tokio::spawn(async move {
            for answer in answers {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let mut buf = Vec::new();
                let mut chunk = [0_u8; 1024];
                while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                    match stream.read(&mut chunk).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    }
                }
                seen_for_task.fetch_add(1, Ordering::SeqCst);
                let _ = stream.write_all(answer.as_bytes()).await;
                let _ = stream.shutdown().await;
            }
        });
        (origin, seen)
    }

    fn answer(status: &str, extra_header: &str, body: &str) -> String {
        format!(
            "HTTP/1.1 {status}\r\n{extra_header}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    const ONE_FEED_BODY: &str = r#"{"data":[{"feed_guid":"g1","feed_url":"https://a.example/feed.xml"}],"pagination":{"cursor":null,"has_more":false}}"#;

    #[tokio::test]
    async fn a_429_then_a_200_gives_the_page_after_one_wait() {
        let (origin, seen) = stub_node(vec![
            answer("429 Too Many Requests", "Retry-After: 1\r\n", "{}"),
            answer("200 OK", "", ONE_FEED_BODY),
        ])
        .await;

        let page = fetch_feeds_recent_page(&reqwest::Client::new(), &origin, None)
            .await
            .expect("the second answer is a page");

        assert_eq!(page.data.len(), 1, "the page holds one feed");
        assert_eq!(page.data[0].feed_guid, "g1");
        assert_eq!(seen.load(Ordering::SeqCst), 2, "the stub saw two requests");
    }

    #[tokio::test]
    async fn six_429_answers_give_an_error_that_holds_429() {
        let answers = (0..6)
            .map(|_| answer("429 Too Many Requests", "Retry-After: 0\r\n", "{}"))
            .collect();
        let (origin, seen) = stub_node(answers).await;

        let err = fetch_feeds_recent_page(&reqwest::Client::new(), &origin, None)
            .await
            .expect_err("no page after repeated 429 answers");

        assert!(err.contains("429"), "error must name the status: {err}");
        assert_eq!(
            seen.load(Ordering::SeqCst),
            5,
            "the reader stops after 5 tries"
        );
    }

    #[tokio::test]
    async fn a_500_gives_an_error_at_once() {
        let (origin, seen) = stub_node(vec![answer("500 Internal Server Error", "", "{}")]).await;

        let err = fetch_feeds_recent_page(&reqwest::Client::new(), &origin, None)
            .await
            .expect_err("a 500 is final");

        assert!(err.contains("500"), "error must name the status: {err}");
        assert_eq!(seen.load(Ordering::SeqCst), 1, "no retry after a 500");
    }
}
