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

use serde::Deserialize;

use crate::modes::batch;

/// Default `INGEST_URL`, matching [`crate::crawl::CrawlConfig`].
const DEFAULT_INGEST_URL: &str = "http://localhost:8008/ingest/feed";
const FEEDS_RECENT_PATH: &str = "/v1/feeds/recent";
/// The route's page-size cap.
const PAGE_LIMIT: u32 = 100;
/// The feed list filters to the music medium by default. The index also holds
/// publisher and `musicL` feeds, so a corrective pass that takes the default
/// covers one medium and omits the rest without saying so. stophammer ADR 0047
/// owns this.
const ALL_MEDIUMS: &str = "all";

#[derive(Debug, Deserialize)]
struct FeedsRecentItem {
    feed_url: String,
}

#[derive(Debug, Deserialize)]
struct FeedsRecentPagination {
    cursor: Option<String>,
    has_more: bool,
}

#[derive(Debug, Deserialize)]
struct FeedsRecentPage {
    data: Vec<FeedsRecentItem>,
    pagination: FeedsRecentPagination,
}

fn ingest_url_from_env() -> String {
    std::env::var("INGEST_URL").unwrap_or_else(|_| DEFAULT_INGEST_URL.to_string())
}

/// Strip the ingest path from `INGEST_URL` to get the node's origin, e.g.
/// `http://host:8008/ingest/feed` becomes `http://host:8008`. Needs no
/// network; parses the URL and reads its origin.
fn query_origin_from_ingest_url(ingest_url: &str) -> Result<String, String> {
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

/// Fetch one page of `GET /v1/feeds/recent` at `limit=100` across every
/// medium, honoring the pagination cursor.
async fn fetch_feeds_recent_page(
    client: &reqwest::Client,
    origin: &str,
    cursor: Option<&str>,
) -> Result<FeedsRecentPage, String> {
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
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(format!("GET {FEEDS_RECENT_PATH} returned {status}: {body}"));
    }

    response
        .json::<FeedsRecentPage>()
        .await
        .map_err(|err| format!("GET {FEEDS_RECENT_PATH} returned an unreadable body: {err}"))
}

/// Page through `fetch_page` while `has_more` is true, collecting every
/// `feed_url`. Stops at the first page that fails and returns the error,
/// never a partial list.
async fn collect_feed_urls<F, Fut>(fetch_page: &mut F) -> Result<Vec<String>, String>
where
    F: FnMut(Option<String>) -> Fut,
    Fut: std::future::Future<Output = Result<FeedsRecentPage, String>>,
{
    let mut urls = Vec::new();
    let mut cursor: Option<String> = None;

    loop {
        let page = fetch_page(cursor.take()).await?;
        urls.extend(page.data.into_iter().map(|item| item.feed_url));

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

    Ok(urls)
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
pub async fn run(concurrency: usize, host_delay_ms: u64, failed_feeds_output: String, force: bool) {
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
    let fetch_page = move |cursor: Option<String>| {
        let client = client.clone();
        let origin = origin.clone();
        async move { fetch_feeds_recent_page(&client, &origin, cursor.as_deref()).await }
    };

    let run_pipeline = move |urls: Vec<String>| {
        batch::run_urls(urls, concurrency, host_delay_ms, failed_feeds_output, force)
    };

    if let Err(err) = run_corrective_pass(fetch_page, run_pipeline).await {
        eprintln!("refresh: failed to read the node's feed list: {err}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use super::{
        FeedsRecentItem, FeedsRecentPage, FeedsRecentPagination, query_origin_from_ingest_url,
        run_corrective_pass,
    };

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

    fn page(urls: &[&str], cursor: Option<&str>, has_more: bool) -> FeedsRecentPage {
        FeedsRecentPage {
            data: urls
                .iter()
                .map(|url| FeedsRecentItem {
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
}
