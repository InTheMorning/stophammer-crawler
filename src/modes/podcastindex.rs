//! The `podcastindex` mode (`stophammer` ADR 0081).
//!
//! This file holds the comparison of ADR 0081 §4, the wait of §5, the limits
//! of §6 and the pass. One pass reads the music list and the node list, and
//! sends the due candidates to the `feed` pipeline. It stores the result of
//! each fetch, and prints the report of §8.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, Mutex};

use rusqlite::{Connection, OptionalExtension, params, params_from_iter};

use crate::crawl::{CrawlReport, DEFAULT_USER_AGENT};
use crate::feed_skip::FeedSkipDb;
use crate::modes::batch::{self, OutcomeSink, UrlPass};
use crate::modes::import::{open_state_connection, set_import_progress_value};
use crate::modes::refresh::{
    FeedsRecentItem, collect_feeds, fetch_feeds_recent_page, ingest_url_from_env,
    query_origin_from_ingest_url,
};
use crate::podcastindex_api::{Credentials, MusicListRow, fetch_music_list};

/// One feed that the node holds (ADR 0081 §4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NodeFeed {
    pub(crate) feed_guid: String,
    pub(crate) feed_url: String,
}

impl From<FeedsRecentItem> for NodeFeed {
    fn from(item: FeedsRecentItem) -> Self {
        Self {
            feed_guid: item.feed_guid,
            feed_url: item.feed_url,
        }
    }
}

/// Make a URL comparable (ADR 0081 §4).
///
/// The steps, in this sequence: trim the text, remove `http://` or
/// `https://` at the start, remove `www.` at the start, remove each `/` at
/// the end, and lowercase the text before the first `/`. The text after the
/// first `/` keeps its case.
pub(crate) fn comparable_url(url: &str) -> String {
    let text = url.trim();
    let text = text
        .strip_prefix("http://")
        .or_else(|| text.strip_prefix("https://"))
        .unwrap_or(text);
    let text = text.strip_prefix("www.").unwrap_or(text);
    let text = text.trim_end_matches('/');
    match text.split_once('/') {
        Some((host, path)) => format!("{}/{path}", host.to_lowercase()),
        None => text.to_lowercase(),
    }
}

/// The result of the comparison (ADR 0081 §4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Classification {
    /// The number of rows that the node holds.
    pub(crate) held: usize,
    /// The rows that the node does not hold, in the sequence of the list.
    pub(crate) candidates: Vec<MusicListRow>,
}

/// Classify each row as held or as a candidate (ADR 0081 §4).
///
/// A row is held when its `podcast_guid` matches a `feed_guid` of the node
/// with no regard to letter case. A row is also held when its
/// `comparable_url` matches that of a node feed. A row with no
/// `podcast_guid` is compared by URL only.
pub(crate) fn classify(rows: Vec<MusicListRow>, node: &[NodeFeed]) -> Classification {
    let guids: HashSet<String> = node
        .iter()
        .map(|feed| feed.feed_guid.to_lowercase())
        .collect();
    let urls: HashSet<String> = node
        .iter()
        .map(|feed| comparable_url(&feed.feed_url))
        .collect();

    let mut held = 0;
    let mut candidates = Vec::new();
    for row in rows {
        let by_guid = row
            .podcast_guid
            .as_ref()
            .is_some_and(|guid| guids.contains(&guid.to_lowercase()));
        if by_guid || urls.contains(&comparable_url(&row.url)) {
            held += 1;
        } else {
            candidates.push(row);
        }
    }
    Classification { held, candidates }
}

/// How long a failed candidate waits, in seconds (ADR 0081 §5).
pub(crate) const RETRY_WAIT_SECS: i64 = 7 * 86_400;

/// The most IDs that one `last_attempts` statement reads.
const ATTEMPT_CHUNK: usize = 500;

/// The last try of one candidate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Attempt {
    /// The `CrawlOutcome::label` of the try.
    pub(crate) outcome: String,
    /// The time of the try, in seconds since the Unix epoch.
    pub(crate) last_attempted_at: i64,
}

/// The table `music_list_attempts` of `import_state.db` (ADR 0081 §5).
///
/// The store opens the file with `open_state_connection`, so the schema of
/// `import` stays in place. The store writes only through `record`. The caller
/// calls `record` only for a URL that the pipeline fetched. No function
/// writes a list row before a fetch (ADR 0081 §7).
#[derive(Debug)]
pub(crate) struct AttemptStore {
    conn: Connection,
}

impl AttemptStore {
    /// Open the file at `path`, and make the table when it is absent.
    ///
    /// # Panics
    ///
    /// Panics when the file cannot open, or when the table cannot be made.
    pub(crate) fn open(path: &str) -> Self {
        let conn = open_state_connection(path);
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS music_list_attempts (
                podcastindex_id   INTEGER PRIMARY KEY,
                feed_url          TEXT NOT NULL,
                outcome           TEXT NOT NULL,
                outcome_reason    TEXT,
                last_attempted_at INTEGER NOT NULL,
                attempt_count     INTEGER NOT NULL DEFAULT 1
            );
            CREATE TABLE IF NOT EXISTS import_gap_passes (
                pass_at INTEGER PRIMARY KEY,
                counted INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS import_gap_rows (
                pass_at         INTEGER NOT NULL,
                podcastindex_id INTEGER NOT NULL,
                feed_url        TEXT NOT NULL,
                PRIMARY KEY (pass_at, podcastindex_id)
            );",
        )
        .expect("failed to create the podcastindex tables");
        Self { conn }
    }

    /// Write the result of one fetch. A second write of the same ID replaces
    /// the URL, the outcome, the reason and the time, and adds 1 to the count.
    ///
    /// # Errors
    ///
    /// Returns the `SQLite` error when the write fails.
    pub(crate) fn record(
        &self,
        id: i64,
        url: &str,
        outcome: &str,
        reason: Option<&str>,
        now: i64,
    ) -> rusqlite::Result<()> {
        self.conn.execute(
            "INSERT INTO music_list_attempts
                 (podcastindex_id, feed_url, outcome, outcome_reason, last_attempted_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(podcastindex_id) DO UPDATE SET
                 feed_url = excluded.feed_url,
                 outcome = excluded.outcome,
                 outcome_reason = excluded.outcome_reason,
                 last_attempted_at = excluded.last_attempted_at,
                 attempt_count = attempt_count + 1",
            params![id, url, outcome, reason, now],
        )?;
        Ok(())
    }

    /// Read the last try of each ID that has a row.
    ///
    /// # Errors
    ///
    /// Returns the `SQLite` error when a read fails.
    pub(crate) fn last_attempts(&self, ids: &[i64]) -> rusqlite::Result<HashMap<i64, Attempt>> {
        let mut found = HashMap::new();
        for chunk in ids.chunks(ATTEMPT_CHUNK) {
            let marks = vec!["?"; chunk.len()].join(",");
            let sql = format!(
                "SELECT podcastindex_id, outcome, last_attempted_at
                 FROM music_list_attempts WHERE podcastindex_id IN ({marks})"
            );
            let mut stmt = self.conn.prepare(&sql)?;
            let rows = stmt.query_map(params_from_iter(chunk.iter()), |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    Attempt {
                        outcome: row.get(1)?,
                        last_attempted_at: row.get(2)?,
                    },
                ))
            })?;
            for row in rows {
                let (id, attempt) = row?;
                found.insert(id, attempt);
            }
        }
        Ok(found)
    }

    /// Read the start time of the previous pass, in seconds since the Unix
    /// epoch. The value is `None` before the first pass.
    ///
    /// # Errors
    ///
    /// Returns the `SQLite` error when the read fails.
    pub(crate) fn pass_started_at(&self) -> rusqlite::Result<Option<i64>> {
        let value: Option<String> = self
            .conn
            .query_row(
                "SELECT value FROM import_progress WHERE key = ?1",
                [PASS_STARTED_AT_KEY],
                |row| row.get(0),
            )
            .optional()?;
        Ok(value.and_then(|text| text.parse().ok()))
    }

    /// Store the start time of this pass, in seconds since the Unix epoch.
    ///
    /// # Errors
    ///
    /// Returns the `SQLite` error when the write fails.
    pub(crate) fn set_pass_started_at(&self, now: i64) -> rusqlite::Result<()> {
        set_import_progress_value(&self.conn, PASS_STARTED_AT_KEY, &now.to_string())?;
        Ok(())
    }

    /// Read the feeds that `import` accepted at or after `since`.
    ///
    /// The rows come from `import_feed_memory`. The result has the
    /// `podcastindex_id` and the `feed_url` of each row, in the sequence of
    /// the ID.
    ///
    /// # Errors
    ///
    /// Returns the `SQLite` error when the read fails.
    pub(crate) fn accepted_imports_since(
        &self,
        since: i64,
    ) -> rusqlite::Result<Vec<(i64, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT podcastindex_id, feed_url FROM import_feed_memory
             WHERE fetch_outcome = 'accepted' AND last_attempted_at >= ?1
             ORDER BY podcastindex_id",
        )?;
        stmt.query_map([since], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect()
    }

    /// Write the result of one measurement of the `import` gap.
    ///
    /// The rows are results of `import` fetches. They are not music list rows,
    /// and no list row gets a stored try before a fetch (ADR 0081 §7). One
    /// transaction writes one `import_gap_passes` row and one `import_gap_rows`
    /// row for each pair. A second call with the same `pass_at` fails, and the
    /// transaction leaves no row.
    ///
    /// # Errors
    ///
    /// Returns the `SQLite` error when a write fails.
    pub(crate) fn record_import_gap(
        &self,
        pass_at: i64,
        rows: &[(i64, String)],
    ) -> rusqlite::Result<()> {
        let tx = self.conn.unchecked_transaction()?;
        tx.execute(
            "INSERT INTO import_gap_passes (pass_at, counted) VALUES (?1, ?2)",
            params![pass_at, rows.len()],
        )?;
        for (id, url) in rows {
            tx.execute(
                "INSERT INTO import_gap_rows (pass_at, podcastindex_id, feed_url)
                 VALUES (?1, ?2, ?3)",
                params![pass_at, id, url],
            )?;
        }
        tx.commit()
    }
}

/// The most lines of counted rows that one pass prints.
const GAP_LINES: usize = 50;

/// Give each accepted `import` feed whose ID is not in the music list.
///
/// The result keeps the sequence of `accepted`.
pub(crate) fn count_import_gap(
    accepted: &[(i64, String)],
    list_ids: &HashSet<i64>,
) -> Vec<(i64, String)> {
    accepted
        .iter()
        .filter(|(id, _)| !list_ids.contains(id))
        .cloned()
        .collect()
}

/// The key of `import_progress` that holds the start time of the last pass.
const PASS_STARTED_AT_KEY: &str = "podcastindex_pass_started_at";

/// Tell if a candidate still waits (ADR 0081 §5).
///
/// The answer is `true` only when the last try has an outcome other than
/// `accepted` or `no_change`, and it is less than `RETRY_WAIT_SECS` old.
pub(crate) fn waits(attempt: Option<&Attempt>, now: i64) -> bool {
    attempt.is_some_and(|a| {
        a.outcome != "accepted"
            && a.outcome != "no_change"
            && now - a.last_attempted_at < RETRY_WAIT_SECS
    })
}

/// Split the candidates into those that do not wait, in their sequence, and
/// the number of those that wait.
pub(crate) fn split_waiting(
    candidates: Vec<MusicListRow>,
    attempts: &HashMap<i64, Attempt>,
    now: i64,
) -> (Vec<MusicListRow>, usize) {
    let total = candidates.len();
    let ready: Vec<MusicListRow> = candidates
        .into_iter()
        .filter(|c| !waits(attempts.get(&c.id), now))
        .collect();
    let waiting = total - ready.len();
    (ready, waiting)
}

/// The fewest list rows that a normal list has (ADR 0081 §6).
pub(crate) const MIN_LIST_ROWS: usize = 1_000;
/// The most list rows that a normal list has (ADR 0081 §6).
pub(crate) const MAX_LIST_ROWS: usize = 20_000;
/// The most due candidates that one pass sends (ADR 0081 §6).
pub(crate) const MAX_DUE_CANDIDATES: usize = 2_000;

/// Check the size of the list and the number of due candidates (ADR 0081 §6).
///
/// # Errors
///
/// Returns an error text when a number is outside its limit. The text names
/// the condition and the numbers.
pub(crate) fn check_limits(rows: usize, due: usize) -> Result<(), String> {
    if rows < MIN_LIST_ROWS {
        return Err(format!(
            "the list has {rows} rows, fewer than the minimum of {MIN_LIST_ROWS}"
        ));
    }
    if rows > MAX_LIST_ROWS {
        return Err(format!(
            "the list has {rows} rows, more than the maximum of {MAX_LIST_ROWS}"
        ));
    }
    if due > MAX_DUE_CANDIDATES {
        return Err(format!(
            "{due} candidates are due, more than the maximum of {MAX_DUE_CANDIDATES}"
        ));
    }
    Ok(())
}

/// The numbers of one pass (ADR 0081 §8).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PassReport {
    /// The rows of the music list.
    pub(crate) rows: usize,
    /// The rows that the node holds.
    pub(crate) held: usize,
    /// The rows that the node does not hold, with those that wait.
    pub(crate) candidates: usize,
    /// The candidates that wait after a failed try.
    pub(crate) waiting: usize,
    /// The candidates that went to the pipeline.
    pub(crate) sent: usize,
    /// The number of candidate fetches for each outcome label.
    pub(crate) outcomes: BTreeMap<String, usize>,
    /// The accepted `import` feeds that the list does not hold, or `None`
    /// when there is no previous pass.
    pub(crate) import_gap: Option<usize>,
}

impl fmt::Display for PassReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "podcastindex: rows={} held={} candidates={} waiting={} sent={}",
            self.rows, self.held, self.candidates, self.waiting, self.sent
        )?;
        for label in ["accepted", "rejected"] {
            write!(f, " {label}={}", self.outcomes.get(label).unwrap_or(&0))?;
        }
        for (label, count) in &self.outcomes {
            if label != "accepted" && label != "rejected" {
                write!(f, " {label}={count}")?;
            }
        }
        match self.import_gap {
            Some(count) => write!(f, " import_gap={count}"),
            None => write!(f, " import_gap=none"),
        }
    }
}

/// Read the time as seconds since the Unix epoch.
fn unix_now() -> i64 {
    chrono::Utc::now().timestamp()
}

/// Make the sink that stores the outcome of each candidate fetch.
///
/// The sink looks up the URL in `ids`. It never uses `report.final_url`. A
/// URL that is not a candidate, such as a follow URL, is not stored and not
/// counted. The sink holds each lock only for one write.
fn candidate_sink(
    ids: HashMap<String, i64>,
    store: Arc<Mutex<AttemptStore>>,
    outcomes: Arc<Mutex<BTreeMap<String, usize>>>,
    now: i64,
) -> OutcomeSink {
    Arc::new(move |url: &str, report: &CrawlReport| {
        let Some(&id) = ids.get(url) else {
            return;
        };
        let label = report.outcome.label();
        *outcomes
            .lock()
            .expect("outcome count mutex poisoned")
            .entry(label.to_string())
            .or_insert(0) += 1;
        let result = store.lock().expect("attempt store mutex poisoned").record(
            id,
            url,
            label,
            report.outcome.reason(),
            now,
        );
        if let Err(err) = result {
            eprintln!("podcastindex: could not store the outcome of {url}: {err}");
        }
    })
}

/// Do one pass with three reads and sends given as closures.
///
/// `read_list` gives the music list. `read_node` gives the feeds of the node.
/// `pipeline` gets the URLs of the due candidates and the sink. It must call
/// the sink for each fetch. A failed read or a limit error gives `Err` before
/// `pipeline` runs. With `dry_run` set, `pipeline` does not run.
///
/// The pipeline gets `Vec<String>` and no API field (ADR 0081 §3).
async fn run_pass<L, LFut, N, NFut, P, PFut>(
    store: &Arc<Mutex<AttemptStore>>,
    now: i64,
    dry_run: bool,
    read_list: L,
    read_node: N,
    pipeline: P,
) -> Result<PassReport, String>
where
    L: FnOnce() -> LFut,
    LFut: std::future::Future<Output = Result<Vec<MusicListRow>, String>>,
    N: FnOnce() -> NFut,
    NFut: std::future::Future<Output = Result<Vec<NodeFeed>, String>>,
    P: FnOnce(Vec<String>, OutcomeSink) -> PFut,
    PFut: std::future::Future<Output = ()>,
{
    let list = read_list().await?;
    let rows = list.len();
    let list_ids: HashSet<i64> = list.iter().map(|row| row.id).collect();
    let node = read_node().await?;

    let Classification { held, candidates } = classify(list, &node);
    let total_candidates = candidates.len();
    let ids: Vec<i64> = candidates.iter().map(|c| c.id).collect();
    let attempts = store
        .lock()
        .expect("attempt store mutex poisoned")
        .last_attempts(&ids)
        .map_err(|err| format!("could not read the earlier tries: {err}"))?;
    let (due, waiting) = split_waiting(candidates, &attempts, now);
    check_limits(rows, due.len())?;
    let import_gap = measure_import_gap(store, now, &list_ids)?;

    let mut report = PassReport {
        rows,
        held,
        candidates: total_candidates,
        waiting,
        sent: 0,
        outcomes: BTreeMap::new(),
        import_gap,
    };
    if dry_run || due.is_empty() {
        return Ok(report);
    }

    report.sent = due.len();
    let urls: Vec<String> = due.iter().map(|c| c.url.clone()).collect();
    let id_by_url: HashMap<String, i64> = due.into_iter().map(|c| (c.url, c.id)).collect();
    let counts = Arc::new(Mutex::new(BTreeMap::new()));
    let sink = candidate_sink(id_by_url, Arc::clone(store), Arc::clone(&counts), now);
    // ADR 0081 §3: the pipeline gets URLs only.
    pipeline(urls, sink).await;
    report.outcomes = counts.lock().expect("outcome count mutex poisoned").clone();
    Ok(report)
}

/// Count the `import` gap and store the start time of this pass.
///
/// The old start time is `since`. With no old start time, the function
/// prints a line and writes no gap row. A `dry_run` pass also measures,
/// because the measurement sends nothing to the node (ADR 0081 §10).
fn measure_import_gap(
    store: &Arc<Mutex<AttemptStore>>,
    now: i64,
    list_ids: &HashSet<i64>,
) -> Result<Option<usize>, String> {
    let guard = store.lock().expect("attempt store mutex poisoned");
    let previous = guard
        .pass_started_at()
        .map_err(|err| format!("could not read the pass start: {err}"))?;
    let gap = if let Some(since) = previous {
        let accepted = guard
            .accepted_imports_since(since)
            .map_err(|err| format!("could not read the accepted imports: {err}"))?;
        let counted = count_import_gap(&accepted, list_ids);
        eprintln!(
            "podcastindex: import gap since {since}: {} accepted import feeds not in the music list",
            counted.len()
        );
        for (id, url) in counted.iter().take(GAP_LINES) {
            eprintln!("podcastindex:   {id} {url}");
        }
        guard
            .record_import_gap(now, &counted)
            .map_err(|err| format!("could not store the import gap: {err}"))?;
        Some(counted.len())
    } else {
        eprintln!("podcastindex: import gap: no previous pass, nothing counted");
        None
    };
    guard
        .set_pass_started_at(now)
        .map_err(|err| format!("could not store the pass start: {err}"))?;
    Ok(gap)
}

/// The settings of the `podcastindex` mode.
#[derive(Debug, Clone)]
pub struct PassOptions {
    pub state: String,
    pub skip_db: String,
    pub feed_cache: String,
    pub api_base: String,
    pub concurrency: usize,
    pub host_delay_ms: u64,
    pub failed_feeds_output: String,
    pub force: bool,
    pub revalidate: bool,
    pub dry_run: bool,
}

/// Print `podcastindex: {err}` and stop the process with status 1.
fn fail(err: &str) -> ! {
    eprintln!("podcastindex: {err}");
    std::process::exit(1);
}

/// Run one pass of the `podcastindex` mode (ADR 0081 §1).
pub async fn run(options: PassOptions) {
    let credentials = Credentials::from_env().unwrap_or_else(|err| fail(&err));

    let store = Arc::new(Mutex::new(AttemptStore::open(&options.state)));
    let now = unix_now();

    let origin = query_origin_from_ingest_url(&ingest_url_from_env()).unwrap_or_else(|e| fail(&e));
    let client = reqwest::Client::new();

    let list_client = client.clone();
    let api_base = options.api_base.clone();
    let read_list = move || async move {
        fetch_music_list(&list_client, &api_base, &credentials, DEFAULT_USER_AGENT).await
    };

    let read_node = move || async move {
        let mut fetch_page = |cursor: Option<String>| {
            let client = client.clone();
            let origin = origin.clone();
            async move { fetch_feeds_recent_page(&client, &origin, cursor.as_deref()).await }
        };
        let feeds = collect_feeds(&mut fetch_page).await?;
        Ok(feeds.into_iter().map(NodeFeed::from).collect())
    };

    let skip_db = Arc::new(Mutex::new(FeedSkipDb::open(&options.skip_db)));
    let pass = UrlPass {
        concurrency: options.concurrency,
        host_delay_ms: options.host_delay_ms,
        failed_feeds_output: options.failed_feeds_output.clone(),
        force: options.force,
        feed_cache: options.feed_cache.clone(),
        revalidate: options.revalidate,
    };
    let pipeline = move |urls: Vec<String>, candidate_sink: OutcomeSink| {
        let sink: OutcomeSink = Arc::new(move |url: &str, report: &CrawlReport| {
            skip_db
                .lock()
                .expect("skip db mutex poisoned")
                .record_outcome(url, report, "podcastindex");
            candidate_sink(url, report);
        });
        batch::run_urls_with_sink(urls, pass, Some(sink))
    };

    let report = run_pass(&store, now, options.dry_run, read_list, read_node, pipeline)
        .await
        .unwrap_or_else(|err| fail(&err));
    if options.dry_run {
        eprintln!("podcastindex: dry run, no URL went to the pipeline");
    }
    eprintln!("{report}");
}

#[cfg(test)]
mod tests {
    use super::{
        Attempt, AttemptStore, MAX_DUE_CANDIDATES, MAX_LIST_ROWS, MIN_LIST_ROWS, NodeFeed,
        PassReport, RETRY_WAIT_SECS, check_limits, classify, comparable_url, count_import_gap,
        run_pass, split_waiting, waits,
    };
    use crate::crawl::{CrawlOutcome, CrawlReport};
    use crate::podcastindex_api::MusicListRow;
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    fn row(id: i64, url: &str, guid: Option<&str>) -> MusicListRow {
        MusicListRow {
            id,
            url: url.to_string(),
            podcast_guid: guid.map(ToString::to_string),
        }
    }

    fn node(guid: &str, url: &str) -> NodeFeed {
        NodeFeed {
            feed_guid: guid.to_string(),
            feed_url: url.to_string(),
        }
    }

    #[test]
    fn comparable_url_removes_scheme_www_and_trailing_slash() {
        assert_eq!(
            comparable_url("https://www.Wavlake.com/feed/music/ABC/"),
            "wavlake.com/feed/music/ABC",
            "the host is lowercase and the path keeps its case"
        );
    }

    #[test]
    fn comparable_url_ignores_the_scheme() {
        assert_eq!(
            comparable_url("http://thebearsnare.com/zza.xml"),
            comparable_url("https://thebearsnare.com/zza.xml"),
            "http and https must give the same text"
        );
    }

    #[test]
    fn comparable_url_accepts_a_url_with_no_scheme() {
        assert_eq!(comparable_url("wavlake.com/feed/x"), "wavlake.com/feed/x");
    }

    #[test]
    fn a_guid_matches_with_no_regard_to_letter_case() {
        let rows = vec![row(1, "https://a.example/one", Some("ABCDEF01-0000"))];
        let nodes = [node("abcdef01-0000", "https://b.example/other")];
        let result = classify(rows, &nodes);
        assert_eq!(result.held, 1, "the GUID must hold the row");
        assert!(result.candidates.is_empty(), "no candidate must remain");
    }

    #[test]
    fn a_guid_holds_a_row_at_another_wavlake_url_form() {
        let rows = vec![row(
            1,
            "https://wavlake.com/feed/1234",
            Some("abcdef01-0000"),
        )];
        let nodes = [node("abcdef01-0000", "https://wavlake.com/feed/music/1234")];
        let result = classify(rows, &nodes);
        assert_eq!(result.held, 1, "the GUID must hold the row");
        assert!(result.candidates.is_empty(), "no candidate must remain");
    }

    #[test]
    fn a_normalized_url_holds_a_row_with_an_unknown_guid() {
        let rows = vec![row(
            1,
            "https://www.example.com/feed/",
            Some("not-held-guid"),
        )];
        let nodes = [node("other-guid", "http://example.com/feed")];
        let result = classify(rows, &nodes);
        assert_eq!(result.held, 1, "the normalized URL must hold the row");
        assert!(result.candidates.is_empty(), "no candidate must remain");
    }

    #[test]
    fn a_row_with_no_guid_is_compared_by_url_only() {
        let nodes = [node("g1", "https://a.example/feed")];
        let held = classify(vec![row(1, "http://a.example/feed/", None)], &nodes);
        assert_eq!(held.held, 1, "a matching URL must hold the row");
        let missed = classify(vec![row(2, "https://z.example/feed", None)], &nodes);
        assert_eq!(missed.held, 0, "no URL matches");
        assert_eq!(missed.candidates.len(), 1, "the row must be a candidate");
    }

    #[test]
    fn a_row_that_matches_nothing_is_a_candidate_in_list_order() {
        let rows = vec![
            row(1, "https://x.example/1", Some("g-x")),
            row(2, "https://a.example/feed", Some("g-a")),
            row(3, "https://y.example/3", None),
            row(4, "https://z.example/4", Some("g-z")),
        ];
        let nodes = [node("g-a", "https://a.example/feed")];
        let total = rows.len();
        let result = classify(rows, &nodes);
        let ids: Vec<i64> = result.candidates.iter().map(|r| r.id).collect();
        assert_eq!(ids, vec![1, 3, 4], "candidates must keep the list order");
        assert_eq!(
            result.held + result.candidates.len(),
            total,
            "each row must be held or a candidate"
        );
    }

    const DAY: i64 = 86_400;

    fn attempt(outcome: &str, at: i64) -> Attempt {
        Attempt {
            outcome: outcome.to_string(),
            last_attempted_at: at,
        }
    }

    fn state_path(dir: &tempfile::TempDir) -> String {
        dir.path()
            .join("import_state.db")
            .to_string_lossy()
            .into_owned()
    }

    fn count_of(path: &str, id: i64) -> i64 {
        let conn = rusqlite::Connection::open(path).unwrap();
        conn.query_row(
            "SELECT attempt_count FROM music_list_attempts WHERE podcastindex_id = ?1",
            [id],
            |r| r.get(0),
        )
        .unwrap()
    }

    #[test]
    fn open_makes_file_and_keeps_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = state_path(&dir);
        assert!(!std::path::Path::new(&path).exists(), "no file at start");
        let store = AttemptStore::open(&path);
        assert!(std::path::Path::new(&path).exists(), "open makes the file");
        store
            .record(7, "https://a/x", "rejected", Some("why"), 100)
            .unwrap();
        drop(store);
        let again = AttemptStore::open(&path);
        let found = again.last_attempts(&[7]).unwrap();
        assert_eq!(found.get(&7), Some(&attempt("rejected", 100)), "row kept");
    }

    #[test]
    fn record_counts_attempts_and_replaces_outcome() {
        let dir = tempfile::tempdir().unwrap();
        let path = state_path(&dir);
        let store = AttemptStore::open(&path);
        store
            .record(1, "https://a/x", "fetch_error", Some("503"), 100)
            .unwrap();
        assert_eq!(count_of(&path, 1), 1, "a new ID has count 1");
        store
            .record(1, "https://a/y", "accepted", None, 200)
            .unwrap();
        assert_eq!(count_of(&path, 1), 2, "a second record adds 1");
        let found = store.last_attempts(&[1]).unwrap();
        assert_eq!(
            found.get(&1),
            Some(&attempt("accepted", 200)),
            "new outcome"
        );
    }

    #[test]
    fn last_attempts_reads_more_than_one_chunk() {
        let dir = tempfile::tempdir().unwrap();
        let store = AttemptStore::open(&state_path(&dir));
        for id in [5_i64, 600, 1_150] {
            store
                .record(id, "https://a/x", "rejected", None, 10)
                .unwrap();
        }
        let ids: Vec<i64> = (1..=1_200).collect();
        let found = store.last_attempts(&ids).unwrap();
        assert_eq!(found.len(), 3, "only the 3 rows with an attempt");
    }

    #[test]
    fn state_file_keeps_import_tables_and_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let path = state_path(&dir);
        let first = crate::modes::import::open_state_connection(&path);
        first
            .execute(
                "INSERT INTO import_progress (key, value) VALUES ('cursor', '42')",
                [],
            )
            .unwrap();
        drop(first);
        let store = AttemptStore::open(&path);
        store.record(1, "https://a/x", "rejected", None, 1).unwrap();
        drop(store);
        let conn = rusqlite::Connection::open(&path).unwrap();
        let cursor: String = conn
            .query_row(
                "SELECT value FROM import_progress WHERE key = 'cursor'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(cursor, "42", "the stored cursor stays");
        let memory: i64 = conn
            .query_row("SELECT COUNT(*) FROM import_feed_memory", [], |r| r.get(0))
            .unwrap();
        assert_eq!(memory, 0, "import_feed_memory stays in place");
    }

    #[test]
    fn waits_for_six_days_not_seven() {
        let now = 100 * DAY;
        assert!(
            waits(Some(&attempt("rejected", now - 6 * DAY)), now),
            "6 days old waits"
        );
        assert!(
            !waits(Some(&attempt("rejected", now - RETRY_WAIT_SECS)), now),
            "7 days old does not wait"
        );
    }

    #[test]
    fn waits_false_for_success_and_none() {
        let now = 100 * DAY;
        assert!(!waits(Some(&attempt("accepted", now - 3_600)), now));
        assert!(!waits(Some(&attempt("no_change", now - 3_600)), now));
        assert!(!waits(None, now), "no attempt does not wait");
    }

    #[test]
    fn waits_true_for_error_outcomes() {
        let now = 100 * DAY;
        for outcome in ["fetch_error", "parse_error", "ingest_error"] {
            assert!(
                waits(Some(&attempt(outcome, now - DAY)), now),
                "{outcome} 1 day old waits"
            );
        }
    }

    #[test]
    fn split_waiting_keeps_sequence() {
        let now = 100 * DAY;
        let list = vec![row(1, "a", None), row(2, "b", None), row(3, "c", None)];
        let mut attempts = HashMap::new();
        attempts.insert(2, attempt("rejected", now - DAY));
        let (ready, waiting) = split_waiting(list, &attempts, now);
        let ids: Vec<i64> = ready.iter().map(|r| r.id).collect();
        assert_eq!(ids, vec![1, 3], "ready candidates keep their sequence");
        assert_eq!(waiting, 1, "one candidate waits");
    }

    #[test]
    fn check_limits_stops_each_condition_of_the_adr() {
        assert!(check_limits(MIN_LIST_ROWS - 1, 0).is_err(), "999 rows");
        assert!(check_limits(MAX_LIST_ROWS + 1, 0).is_err(), "20001 rows");
        assert!(
            check_limits(8_601, MAX_DUE_CANDIDATES + 1).is_err(),
            "2001 due candidates"
        );
        assert!(check_limits(8_601, MAX_DUE_CANDIDATES).is_ok(), "2000 due");
        assert!(check_limits(MIN_LIST_ROWS, 0).is_ok(), "1000 rows");
    }

    #[test]
    fn check_limits_error_names_the_numbers() {
        let err = check_limits(999, 0).unwrap_err();
        assert!(err.contains("999") && err.contains("1000"), "{err}");
    }

    #[test]
    fn pass_report_display_is_one_line_with_each_count() {
        let mut outcomes = BTreeMap::new();
        outcomes.insert("accepted".to_string(), 56);
        outcomes.insert("fetch_error".to_string(), 3);
        let report = PassReport {
            rows: 8_601,
            held: 8_403,
            candidates: 198,
            waiting: 142,
            sent: 56,
            outcomes,
            import_gap: Some(1),
        };
        assert_eq!(
            report.to_string(),
            "podcastindex: rows=8601 held=8403 candidates=198 waiting=142 sent=56 \
             accepted=56 rejected=0 fetch_error=3 import_gap=1"
        );
        let none = PassReport {
            import_gap: None,
            ..report
        };
        assert!(
            none.to_string().ends_with(" import_gap=none"),
            "no previous pass prints none"
        );
    }

    fn list_of_rows(extra: Vec<MusicListRow>) -> Vec<MusicListRow> {
        let mut rows: Vec<MusicListRow> = (0_i64..)
            .take(MIN_LIST_ROWS)
            .map(|i| row(10_000 + i, &format!("https://held.example/{i}"), None))
            .collect();
        rows.extend(extra);
        rows
    }

    fn node_of_list() -> Vec<NodeFeed> {
        (0..MIN_LIST_ROWS)
            .map(|i| node(&format!("g{i}"), &format!("https://held.example/{i}")))
            .collect()
    }

    fn rejected_report() -> CrawlReport {
        CrawlReport {
            outcome: CrawlOutcome::Rejected {
                reason: "[v4v_payment] none".to_string(),
                warnings: Vec::new(),
            },
            fetch_http_status: Some(200),
            raw_medium: None,
            parsed_feed_guid: None,
            final_url: Some("https://other.example/final".to_string()),
            content_sha256: None,
            raw_xml: None,
            parsed_feed: None,
            redirects: Vec::new(),
            wasted_200: false,
        }
    }

    fn store_in(dir: &tempfile::TempDir) -> Arc<Mutex<AttemptStore>> {
        Arc::new(Mutex::new(AttemptStore::open(&state_path(dir))))
    }

    #[tokio::test]
    async fn a_failed_node_page_gives_err_and_no_pipeline_call() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_in(&dir);
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&calls);
        let result = run_pass(
            &store,
            1_000,
            false,
            || async { Ok(list_of_rows(vec![row(1, "https://new.example/a", None)])) },
            || async { Err("page 2 failed".to_string()) },
            move |_urls, _sink| async move {
                seen.fetch_add(1, Ordering::SeqCst);
            },
        )
        .await;
        assert!(result.is_err(), "a failed page must give Err");
        assert_eq!(calls.load(Ordering::SeqCst), 0, "no URL must be sent");
    }

    #[tokio::test]
    async fn a_failed_api_read_gives_err_and_no_pipeline_call() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_in(&dir);
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&calls);
        let result = run_pass(
            &store,
            1_000,
            false,
            || async { Err("api failed".to_string()) },
            || async { Ok(node_of_list()) },
            move |_urls, _sink| async move {
                seen.fetch_add(1, Ordering::SeqCst);
            },
        )
        .await;
        assert!(result.is_err(), "a failed API read must give Err");
        assert_eq!(calls.load(Ordering::SeqCst), 0, "no URL must be sent");
    }

    #[tokio::test]
    async fn a_list_outside_the_limits_gives_err_and_no_pipeline_call() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_in(&dir);
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&calls);
        let result = run_pass(
            &store,
            1_000,
            false,
            || async { Ok(vec![row(1, "https://new.example/a", None)]) },
            || async { Ok(Vec::new()) },
            move |_urls, _sink| async move {
                seen.fetch_add(1, Ordering::SeqCst);
            },
        )
        .await;
        assert!(result.is_err(), "a short list must give Err");
        assert_eq!(calls.load(Ordering::SeqCst), 0, "no URL must be sent");
    }

    /// The list holds 1,000 held rows and 2 candidates. One candidate waits.
    fn two_candidates() -> Vec<MusicListRow> {
        list_of_rows(vec![
            row(1, "https://due.example/a", Some("gd")),
            row(2, "https://wait.example/b", Some("gw")),
        ])
    }

    #[tokio::test]
    async fn the_pipeline_gets_only_the_url_of_the_due_candidate() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_in(&dir);
        store
            .lock()
            .unwrap()
            .record(2, "https://wait.example/b", "rejected", None, 1_000 - 60)
            .unwrap();
        let sent: Arc<Mutex<Vec<Vec<String>>>> = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&sent);
        let report = run_pass(
            &store,
            1_000,
            false,
            || async { Ok(two_candidates()) },
            || async { Ok(node_of_list()) },
            move |urls, _sink| async move {
                seen.lock().unwrap().push(urls);
            },
        )
        .await
        .expect("the pass must succeed");
        let sent = sent.lock().unwrap();
        assert_eq!(sent.len(), 1, "the pipeline must run one time");
        assert_eq!(
            sent[0],
            vec!["https://due.example/a".to_string()],
            "only the due candidate goes to the pipeline"
        );
        assert_eq!(report.held, MIN_LIST_ROWS, "held rows");
        assert_eq!(report.candidates, 2, "candidates");
        assert_eq!(report.waiting, 1, "waiting");
        assert_eq!(report.sent, 1, "sent");
    }

    #[tokio::test]
    async fn dry_run_never_calls_the_pipeline() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_in(&dir);
        store
            .lock()
            .unwrap()
            .record(2, "https://wait.example/b", "rejected", None, 1_000 - 60)
            .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&calls);
        let report = run_pass(
            &store,
            1_000,
            true,
            || async { Ok(two_candidates()) },
            || async { Ok(node_of_list()) },
            move |_urls, _sink| async move {
                seen.fetch_add(1, Ordering::SeqCst);
            },
        )
        .await
        .expect("a dry run must succeed");
        assert_eq!(calls.load(Ordering::SeqCst), 0, "no pipeline call");
        assert_eq!(report.candidates, 2, "candidates");
        assert_eq!(report.sent, 0, "sent");
    }

    #[tokio::test]
    async fn the_sink_stores_the_outcome_by_candidate_url_not_final_url() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_in(&dir);
        let report = run_pass(
            &store,
            1_000,
            false,
            || async { Ok(two_candidates()) },
            || async { Ok(node_of_list()) },
            move |_urls, sink| async move {
                sink("https://due.example/a", &rejected_report());
                sink("https://follow.example/x", &rejected_report());
            },
        )
        .await
        .expect("the pass must succeed");
        let found = store.lock().unwrap().last_attempts(&[1, 2]).unwrap();
        assert_eq!(
            found.get(&1).map(|a| a.outcome.as_str()),
            Some("rejected"),
            "candidate 1 must hold the outcome"
        );
        assert!(!found.contains_key(&2), "candidate 2 had no fetch");
        assert_eq!(
            report.outcomes.get("rejected"),
            Some(&1),
            "only the candidate URL is counted"
        );
    }

    #[test]
    fn pass_start_is_none_then_the_stored_value() {
        let dir = tempfile::tempdir().unwrap();
        let store = AttemptStore::open(&state_path(&dir));
        assert_eq!(store.pass_started_at().unwrap(), None, "no pass yet");
        store.set_pass_started_at(123).unwrap();
        store.set_pass_started_at(456).unwrap();
        assert_eq!(store.pass_started_at().unwrap(), Some(456), "last value");
    }

    fn insert_memory(path: &str, id: i64, url: &str, outcome: &str, at: i64) {
        let conn = rusqlite::Connection::open(path).unwrap();
        conn.execute(
            "INSERT INTO import_feed_memory
                 (podcastindex_id, feed_url, fetch_outcome, first_attempted_at, last_attempted_at)
             VALUES (?1, ?2, ?3, ?4, ?4)",
            rusqlite::params![id, url, outcome, at],
        )
        .unwrap();
    }

    #[test]
    fn the_import_gap_counts_only_new_accepted_rows_outside_the_list() {
        let dir = tempfile::tempdir().unwrap();
        let path = state_path(&dir);
        let store = AttemptStore::open(&path);
        insert_memory(&path, 1, "https://a.example/1", "accepted", 500);
        insert_memory(&path, 2, "https://a.example/2", "accepted", 500);
        insert_memory(&path, 3, "https://a.example/3", "rejected", 500);
        insert_memory(&path, 4, "https://a.example/4", "accepted", 99);
        let accepted = store.accepted_imports_since(100).unwrap();
        let list_ids: HashSet<i64> = [2].into_iter().collect();
        let counted = count_import_gap(&accepted, &list_ids);
        assert_eq!(
            counted,
            vec![(1, "https://a.example/1".to_string())],
            "only the accepted row after since with an ID outside the list counts"
        );
    }

    #[test]
    fn count_import_gap_keeps_the_input_sequence() {
        let accepted = vec![
            (9, "u9".to_string()),
            (3, "u3".to_string()),
            (5, "u5".to_string()),
        ];
        let list_ids: HashSet<i64> = [3].into_iter().collect();
        let ids: Vec<i64> = count_import_gap(&accepted, &list_ids)
            .iter()
            .map(|pair| pair.0)
            .collect();
        assert_eq!(ids, vec![9, 5], "the input sequence stays");
    }

    #[test]
    fn record_import_gap_writes_both_tables_and_a_repeat_fails_whole() {
        let dir = tempfile::tempdir().unwrap();
        let path = state_path(&dir);
        let store = AttemptStore::open(&path);
        let rows = vec![(1, "u1".to_string()), (2, "u2".to_string())];
        store.record_import_gap(1_000, &rows).unwrap();
        let conn = rusqlite::Connection::open(&path).unwrap();
        let counted: i64 = conn
            .query_row(
                "SELECT counted FROM import_gap_passes WHERE pass_at = 1000",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(counted, 2, "counted holds the number of rows");
        let kept: i64 = conn
            .query_row("SELECT COUNT(*) FROM import_gap_rows", [], |r| r.get(0))
            .unwrap();
        assert_eq!(kept, 2, "one row for each pair");

        let more = vec![(3, "u3".to_string())];
        assert!(
            store.record_import_gap(1_000, &more).is_err(),
            "a second call with the same pass_at fails"
        );
        let kept: i64 = conn
            .query_row("SELECT COUNT(*) FROM import_gap_rows", [], |r| r.get(0))
            .unwrap();
        assert_eq!(kept, 2, "a failed call leaves no partial row");
    }

    #[tokio::test]
    async fn a_pass_with_no_previous_start_counts_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let path = state_path(&dir);
        let store = Arc::new(Mutex::new(AttemptStore::open(&path)));
        let report = run_pass(
            &store,
            1_000,
            true,
            || async { Ok(two_candidates()) },
            || async { Ok(node_of_list()) },
            |_urls, _sink| async {},
        )
        .await
        .expect("the pass must succeed");
        assert_eq!(report.import_gap, None, "no previous pass");
        let conn = rusqlite::Connection::open(&path).unwrap();
        let passes: i64 = conn
            .query_row("SELECT COUNT(*) FROM import_gap_passes", [], |r| r.get(0))
            .unwrap();
        assert_eq!(passes, 0, "no gap row is written");
        assert_eq!(
            store.lock().unwrap().pass_started_at().unwrap(),
            Some(1_000),
            "the start time is stored"
        );
    }

    #[tokio::test]
    async fn a_pass_with_a_previous_start_counts_the_gap() {
        let dir = tempfile::tempdir().unwrap();
        let path = state_path(&dir);
        let store = Arc::new(Mutex::new(AttemptStore::open(&path)));
        store.lock().unwrap().set_pass_started_at(500).unwrap();
        insert_memory(&path, 77, "https://gap.example/x", "accepted", 600);
        insert_memory(&path, 10_000, "https://held.example/0", "accepted", 600);
        let report = run_pass(
            &store,
            1_000,
            true,
            || async { Ok(two_candidates()) },
            || async { Ok(node_of_list()) },
            |_urls, _sink| async {},
        )
        .await
        .expect("the pass must succeed");
        assert_eq!(
            report.import_gap,
            Some(1),
            "one accepted feed is not listed"
        );
        assert!(
            report.to_string().contains("import_gap=1"),
            "the report prints the count"
        );
        let conn = rusqlite::Connection::open(&path).unwrap();
        let url: String = conn
            .query_row(
                "SELECT feed_url FROM import_gap_rows WHERE pass_at = 1000 AND podcastindex_id = 77",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(url, "https://gap.example/x", "the counted row is stored");
    }

    #[tokio::test]
    async fn a_failed_list_read_leaves_the_pass_start_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_in(&dir);
        store.lock().unwrap().set_pass_started_at(500).unwrap();
        let result = run_pass(
            &store,
            1_000,
            false,
            || async { Err("api failed".to_string()) },
            || async { Ok(node_of_list()) },
            |_urls, _sink| async {},
        )
        .await;
        assert!(result.is_err(), "a failed list read must give Err");
        assert_eq!(
            store.lock().unwrap().pass_started_at().unwrap(),
            Some(500),
            "the start time stays"
        );
    }

    #[tokio::test]
    async fn a_short_list_gives_err_and_no_gap_row_and_keeps_the_start() {
        let dir = tempfile::tempdir().unwrap();
        let path = state_path(&dir);
        let store = Arc::new(Mutex::new(AttemptStore::open(&path)));
        store.lock().unwrap().set_pass_started_at(500).unwrap();
        let result = run_pass(
            &store,
            1_000,
            false,
            || async { Ok(vec![row(1, "https://new.example/a", None)]) },
            || async { Ok(Vec::new()) },
            |_urls, _sink| async {},
        )
        .await;
        assert!(result.is_err(), "a short list must give Err");
        let conn = rusqlite::Connection::open(&path).unwrap();
        let passes: i64 = conn
            .query_row("SELECT COUNT(*) FROM import_gap_passes", [], |r| r.get(0))
            .unwrap();
        assert_eq!(passes, 0, "no gap row is written");
        assert_eq!(
            store.lock().unwrap().pass_started_at().unwrap(),
            Some(500),
            "the start time stays"
        );
    }
}
