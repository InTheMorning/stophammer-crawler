use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::fs::OpenOptions;
use std::io::{self, Read, Write};
use std::io::{BufRead, BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use chrono::Utc;
use flate2::read::GzDecoder;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use stophammer_parser::extract_podcast_namespace;
use stophammer_parser::types::{IngestFeedData, IngestPodcastNamespaceSnapshot};
use tar::Archive;
use tokio::sync::{Mutex as AsyncMutex, oneshot, watch};

use crate::crawl::{CrawlConfig, CrawlOutcome, CrawlReport, crawl_feed_report};
use crate::pool::run_pool;
use crate::url_queue::{host_key, interleave_by_host};

const IMPORT_FETCH_TIMEOUT_SECS: u64 = 5;
const IMPORT_TASK_HARD_TIMEOUT_SECS: u64 = 15;
const IMPORT_MAX_CONSECUTIVE_TIMEOUTS: i64 = 3;
const IMPORT_BATCH_HEARTBEAT_SECS: u64 = 30;
const IMPORT_BATCH_HEARTBEAT_SAMPLE_SIZE: usize = 5;
const WAVLAKE_IMPORT_MIN_DELAY_MS: u64 = 2_000;
const WAVLAKE_IMPORT_FALLBACK_429_BACKOFF_SECS: u64 = 300;
const SNAPSHOT_CONNECT_TIMEOUT_SECS: u64 = 30;
const RESOLVERCTL_BIN_ENV: &str = "RESOLVERCTL_BIN";
const RESOLVER_DB_PATH_ENV: &str = "RESOLVER_DB_PATH";
const RESOLVER_HEARTBEAT_INTERVAL_SECS: u64 = 60;
const WAVLAKE_HOSTS: [&str; 2] = ["wavlake.com", "www.wavlake.com"];

struct CandidateRow {
    id: i64,
    url: String,
    podcast_guid: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ImportScope {
    AllFeeds,
    WavlakeOnly,
}

impl ImportScope {
    fn from_wavlake_only(wavlake_only: bool) -> Self {
        if wavlake_only {
            Self::WavlakeOnly
        } else {
            Self::AllFeeds
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::AllFeeds => "all_feeds",
            Self::WavlakeOnly => "wavlake_only",
        }
    }

    fn cursor_key(self) -> &'static str {
        match self {
            Self::AllFeeds => "last_processed_id:all_feeds",
            Self::WavlakeOnly => "last_processed_id:wavlake_only",
        }
    }

    fn effective_concurrency(self, requested: usize) -> usize {
        match self {
            Self::AllFeeds => requested,
            Self::WavlakeOnly => 1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KnownSkipKind {
    Irrelevant,
    Success,
}

impl KnownSkipKind {
    fn label(self) -> &'static str {
        match self {
            Self::Irrelevant => "skipped_known_irrelevant",
            Self::Success => "skipped_known_success",
        }
    }
}

fn effective_audit_append(requested_replace: bool, audit_output: Option<&str>) -> bool {
    audit_output.is_some() && !requested_replace
}

#[derive(Debug, Default)]
struct WavlakeThrottle {
    state: AsyncMutex<WavlakeThrottleState>,
}

#[derive(Debug, Default)]
struct WavlakeThrottleState {
    next_allowed_at: Option<Instant>,
    extra_delay_secs: u64,
}

impl WavlakeThrottle {
    fn new() -> Self {
        Self::default()
    }

    async fn wait_for_turn(&self, candidate: &CandidateRow) {
        loop {
            let sleep_for = {
                let guard = self.state.lock().await;
                guard
                    .next_allowed_at
                    .and_then(|deadline| deadline.checked_duration_since(Instant::now()))
            };

            let Some(delay) = sleep_for else {
                break;
            };
            if delay.is_zero() {
                break;
            }

            eprintln!(
                "  import: wavlake throttle sleeping {:?} before id={} {}",
                delay, candidate.id, candidate.url,
            );
            tokio::time::sleep(delay).await;
        }
    }

    async fn record_attempt(&self, candidate: &CandidateRow, report: &CrawlReport) {
        let mut guard = self.state.lock().await;
        let status = report.fetch_http_status;
        if status == Some(429) || status == Some(503) {
            guard.extra_delay_secs = guard.extra_delay_secs.saturating_add(1);
        }
        let delay = wavlake_throttle_delay(report, guard.extra_delay_secs);
        guard.next_allowed_at = Some(Instant::now() + delay);

        if status == Some(429) || status == Some(503) {
            eprintln!(
                "  import: wavlake {} backoff {:?} after id={} {} (extra_inter_fetch_delay={}s)",
                status.unwrap(),
                delay,
                candidate.id,
                candidate.url,
                guard.extra_delay_secs,
            );
        }
    }
}

#[derive(Debug, Clone)]
struct ActiveImportTask {
    url: String,
    started_at: Instant,
}

struct ActiveImportTaskGuard {
    active_tasks: Arc<Mutex<BTreeMap<i64, ActiveImportTask>>>,
    row_id: i64,
}

impl Drop for ActiveImportTaskGuard {
    fn drop(&mut self) {
        self.active_tasks
            .lock()
            .expect("active import task mutex poisoned")
            .remove(&self.row_id);
    }
}

#[derive(Debug, Clone)]
struct ImportAuditRow {
    source_db: ImportAuditSourceDb,
    fetched_at: i64,
    fetch: ImportAuditFetch,
    raw_xml: String,
    parsed_feed: Option<IngestFeedData>,
    podcast_namespace: Option<IngestPodcastNamespaceSnapshot>,
    parse_error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ImportAuditSourceDb {
    feed_guid: String,
    feed_url: String,
    title: String,
}

#[derive(Debug, Clone, Serialize)]
struct ImportAuditFetch {
    final_url: Option<String>,
    http_status: Option<u16>,
    content_sha256: Option<String>,
    error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ImportAuditRecord {
    source_db: ImportAuditSourceDb,
    fetched_at: i64,
    fetch: ImportAuditFetch,
    raw_xml: String,
    parsed_feed: Option<IngestFeedData>,
    podcast_namespace: Option<IngestPodcastNamespaceSnapshot>,
    parse_error: Option<String>,
}

impl From<ImportAuditRow> for ImportAuditRecord {
    fn from(row: ImportAuditRow) -> Self {
        Self {
            source_db: row.source_db,
            fetched_at: row.fetched_at,
            fetch: row.fetch,
            raw_xml: row.raw_xml,
            parsed_feed: row.parsed_feed,
            podcast_namespace: row.podcast_namespace,
            parse_error: row.parse_error,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ImportAuditDedupeKey {
    feed_guid: String,
    content_sha256: String,
}

#[derive(Debug, Deserialize)]
struct ExistingImportAuditRecord {
    source_db: ExistingImportAuditSourceDb,
    fetch: ExistingImportAuditFetch,
}

#[derive(Debug, Deserialize)]
struct ExistingImportAuditSourceDb {
    feed_guid: String,
}

#[derive(Debug, Deserialize)]
struct ExistingImportAuditFetch {
    content_sha256: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct KnownImportMemory {
    fetch_http_status: Option<u16>,
    fetch_outcome: String,
    outcome_reason: Option<String>,
    raw_medium: Option<String>,
    parsed_feed_guid: Option<String>,
    consecutive_timeout_count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ImportMemoryRow {
    podcastindex_id: i64,
    feed_url: String,
    podcastindex_guid: Option<String>,
    fetch_http_status: Option<u16>,
    fetch_outcome: String,
    outcome_reason: Option<String>,
    retryable: bool,
    raw_medium: Option<String>,
    parsed_feed_guid: Option<String>,
    attempted_at: i64,
    attempt_duration_ms: i64,
}

impl ImportMemoryRow {
    fn from_crawl_report(
        candidate: &CandidateRow,
        report: &CrawlReport,
        attempted_at: i64,
        attempt_duration_ms: i64,
    ) -> Self {
        let feed_url = report
            .final_url
            .as_ref()
            .filter(|u| u.as_str() != candidate.url)
            .cloned()
            .unwrap_or_else(|| candidate.url.clone());
        Self {
            podcastindex_id: candidate.id,
            feed_url,
            podcastindex_guid: candidate.podcast_guid.clone(),
            fetch_http_status: report.fetch_http_status,
            fetch_outcome: report.outcome.label().to_string(),
            outcome_reason: report.outcome.reason().map(ToOwned::to_owned),
            retryable: report.is_retryable(),
            raw_medium: report.raw_medium.clone(),
            parsed_feed_guid: report.parsed_feed_guid.clone(),
            attempted_at,
            attempt_duration_ms,
        }
    }

    fn skipped_known(
        candidate: &CandidateRow,
        known: &KnownImportMemory,
        attempted_at: i64,
        attempt_duration_ms: i64,
        skip_kind: KnownSkipKind,
    ) -> Self {
        let outcome_reason = match skip_kind {
            KnownSkipKind::Irrelevant => known.raw_medium.as_ref().map_or_else(
                || {
                    known
                        .outcome_reason
                        .clone()
                        .or_else(|| Some("known medium-gate rejection".to_string()))
                },
                |raw_medium| Some(format!("known raw_medium={raw_medium}")),
            ),
            KnownSkipKind::Success => Some(format!(
                "known prior successful ingest outcome={}",
                known.fetch_outcome
            )),
        };

        Self {
            podcastindex_id: candidate.id,
            feed_url: candidate.url.clone(),
            podcastindex_guid: candidate.podcast_guid.clone(),
            fetch_http_status: known.fetch_http_status,
            fetch_outcome: skip_kind.label().to_string(),
            outcome_reason,
            retryable: false,
            raw_medium: known.raw_medium.clone(),
            parsed_feed_guid: known.parsed_feed_guid.clone(),
            attempted_at,
            attempt_duration_ms,
        }
    }
}

enum ImportStateCommand {
    UpsertMemory(ImportMemoryRow),
    SetLastId {
        scope: ImportScope,
        id: i64,
        ack: oneshot::Sender<()>,
    },
    Shutdown {
        ack: oneshot::Sender<()>,
    },
}

enum ImportAuditCommand {
    Write(Box<ImportAuditRow>),
    Shutdown { ack: oneshot::Sender<()> },
}

struct ImportStateWriter {
    tx: mpsc::Sender<ImportStateCommand>,
    handle: Option<thread::JoinHandle<()>>,
}

struct ImportAuditWriter {
    tx: mpsc::Sender<ImportAuditCommand>,
    handle: Option<thread::JoinHandle<()>>,
    lock_path: PathBuf,
}

impl ImportStateWriter {
    fn spawn(path: &str) -> Self {
        let (tx, rx) = mpsc::channel();
        let path = path.to_string();
        let handle = thread::spawn(move || {
            let conn = open_state_connection(&path);
            while let Ok(command) = rx.recv() {
                match command {
                    ImportStateCommand::UpsertMemory(row) => {
                        if let Err(e) = upsert_import_memory(&conn, &row) {
                            eprintln!(
                                "import: WARNING: failed to upsert memory for podcastindex_id={}: {e} (skipping)",
                                row.podcastindex_id
                            );
                        }
                    }
                    ImportStateCommand::SetLastId { scope, id, ack } => {
                        set_last_processed_id(&conn, scope, id).unwrap_or_else(|e| {
                            panic!(
                                "failed to persist import cursor key={} at id={id}: {e}",
                                scope.cursor_key()
                            );
                        });
                        let _ = ack.send(());
                    }
                    ImportStateCommand::Shutdown { ack } => {
                        let _ = ack.send(());
                        break;
                    }
                }
            }
        });

        Self {
            tx,
            handle: Some(handle),
        }
    }

    async fn set_last_id(&self, scope: ImportScope, id: i64) {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.tx
            .send(ImportStateCommand::SetLastId {
                scope,
                id,
                ack: ack_tx,
            })
            .expect("import state writer channel closed unexpectedly");
        ack_rx
            .await
            .expect("import state writer did not acknowledge cursor update");
    }

    async fn shutdown(mut self) {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.tx
            .send(ImportStateCommand::Shutdown { ack: ack_tx })
            .expect("import state writer channel closed unexpectedly");
        ack_rx
            .await
            .expect("import state writer did not acknowledge shutdown");
        if let Some(handle) = self.handle.take() {
            tokio::task::spawn_blocking(move || {
                handle.join().expect("import state writer thread panicked");
            })
            .await
            .expect("failed to join import state writer thread");
        }
    }
}

impl ImportAuditWriter {
    fn spawn(path: &str, append: bool) -> Self {
        let (tx, rx) = mpsc::channel();
        let path = path.to_string();
        let lock_path = acquire_pid_lock(&path, "import audit");
        let handle = thread::spawn(move || {
            let seen_keys = load_existing_audit_keys(&path, append);
            let writer = open_audit_output(&path, append);
            run_audit_writer_loop(writer, rx, seen_keys);
        });

        Self {
            tx,
            handle: Some(handle),
            lock_path,
        }
    }

    async fn shutdown(mut self) {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.tx
            .send(ImportAuditCommand::Shutdown { ack: ack_tx })
            .expect("import audit writer channel closed unexpectedly");
        ack_rx
            .await
            .expect("import audit writer did not acknowledge shutdown");
        if let Some(handle) = self.handle.take() {
            tokio::task::spawn_blocking(move || {
                handle.join().expect("import audit writer thread panicked");
            })
            .await
            .expect("failed to join import audit writer thread");
        }
        fs::remove_file(&self.lock_path).unwrap_or_else(|err| {
            panic!(
                "failed to remove import audit lock {}: {err}",
                self.lock_path.display()
            )
        });
    }
}

fn acquire_pid_lock(path: &str, label: &str) -> PathBuf {
    let lock_path = PathBuf::from(format!("{path}.lock"));
    if let Some(parent) = lock_path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).unwrap_or_else(|err| {
            panic!(
                "failed to create {label} lock directory {}: {err}",
                parent.display()
            )
        });
    }
    match OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&lock_path)
    {
        Ok(mut f) => {
            let _ = write!(f, "{}", std::process::id());
        }
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
            if let Ok(contents) = fs::read_to_string(&lock_path)
                && let Ok(pid) = contents.trim().parse::<u32>()
            {
                assert!(
                    !Path::new(&format!("/proc/{pid}")).exists(),
                    "{label} lock {} held by live process pid={pid}",
                    lock_path.display()
                );
            }
            eprintln!(
                "import: WARNING: reclaiming stale {label} lock {}",
                lock_path.display()
            );
            let mut f = OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(&lock_path)
                .unwrap_or_else(|err| {
                    panic!(
                        "failed to reclaim {label} lock {}: {err}",
                        lock_path.display()
                    )
                });
            let _ = write!(f, "{}", std::process::id());
        }
        Err(err) => {
            panic!(
                "failed to acquire {label} lock {}: {err}",
                lock_path.display()
            );
        }
    }
    lock_path
}

struct PidLockGuard {
    path: PathBuf,
}

impl Drop for PidLockGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn enqueue_import_memory(tx: &mpsc::Sender<ImportStateCommand>, row: ImportMemoryRow) {
    if let Err(e) = tx.send(ImportStateCommand::UpsertMemory(row)) {
        let ImportStateCommand::UpsertMemory(row) = e.0 else {
            return;
        };
        eprintln!(
            "import: WARNING: state writer channel closed, dropping memory for podcastindex_id={}",
            row.podcastindex_id
        );
    }
}

fn enqueue_import_audit(tx: &mpsc::Sender<ImportAuditCommand>, row: ImportAuditRow) {
    if tx.send(ImportAuditCommand::Write(Box::new(row))).is_err() {
        eprintln!("import: WARNING: audit writer channel closed, dropping audit row");
    }
}

fn open_audit_output(path: &str, append: bool) -> BufWriter<fs::File> {
    if let Some(parent) = Path::new(path).parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).unwrap_or_else(|e| {
            panic!(
                "failed to create import audit output directory {}: {e}",
                parent.display()
            )
        });
    }

    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(append)
        .truncate(!append)
        .open(path)
        .unwrap_or_else(|e| panic!("failed to open import audit output {path}: {e}"));
    BufWriter::new(file)
}

fn load_existing_audit_keys(path: &str, append: bool) -> HashSet<ImportAuditDedupeKey> {
    if !append || !Path::new(path).is_file() {
        return HashSet::new();
    }

    let file = fs::File::open(path)
        .unwrap_or_else(|e| panic!("failed to open existing import audit output {path}: {e}"));
    let reader = BufReader::new(file);
    let mut keys = HashSet::new();

    for line in reader.lines().map_while(Result::ok) {
        let Ok(record) = serde_json::from_str::<ExistingImportAuditRecord>(&line) else {
            continue;
        };
        let Some(content_sha256) = record.fetch.content_sha256 else {
            continue;
        };
        keys.insert(ImportAuditDedupeKey {
            feed_guid: record.source_db.feed_guid,
            content_sha256,
        });
    }

    keys
}

fn audit_dedupe_key(row: &ImportAuditRow) -> Option<ImportAuditDedupeKey> {
    Some(ImportAuditDedupeKey {
        feed_guid: row.source_db.feed_guid.clone(),
        content_sha256: row.fetch.content_sha256.clone()?,
    })
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "the writer thread takes ownership of the receiver for the duration of the loop"
)]
fn run_audit_writer_loop(
    mut writer: BufWriter<fs::File>,
    rx: mpsc::Receiver<ImportAuditCommand>,
    mut seen_keys: HashSet<ImportAuditDedupeKey>,
) {
    while let Ok(command) = rx.recv() {
        match command {
            ImportAuditCommand::Write(row) => {
                if let Some(key) = audit_dedupe_key(&row)
                    && !seen_keys.insert(key)
                {
                    continue;
                }
                let record = ImportAuditRecord::from(*row);
                serde_json::to_writer(&mut writer, &record)
                    .expect("failed to serialize import audit row");
                writer
                    .write_all(b"\n")
                    .expect("failed to terminate import audit row");
                writer.flush().expect("failed to flush import audit row");
            }
            ImportAuditCommand::Shutdown { ack } => {
                writer.flush().expect("failed to flush import audit writer");
                let _ = ack.send(());
                break;
            }
        }
    }
}

fn build_import_audit_row(
    candidate: &CandidateRow,
    report: &CrawlReport,
    fetched_at: i64,
) -> Option<ImportAuditRow> {
    let raw_xml = report.raw_xml.clone()?;
    if report.fetch_http_status != Some(200) {
        return None;
    }

    if !matches!(
        report.outcome,
        CrawlOutcome::Accepted { .. } | CrawlOutcome::NoChange
    ) {
        return None;
    }

    let parsed_feed = report.parsed_feed.clone();
    let podcast_namespace = extract_podcast_namespace(&raw_xml).ok().flatten();
    let source_feed_guid = parsed_feed
        .as_ref()
        .map(|feed| feed.feed_guid.clone())
        .or_else(|| candidate.podcast_guid.clone())
        .unwrap_or_else(|| candidate.url.clone());
    let title = parsed_feed
        .as_ref()
        .map_or_else(|| candidate.url.clone(), |feed| feed.title.clone());

    Some(ImportAuditRow {
        source_db: ImportAuditSourceDb {
            feed_guid: source_feed_guid,
            feed_url: candidate.url.clone(),
            title,
        },
        fetched_at,
        fetch: ImportAuditFetch {
            final_url: report.final_url.clone(),
            http_status: report.fetch_http_status,
            content_sha256: report.content_sha256.clone(),
            error: None,
        },
        raw_xml,
        parsed_feed,
        podcast_namespace,
        parse_error: None,
    })
}

fn begin_active_import_task(
    active_tasks: &Arc<Mutex<BTreeMap<i64, ActiveImportTask>>>,
    row: &CandidateRow,
) -> ActiveImportTaskGuard {
    active_tasks
        .lock()
        .expect("active import task mutex poisoned")
        .insert(
            row.id,
            ActiveImportTask {
                url: row.url.clone(),
                started_at: Instant::now(),
            },
        );

    ActiveImportTaskGuard {
        active_tasks: Arc::clone(active_tasks),
        row_id: row.id,
    }
}

fn build_import_timeout_report(timeout_secs: u64) -> CrawlReport {
    CrawlReport {
        outcome: CrawlOutcome::FetchError {
            reason: format!(
                "import crawl exceeded hard deadline of {timeout_secs}s; marking row as failed"
            ),
            retryable: true,
            retry_after_secs: None,
        },
        fetch_http_status: None,
        raw_medium: None,
        parsed_feed_guid: None,
        final_url: None,
        content_sha256: None,
        raw_xml: None,
        parsed_feed: None,
    }
}

async fn run_import_batch_heartbeat(
    batch_start_cursor: i64,
    batch_max_id: i64,
    batch_len: usize,
    active_tasks: Arc<Mutex<BTreeMap<i64, ActiveImportTask>>>,
    mut stop_rx: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            changed = stop_rx.changed() => {
                match changed {
                    Ok(()) if *stop_rx.borrow() => break,
                    Ok(()) => {}
                    Err(_) => break,
                }
            }
            () = tokio::time::sleep(Duration::from_secs(IMPORT_BATCH_HEARTBEAT_SECS)) => {
                let snapshot = {
                    let active = active_tasks
                        .lock()
                        .expect("active import task mutex poisoned");
                    active
                        .iter()
                        .map(|(id, task)| {
                            (
                                *id,
                                task.url.clone(),
                                task.started_at.elapsed().as_secs(),
                            )
                        })
                        .collect::<Vec<_>>()
                };

                if snapshot.is_empty() {
                    continue;
                }

                let oldest_pending_secs = snapshot
                    .iter()
                    .map(|(_, _, elapsed_secs)| *elapsed_secs)
                    .max()
                    .unwrap_or(0);
                let pending_sample = snapshot
                    .iter()
                    .take(IMPORT_BATCH_HEARTBEAT_SAMPLE_SIZE)
                    .map(|(id, url, elapsed_secs)| format!("id={id} age={elapsed_secs}s {url}"))
                    .collect::<Vec<_>>()
                    .join(" | ");

                eprintln!(
                    "import: batch heartbeat start_cursor={batch_start_cursor} batch_max_id={batch_max_id} batch_len={batch_len} pending={} oldest_pending={}s sample={pending_sample}",
                    snapshot.len(),
                    oldest_pending_secs,
                );
            }
        }
    }
}

struct ResolverImportGuard {
    resolverctl_bin: String,
    resolver_db_path: String,
    stop_heartbeat: mpsc::SyncSender<()>,
    heartbeat_thread: Option<std::thread::JoinHandle<()>>,
}

impl ResolverImportGuard {
    fn maybe_activate() -> Option<Self> {
        let resolver_db_path = std::env::var(RESOLVER_DB_PATH_ENV).ok()?;
        let resolverctl_bin =
            std::env::var(RESOLVERCTL_BIN_ENV).unwrap_or_else(|_| "resolverctl".to_string());

        set_import_active(&resolverctl_bin, &resolver_db_path, true);
        let (stop_heartbeat, heartbeat_stop_rx) = mpsc::sync_channel(1);
        let thread_bin = resolverctl_bin.clone();
        let thread_db = resolver_db_path.clone();
        let heartbeat_thread = std::thread::spawn(move || {
            loop {
                match heartbeat_stop_rx
                    .recv_timeout(Duration::from_secs(RESOLVER_HEARTBEAT_INTERVAL_SECS))
                {
                    Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        set_import_active(&thread_bin, &thread_db, true);
                    }
                }
            }
        });
        Some(Self {
            resolverctl_bin,
            resolver_db_path,
            stop_heartbeat,
            heartbeat_thread: Some(heartbeat_thread),
        })
    }
}

impl Drop for ResolverImportGuard {
    fn drop(&mut self) {
        let _ = self.stop_heartbeat.send(());
        if let Some(handle) = self.heartbeat_thread.take() {
            let _ = handle.join();
        }
        set_import_active(&self.resolverctl_bin, &self.resolver_db_path, false);
    }
}

/// Resume cursor stored in a separate state DB.
struct ProgressStore {
    conn: Connection,
}

impl ProgressStore {
    fn open(path: &str) -> Self {
        let conn = open_state_connection(path);
        Self { conn }
    }

    fn get_last_id(&self, scope: ImportScope) -> i64 {
        let scoped_value = self
            .conn
            .query_row(
                "SELECT value FROM import_progress WHERE key = ?1",
                [scope.cursor_key()],
                |row| row.get::<_, String>(0),
            )
            .ok()
            .and_then(|v| v.parse().ok());
        if let Some(value) = scoped_value {
            return value;
        }

        if scope == ImportScope::AllFeeds {
            return self
                .conn
                .query_row(
                    "SELECT value FROM import_progress WHERE key = 'last_processed_id'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
        }

        0
    }

    fn set_last_id(&self, scope: ImportScope, id: i64) {
        set_last_processed_id(&self.conn, scope, id).expect("failed to override import cursor");
    }

    fn known_memory_for_ids(
        &self,
        ids: &[i64],
    ) -> std::collections::HashMap<i64, KnownImportMemory> {
        load_known_import_memory(&self.conn, ids)
    }
}

fn initialize_state_schema(conn: &Connection) {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS import_progress (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS import_feed_memory (
            podcastindex_id    INTEGER PRIMARY KEY,
            feed_url           TEXT NOT NULL,
            podcastindex_guid  TEXT,
            fetch_http_status  INTEGER,
            fetch_outcome      TEXT NOT NULL,
            outcome_reason     TEXT,
            retryable          INTEGER NOT NULL DEFAULT 0,
            raw_medium         TEXT,
            parsed_feed_guid   TEXT,
            first_attempted_at INTEGER NOT NULL,
            last_attempted_at  INTEGER NOT NULL,
            attempt_duration_ms INTEGER NOT NULL DEFAULT 0,
            attempt_count      INTEGER NOT NULL DEFAULT 1,
            consecutive_timeout_count INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_import_feed_memory_status
            ON import_feed_memory(fetch_http_status);
        CREATE INDEX IF NOT EXISTS idx_import_feed_memory_medium
            ON import_feed_memory(raw_medium);",
    )
    .expect("failed to initialize import state schema");

    let has_attempt_duration_ms = conn
        .prepare("PRAGMA table_info(import_feed_memory)")
        .expect("failed to prepare import_feed_memory table_info pragma")
        .query_map([], |row| row.get::<_, String>(1))
        .expect("failed to inspect import_feed_memory schema")
        .filter_map(Result::ok)
        .any(|column| column == "attempt_duration_ms");
    if !has_attempt_duration_ms {
        conn.execute(
            "ALTER TABLE import_feed_memory ADD COLUMN attempt_duration_ms INTEGER NOT NULL DEFAULT 0",
            [],
        )
        .expect("failed to add attempt_duration_ms to import_feed_memory");
    }

    let has_consecutive_timeout_count = conn
        .prepare("PRAGMA table_info(import_feed_memory)")
        .expect("failed to prepare import_feed_memory table_info pragma")
        .query_map([], |row| row.get::<_, String>(1))
        .expect("failed to inspect import_feed_memory schema")
        .filter_map(Result::ok)
        .any(|column| column == "consecutive_timeout_count");
    if !has_consecutive_timeout_count {
        conn.execute(
            "ALTER TABLE import_feed_memory ADD COLUMN consecutive_timeout_count INTEGER NOT NULL DEFAULT 0",
            [],
        )
        .expect("failed to add consecutive_timeout_count to import_feed_memory");
    }
}

fn open_state_connection(path: &str) -> Connection {
    if let Some(parent) = Path::new(path).parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).unwrap_or_else(|e| {
            panic!(
                "failed to create import state directory {}: {e}",
                parent.display()
            )
        });
    }

    let conn = Connection::open(path).expect("failed to open import state DB");
    conn.pragma_update(None, "journal_mode", "MEMORY")
        .expect("failed to set import state journal mode");
    conn.busy_timeout(Duration::from_secs(5))
        .expect("failed to set import state busy timeout");
    initialize_state_schema(&conn);
    conn
}

fn set_last_processed_id(
    conn: &Connection,
    scope: ImportScope,
    id: i64,
) -> rusqlite::Result<usize> {
    conn.execute(
        "INSERT INTO import_progress (key, value) VALUES (?1, ?2)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        params![scope.cursor_key(), id.to_string()],
    )
}

fn is_timeout_outcome(row: &ImportMemoryRow) -> bool {
    row.fetch_http_status.is_none() && row.fetch_outcome == "fetch_error"
}

fn upsert_import_memory(conn: &Connection, row: &ImportMemoryRow) -> rusqlite::Result<usize> {
    let timeout_flag: i64 = i64::from(is_timeout_outcome(row));
    conn.execute(
        "INSERT INTO import_feed_memory (
            podcastindex_id,
            feed_url,
            podcastindex_guid,
            fetch_http_status,
            fetch_outcome,
            outcome_reason,
            retryable,
            raw_medium,
            parsed_feed_guid,
            first_attempted_at,
            last_attempted_at,
            attempt_duration_ms,
            attempt_count,
            consecutive_timeout_count
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?10, ?11, 1, ?12)
        ON CONFLICT(podcastindex_id) DO UPDATE SET
            feed_url = excluded.feed_url,
            podcastindex_guid = excluded.podcastindex_guid,
            fetch_http_status = excluded.fetch_http_status,
            fetch_outcome = excluded.fetch_outcome,
            outcome_reason = excluded.outcome_reason,
            retryable = excluded.retryable,
            raw_medium = excluded.raw_medium,
            parsed_feed_guid = excluded.parsed_feed_guid,
            last_attempted_at = excluded.last_attempted_at,
            attempt_duration_ms = excluded.attempt_duration_ms,
            attempt_count = import_feed_memory.attempt_count + 1,
            consecutive_timeout_count = CASE
                WHEN excluded.consecutive_timeout_count > 0
                THEN import_feed_memory.consecutive_timeout_count + 1
                ELSE 0
            END",
        params![
            row.podcastindex_id,
            row.feed_url,
            row.podcastindex_guid,
            row.fetch_http_status.map(i64::from),
            row.fetch_outcome,
            row.outcome_reason,
            i64::from(row.retryable),
            row.raw_medium,
            row.parsed_feed_guid,
            row.attempted_at,
            row.attempt_duration_ms,
            timeout_flag,
        ],
    )
}

fn load_known_import_memory(
    conn: &Connection,
    ids: &[i64],
) -> std::collections::HashMap<i64, KnownImportMemory> {
    if ids.is_empty() {
        return std::collections::HashMap::new();
    }

    let placeholders = std::iter::repeat_n("?", ids.len())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT podcastindex_id, fetch_http_status, fetch_outcome, outcome_reason, raw_medium, parsed_feed_guid, consecutive_timeout_count
         FROM import_feed_memory
         WHERE podcastindex_id IN ({placeholders})"
    );
    let mut stmt = conn
        .prepare(&sql)
        .expect("failed to prepare import memory lookup");
    let rows = stmt
        .query_map(rusqlite::params_from_iter(ids.iter()), |row| {
            Ok((
                row.get::<_, i64>(0)?,
                KnownImportMemory {
                    fetch_http_status: row.get(1)?,
                    fetch_outcome: row.get(2)?,
                    outcome_reason: row.get(3)?,
                    raw_medium: row.get(4)?,
                    parsed_feed_guid: row.get(5)?,
                    consecutive_timeout_count: row.get(6)?,
                },
            ))
        })
        .expect("failed to query import memory");

    rows.filter_map(Result::ok).collect()
}

fn is_skip_known_non_music(raw_medium: &str) -> bool {
    !raw_medium.eq_ignore_ascii_case("music") && !raw_medium.eq_ignore_ascii_case("publisher")
}

fn is_known_medium_gate_rejection(known: &KnownImportMemory) -> bool {
    matches!(
        known.fetch_outcome.as_str(),
        "rejected" | "skipped_known_irrelevant"
    ) && known
        .outcome_reason
        .as_deref()
        .is_some_and(|reason| reason.starts_with("[medium_music]"))
}

fn is_known_successful_ingest(known: &KnownImportMemory) -> bool {
    known.fetch_http_status == Some(200)
        && matches!(
            known.fetch_outcome.as_str(),
            "accepted" | "no_change" | "skipped_known_success"
        )
}

fn known_skip_kind(
    known: Option<&KnownImportMemory>,
    skip_known_non_music: bool,
    skip_known_success: bool,
) -> Option<KnownSkipKind> {
    known.and_then(|memory| {
        if skip_known_success && is_known_successful_ingest(memory) {
            return Some(KnownSkipKind::Success);
        }

        if skip_known_non_music
            && memory.fetch_http_status == Some(200)
            && (memory
                .raw_medium
                .as_deref()
                .is_some_and(is_skip_known_non_music)
                || is_known_medium_gate_rejection(memory))
        {
            return Some(KnownSkipKind::Irrelevant);
        }

        None
    })
}

fn wavlake_host_filter_sql() -> String {
    let hosts = WAVLAKE_HOSTS
        .iter()
        .map(|host| format!("'{host}'"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("host IN ({hosts})")
}

fn non_wavlake_host_filter_sql() -> String {
    format!("NOT ({})", wavlake_host_filter_sql())
}

fn query_batch(
    db: &Connection,
    start_id: i64,
    batch_size: usize,
    scope: ImportScope,
) -> Vec<CandidateRow> {
    let sql = match scope {
        ImportScope::AllFeeds => format!(
            "SELECT id, url, podcastGuid
             FROM   podcasts
             WHERE  id > ?1
               AND  {}
             ORDER BY id ASC
             LIMIT ?2",
            non_wavlake_host_filter_sql()
        ),
        ImportScope::WavlakeOnly => format!(
            "SELECT id, url, podcastGuid
             FROM   podcasts
             WHERE  id > ?1
               AND  {}
             ORDER BY id ASC
             LIMIT ?2",
            wavlake_host_filter_sql()
        ),
    };
    let mut stmt = db.prepare_cached(&sql).expect("failed to prepare query");

    let rows: Vec<CandidateRow> = stmt
        .query_map(rusqlite::params![start_id, batch_size], |row| {
            Ok(CandidateRow {
                id: row.get(0)?,
                url: row.get(1)?,
                podcast_guid: row.get(2)?,
            })
        })
        .expect("query failed")
        .filter_map(Result::ok)
        .collect();

    interleave_by_host(rows, |row| host_key(&row.url))
}

fn ensure_parent_dir(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }
    Ok(())
}

fn wavlake_throttle_delay(report: &CrawlReport, extra_delay_secs: u64) -> Duration {
    let base_delay = if matches!(report.fetch_http_status, Some(429 | 503))
        && let CrawlOutcome::FetchError {
            retry_after_secs, ..
        } = &report.outcome
    {
        Duration::from_secs(retry_after_secs.unwrap_or(WAVLAKE_IMPORT_FALLBACK_429_BACKOFF_SECS))
    } else {
        Duration::from_millis(WAVLAKE_IMPORT_MIN_DELAY_MS)
    };

    base_delay.saturating_add(Duration::from_secs(extra_delay_secs))
}

fn set_import_active(resolverctl_bin: &str, resolver_db_path: &str, active: bool) {
    let command = if active {
        "import-active"
    } else {
        "import-idle"
    };

    match Command::new(resolverctl_bin)
        .args(["--db", resolver_db_path, command])
        .status()
    {
        Ok(status) if status.success() => {
            eprintln!(
                "import: resolver queue {} via {} --db {} {}",
                if active { "paused" } else { "resumed" },
                resolverctl_bin,
                resolver_db_path,
                command,
            );
        }
        Ok(status) => {
            eprintln!(
                "import: WARNING: resolverctl returned {status} while running `{resolverctl_bin} --db {resolver_db_path} {command}`"
            );
        }
        Err(err) => {
            eprintln!(
                "import: WARNING: failed to run `{resolverctl_bin} --db {resolver_db_path} {command}`: {err}"
            );
        }
    }
}

fn extract_snapshot_archive<R: Read>(reader: R, db_path: &Path) -> io::Result<()> {
    ensure_parent_dir(db_path)?;

    let tmp_path = db_path.with_extension("download");
    let backup_path = db_path.with_extension("backup");
    let _ = fs::remove_file(&tmp_path);
    let _ = fs::remove_file(&backup_path);
    let mut wrote_db = false;

    {
        let decoder = GzDecoder::new(reader);
        let mut archive = Archive::new(decoder);

        for entry_result in archive.entries()? {
            let mut entry = entry_result?;
            if !entry.header().entry_type().is_file() {
                continue;
            }

            let entry_path = entry.path()?;
            let Some(file_name) = entry_path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };

            if !Path::new(file_name)
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("db"))
            {
                continue;
            }

            let mut tmp_file = fs::File::create(&tmp_path)?;
            io::copy(&mut entry, &mut tmp_file)?;
            tmp_file.flush()?;
            wrote_db = true;
            break;
        }
    }

    if !wrote_db {
        let _ = fs::remove_file(&tmp_path);
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot archive did not contain a .db file",
        ));
    }

    if db_path.exists() {
        fs::rename(db_path, &backup_path)?;
        if let Err(err) = fs::rename(&tmp_path, db_path) {
            let _ = fs::rename(&backup_path, db_path);
            let _ = fs::remove_file(&tmp_path);
            return Err(err);
        }
        fs::remove_file(&backup_path)?;
    } else {
        fs::rename(&tmp_path, db_path)?;
    }
    Ok(())
}

fn download_snapshot_archive(db_url: &str, db_path: &Path) -> io::Result<()> {
    eprintln!(
        "import: downloading latest PodcastIndex snapshot from {db_url} to {}",
        db_path.display()
    );

    let response = reqwest::blocking::Client::builder()
        .connect_timeout(Duration::from_secs(SNAPSHOT_CONNECT_TIMEOUT_SECS))
        .build()
        .map_err(io::Error::other)?
        .get(db_url)
        .send()
        .and_then(reqwest::blocking::Response::error_for_status)
        .map_err(io::Error::other)?;

    extract_snapshot_archive(response, db_path)?;
    eprintln!(
        "import: snapshot ready at {} (archive not retained)",
        db_path.display()
    );
    Ok(())
}

async fn ensure_snapshot_db(db_path: &str, db_url: &str, refresh_db: bool) {
    let db_path_buf = db_path.to_owned();
    let db_url_buf = db_url.to_owned();
    let should_download = refresh_db || !Path::new(db_path).is_file();

    if !should_download {
        eprintln!("import: using existing PodcastIndex snapshot at {db_path}");
        return;
    }

    if refresh_db {
        eprintln!("import: refreshing PodcastIndex snapshot at {db_path}");
    }

    tokio::task::spawn_blocking(move || {
        download_snapshot_archive(&db_url_buf, Path::new(&db_path_buf))
    })
    .await
    .unwrap_or_else(|e| {
        eprintln!("import: snapshot download task failed: {e}");
        std::process::exit(1);
    })
    .unwrap_or_else(|e| {
        eprintln!("import: failed to download PodcastIndex snapshot: {e}");
        std::process::exit(1);
    });
}

#[allow(
    clippy::too_many_arguments,
    clippy::fn_params_excessive_bools,
    clippy::too_many_lines,
    reason = "import mode keeps batch resume, fetch, skip policy, and ingest orchestration in one async entrypoint"
)]
pub async fn run(
    db_path: String,
    db_url: String,
    refresh_db: bool,
    state_path: String,
    batch_size: usize,
    concurrency: usize,
    audit_output: Option<String>,
    audit_replace: bool,
    skip_known_non_music: bool,
    skip_known_success: bool,
    wavlake_only: bool,
    dry_run: bool,
    cursor_override: Option<i64>,
) {
    let scope = ImportScope::from_wavlake_only(wavlake_only);
    let audit_append = effective_audit_append(audit_replace, audit_output.as_deref());
    let _state_lock = if dry_run {
        None
    } else {
        let lock = acquire_pid_lock(&state_path, "import state");
        Some(PidLockGuard { path: lock })
    };
    let progress = ProgressStore::open(&state_path);
    let state_writer = (!dry_run).then(|| ImportStateWriter::spawn(&state_path));
    let audit_writer = if dry_run {
        None
    } else {
        audit_output
            .as_deref()
            .map(|path| ImportAuditWriter::spawn(path, audit_append))
    };

    ensure_snapshot_db(&db_path, &db_url, refresh_db).await;

    let mut cursor = cursor_override.unwrap_or_else(|| progress.get_last_id(scope));
    if let Some(override_cursor) = cursor_override {
        eprintln!("import: cursor override to {override_cursor}");
        if dry_run {
            eprintln!("import: dry-run leaves stored cursor unchanged");
        } else {
            progress.set_last_id(scope, override_cursor);
        }
    }

    let effective_concurrency = scope.effective_concurrency(concurrency);
    if scope == ImportScope::WavlakeOnly && !dry_run && concurrency != effective_concurrency {
        eprintln!(
            "import: wavlake_only forces single-flight fetches; requested concurrency={concurrency}, effective_concurrency={effective_concurrency}"
        );
    }
    if !dry_run && audit_output.is_some() && audit_append {
        eprintln!("import: audit_output will append with feed_guid+content_sha256 dedupe");
    }

    eprintln!(
        "import: starting from id={cursor}, batch={batch_size}, concurrency={effective_concurrency}, fetch_timeout={IMPORT_FETCH_TIMEOUT_SECS}s, skip_known_non_music={skip_known_non_music}, skip_known_success={skip_known_success}, scope={}, cursor_key={}, audit_output={}, audit_append={audit_append}",
        scope.label(),
        scope.cursor_key(),
        audit_output.as_deref().unwrap_or("(off)")
    );

    let pi_db = Connection::open_with_flags(
        &db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .unwrap_or_else(|e| {
        eprintln!("import: failed to open PodcastIndex DB at {db_path}: {e}");
        std::process::exit(1);
    });

    let config = Arc::new(if dry_run {
        CrawlConfig::dry_run(
            "stophammer-crawler/0.1 (dry-run)",
            std::time::Duration::from_secs(IMPORT_FETCH_TIMEOUT_SECS),
        )
    } else {
        let mut config = CrawlConfig::from_env();
        config.fetch_timeout = std::time::Duration::from_secs(IMPORT_FETCH_TIMEOUT_SECS);
        config
    });

    let _resolver_guard = if dry_run {
        None
    } else {
        ResolverImportGuard::maybe_activate()
    };

    let client = Arc::new(reqwest::Client::new());
    let wavlake_throttle =
        (scope == ImportScope::WavlakeOnly && !dry_run).then(|| Arc::new(WavlakeThrottle::new()));
    let mut total_processed: u64 = 0;

    loop {
        let batch = query_batch(&pi_db, cursor, batch_size, scope);
        if batch.is_empty() {
            eprintln!("import: no more candidates after id={cursor}");
            break;
        }

        let batch_max_id = batch.last().map_or(cursor, |r| r.id);
        let batch_len = batch.len();
        let known_memory = if skip_known_non_music || skip_known_success {
            progress.known_memory_for_ids(&batch.iter().map(|row| row.id).collect::<Vec<_>>())
        } else {
            std::collections::HashMap::default()
        };

        if dry_run {
            for row in &batch {
                let guid_display = row.podcast_guid.as_deref().unwrap_or("(none)");
                if let Some(skip_kind) = known_skip_kind(
                    known_memory.get(&row.id),
                    skip_known_non_music,
                    skip_known_success,
                ) {
                    eprintln!(
                        "  [dry-run] would_{} id={} guid={guid_display} {}",
                        skip_kind.label(),
                        row.id,
                        row.url
                    );
                } else {
                    eprintln!("  [dry-run] id={} guid={guid_display} {}", row.id, row.url);
                }
            }
        } else {
            let accepted = Arc::new(AtomicU64::new(0));
            let rejected = Arc::new(AtomicU64::new(0));
            let no_change = Arc::new(AtomicU64::new(0));
            let errors = Arc::new(AtomicU64::new(0));
            let skipped = Arc::new(AtomicU64::new(0));
            let active_tasks = Arc::new(Mutex::new(BTreeMap::new()));
            let (heartbeat_stop_tx, heartbeat_stop_rx) = watch::channel(false);
            let heartbeat_handle = tokio::spawn(run_import_batch_heartbeat(
                cursor,
                batch_max_id,
                batch_len,
                Arc::clone(&active_tasks),
                heartbeat_stop_rx,
            ));
            let state_tx = state_writer
                .as_ref()
                .expect("import state writer missing")
                .tx
                .clone();
            let audit_tx = audit_writer.as_ref().map(|writer| writer.tx.clone());
            let known_memory = Arc::new(known_memory);

            let tasks: Vec<_> = batch
                .into_iter()
                .map(|row| {
                    let client = Arc::clone(&client);
                    let config = Arc::clone(&config);
                    let accepted = Arc::clone(&accepted);
                    let rejected = Arc::clone(&rejected);
                    let no_change = Arc::clone(&no_change);
                    let errors = Arc::clone(&errors);
                    let skipped = Arc::clone(&skipped);
                    let state_tx = state_tx.clone();
                    let audit_tx = audit_tx.clone();
                    let active_tasks = Arc::clone(&active_tasks);
                    let known_memory = Arc::clone(&known_memory);
                    let wavlake_throttle = wavlake_throttle.as_ref().map(Arc::clone);
                    move || async move {
                        let attempted_at = Utc::now().timestamp();
                        let task_started_at = Instant::now();
                        if let Some(skip_kind) = known_skip_kind(
                            known_memory.get(&row.id),
                            skip_known_non_music,
                            skip_known_success,
                        ) {
                            skipped.fetch_add(1, Ordering::Relaxed);
                            let memory_row = ImportMemoryRow::skipped_known(
                                &row,
                                known_memory
                                    .get(&row.id)
                                    .expect("known memory missing for skipped row"),
                                attempted_at,
                                i64::try_from(task_started_at.elapsed().as_millis())
                                    .expect("skip attempt duration exceeded i64"),
                                skip_kind,
                            );
                            enqueue_import_memory(&state_tx, memory_row);
                            eprintln!("  {}: id={} {}", skip_kind.label(), row.id, row.url);
                            return;
                        }

                        if let Some(known) = known_memory.get(&row.id)
                            && known.consecutive_timeout_count >= IMPORT_MAX_CONSECUTIVE_TIMEOUTS
                        {
                            skipped.fetch_add(1, Ordering::Relaxed);
                            eprintln!(
                                "  skip_timeout_exhausted: id={} {} ({} consecutive timeouts)",
                                row.id, row.url, known.consecutive_timeout_count,
                            );
                            return;
                        }

                        let _active_guard = begin_active_import_task(&active_tasks, &row);
                        if let Some(throttle) = &wavlake_throttle {
                            throttle.wait_for_turn(&row).await;
                        }
                        let fallback = row.podcast_guid.as_deref();
                        let report = if let Ok(report) = tokio::time::timeout(
                            Duration::from_secs(IMPORT_TASK_HARD_TIMEOUT_SECS),
                            crawl_feed_report(&client, &row.url, fallback, &config),
                        )
                        .await
                        {
                            report
                        } else {
                            eprintln!(
                                "  import: ERROR hard timeout after {}s for id={} {}",
                                IMPORT_TASK_HARD_TIMEOUT_SECS, row.id, row.url,
                            );
                            build_import_timeout_report(IMPORT_TASK_HARD_TIMEOUT_SECS)
                        };
                        if let Some(throttle) = &wavlake_throttle {
                            throttle.record_attempt(&row, &report).await;
                        }
                        let outcome = &report.outcome;
                        let memory_row = ImportMemoryRow::from_crawl_report(
                            &row,
                            &report,
                            attempted_at,
                            i64::try_from(task_started_at.elapsed().as_millis())
                                .expect("attempt duration exceeded i64"),
                        );
                        enqueue_import_memory(&state_tx, memory_row);
                        if let Some(audit_tx) = &audit_tx
                            && let Some(audit_row) =
                                build_import_audit_row(&row, &report, attempted_at)
                        {
                            enqueue_import_audit(audit_tx, audit_row);
                        }

                        match outcome {
                            CrawlOutcome::Accepted { .. } => {
                                accepted.fetch_add(1, Ordering::Relaxed);
                            }
                            CrawlOutcome::Rejected { .. } => {
                                rejected.fetch_add(1, Ordering::Relaxed);
                            }
                            CrawlOutcome::NoChange => {
                                no_change.fetch_add(1, Ordering::Relaxed);
                            }
                            CrawlOutcome::FetchError { .. }
                            | CrawlOutcome::ParseError(_)
                            | CrawlOutcome::IngestError { .. } => {
                                errors.fetch_add(1, Ordering::Relaxed);
                            }
                        }

                        eprintln!("  {outcome}: id={} {}", row.id, row.url);
                    }
                })
                .collect();

            run_pool(tasks, effective_concurrency).await;
            let _ = heartbeat_stop_tx.send(true);
            heartbeat_handle
                .await
                .expect("import batch heartbeat task panicked");

            eprintln!(
                "import: batch done — accepted={} rejected={} no_change={} errors={} skipped={}",
                accepted.load(Ordering::Relaxed),
                rejected.load(Ordering::Relaxed),
                no_change.load(Ordering::Relaxed),
                errors.load(Ordering::Relaxed),
                skipped.load(Ordering::Relaxed),
            );
        }

        cursor = batch_max_id;
        if let Some(writer) = &state_writer {
            writer.set_last_id(scope, cursor).await;
        }
        total_processed += batch_len as u64;
        eprintln!("import: cursor={cursor} total_processed={total_processed}");
    }

    if let Some(writer) = state_writer {
        writer.shutdown().await;
    }
    if let Some(writer) = audit_writer {
        writer.shutdown().await;
    }

    eprintln!("import: complete — {total_processed} feeds processed");
}

#[cfg(test)]
mod tests {
    use super::{
        ImportAuditFetch, ImportAuditRow, ImportAuditSourceDb, ImportAuditWriter, ImportMemoryRow,
        ImportScope, ImportStateWriter, KnownSkipKind, ProgressStore, WAVLAKE_HOSTS,
        build_import_audit_row, build_import_timeout_report, effective_audit_append,
        enqueue_import_audit, enqueue_import_memory, extract_snapshot_archive,
        is_skip_known_non_music, known_skip_kind, load_known_import_memory, open_state_connection,
        query_batch, upsert_import_memory, wavlake_throttle_delay,
    };
    use crate::crawl::{CrawlOutcome, CrawlReport};
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use rusqlite::Connection;
    use std::fs;
    use std::io::{self, Cursor};
    use std::time::Duration;
    use stophammer_parser::types::IngestFeedData;
    use tar::{Builder, Header};
    use tempfile::tempdir;

    fn make_snapshot_archive(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let gz = GzEncoder::new(Vec::new(), Compression::default());
        let mut tar = Builder::new(gz);

        for (name, contents) in entries {
            let mut header = Header::new_gnu();
            header.set_path(name).expect("path");
            header.set_size(contents.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            tar.append(&header, Cursor::new(*contents)).expect("append");
        }

        tar.into_inner()
            .expect("tar into inner")
            .finish()
            .expect("finish")
    }

    fn sample_memory_row(
        podcastindex_id: i64,
        fetch_http_status: Option<u16>,
        fetch_outcome: &str,
        outcome_reason: Option<&str>,
        retryable: bool,
        raw_medium: Option<&str>,
    ) -> ImportMemoryRow {
        ImportMemoryRow {
            podcastindex_id,
            feed_url: format!("https://example.com/{podcastindex_id}.xml"),
            podcastindex_guid: Some(format!("pi-guid-{podcastindex_id}")),
            fetch_http_status,
            fetch_outcome: fetch_outcome.to_string(),
            outcome_reason: outcome_reason.map(ToOwned::to_owned),
            retryable,
            raw_medium: raw_medium.map(ToOwned::to_owned),
            parsed_feed_guid: Some(format!("feed-guid-{podcastindex_id}")),
            attempted_at: 1_700_000_000 + podcastindex_id,
            attempt_duration_ms: 1_000 + podcastindex_id,
        }
    }

    fn sample_candidate_row() -> super::CandidateRow {
        super::CandidateRow {
            id: 4630863,
            url: "https://example.com/feed.xml".to_string(),
            podcast_guid: Some("pi-guid".to_string()),
        }
    }

    fn sample_parsed_feed() -> IngestFeedData {
        IngestFeedData {
            feed_guid: "feed-guid".to_string(),
            title: "Feed Title".to_string(),
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

    fn sample_audit_report() -> CrawlReport {
        CrawlReport {
            outcome: CrawlOutcome::Accepted {
                warnings: Vec::new(),
            },
            fetch_http_status: Some(200),
            raw_medium: Some("music".to_string()),
            parsed_feed_guid: Some("feed-guid".to_string()),
            final_url: Some("https://cdn.example.com/final.xml".to_string()),
            content_sha256: Some("abc123".to_string()),
            raw_xml: Some("<rss/>".to_string()),
            parsed_feed: Some(sample_parsed_feed()),
        }
    }

    #[test]
    fn extract_snapshot_archive_writes_db_file() {
        let tempdir = tempdir().expect("tempdir");
        let db_path = tempdir.path().join("podcastindex_feeds.db");
        let archive = make_snapshot_archive(&[
            ("README.txt", b"ignore me"),
            ("podcastindex_feeds.db", b"sqlite bytes"),
        ]);

        extract_snapshot_archive(Cursor::new(archive), &db_path).expect("extract archive");

        assert_eq!(fs::read(&db_path).expect("read db"), b"sqlite bytes");
        assert!(!db_path.with_extension("download").exists());
    }

    #[test]
    fn extract_snapshot_archive_replaces_existing_db() {
        let tempdir = tempdir().expect("tempdir");
        let db_path = tempdir.path().join("podcastindex_feeds.db");
        fs::write(&db_path, b"old sqlite bytes").expect("seed old db");
        let archive = make_snapshot_archive(&[("podcastindex_feeds.db", b"new sqlite bytes")]);

        extract_snapshot_archive(Cursor::new(archive), &db_path).expect("replace archive");

        assert_eq!(fs::read(&db_path).expect("read db"), b"new sqlite bytes");
        assert!(!db_path.with_extension("download").exists());
        assert!(!db_path.with_extension("backup").exists());
    }

    #[test]
    fn extract_snapshot_archive_rejects_missing_db_entry() {
        let tempdir = tempdir().expect("tempdir");
        let db_path = tempdir.path().join("podcastindex_feeds.db");
        let archive = make_snapshot_archive(&[("README.txt", b"ignore me")]);

        let err = extract_snapshot_archive(Cursor::new(archive), &db_path).expect_err("missing db");

        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(!db_path.exists());
    }

    #[test]
    fn progress_store_creates_import_memory_table() {
        let tempdir = tempdir().expect("tempdir");
        let state_path = tempdir.path().join("import_state.db");
        let store = ProgressStore::open(state_path.to_str().expect("utf-8 path"));

        let table_exists: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'import_feed_memory'",
                [],
                |row| row.get(0),
            )
            .expect("query sqlite_master");

        assert_eq!(
            table_exists, 1,
            "expected import_feed_memory table to exist"
        );
    }

    #[test]
    fn progress_store_adds_attempt_duration_column() {
        let tempdir = tempdir().expect("tempdir");
        let state_path = tempdir.path().join("import_state.db");
        let store = ProgressStore::open(state_path.to_str().expect("utf-8 path"));

        let has_attempt_duration_ms = store
            .conn
            .prepare("PRAGMA table_info(import_feed_memory)")
            .expect("prepare pragma")
            .query_map([], |row| row.get::<_, String>(1))
            .expect("query pragma")
            .filter_map(Result::ok)
            .any(|column| column == "attempt_duration_ms");

        assert!(
            has_attempt_duration_ms,
            "expected import_feed_memory to expose attempt_duration_ms"
        );
    }

    #[test]
    fn upsert_import_memory_stores_status_and_medium() {
        let tempdir = tempdir().expect("tempdir");
        let state_path = tempdir.path().join("import_state.db");
        let conn = open_state_connection(state_path.to_str().expect("utf-8 path"));
        let row = sample_memory_row(4630863, Some(200), "accepted", None, false, Some("music"));

        upsert_import_memory(&conn, &row).expect("upsert import memory");

        let stored: (Option<u16>, String, Option<String>, i64) = conn
            .query_row(
                "SELECT fetch_http_status, fetch_outcome, raw_medium, attempt_duration_ms
                 FROM import_feed_memory
                 WHERE podcastindex_id = ?1",
                [row.podcastindex_id],
                |db_row| {
                    Ok((
                        db_row.get(0)?,
                        db_row.get(1)?,
                        db_row.get(2)?,
                        db_row.get(3)?,
                    ))
                },
            )
            .expect("query stored import memory");

        assert_eq!(stored.0, Some(200));
        assert_eq!(stored.1, "accepted");
        assert_eq!(stored.2.as_deref(), Some("music"));
        assert_eq!(stored.3, row.attempt_duration_ms);
    }

    #[test]
    fn upsert_import_memory_increments_attempt_count() {
        let tempdir = tempdir().expect("tempdir");
        let state_path = tempdir.path().join("import_state.db");
        let conn = open_state_connection(state_path.to_str().expect("utf-8 path"));
        let first = sample_memory_row(4630863, Some(404), "fetch_error", None, false, None);
        let second = sample_memory_row(4630863, Some(429), "fetch_error", None, true, None);

        upsert_import_memory(&conn, &first).expect("first upsert");
        upsert_import_memory(&conn, &second).expect("second upsert");

        let stored: (Option<u16>, i64, i64) = conn
            .query_row(
                "SELECT fetch_http_status, attempt_count, attempt_duration_ms
                 FROM import_feed_memory
                 WHERE podcastindex_id = ?1",
                [first.podcastindex_id],
                |db_row| Ok((db_row.get(0)?, db_row.get(1)?, db_row.get(2)?)),
            )
            .expect("query attempt count");

        assert_eq!(stored.0, Some(429));
        assert_eq!(stored.1, 2, "expected retry to increment attempt_count");
        assert_eq!(stored.2, second.attempt_duration_ms);
    }

    #[test]
    fn known_memory_lookup_supports_skip_mode() {
        let tempdir = tempdir().expect("tempdir");
        let state_path = tempdir.path().join("import_state.db");
        let conn = open_state_connection(state_path.to_str().expect("utf-8 path"));
        let known_non_music =
            sample_memory_row(10, Some(200), "accepted", None, false, Some("podcast"));
        let known_publisher =
            sample_memory_row(11, Some(200), "accepted", None, false, Some("publisher"));
        let known_medium_absent = sample_memory_row(
            12,
            Some(200),
            "rejected",
            Some("[medium_music] podcast:medium absent — must be 'music' or 'publisher'"),
            false,
            None,
        );
        let unrelated_rejection = sample_memory_row(
            13,
            Some(200),
            "rejected",
            Some("[other_gate] some other reason"),
            false,
            None,
        );

        upsert_import_memory(&conn, &known_non_music).expect("upsert non-music row");
        upsert_import_memory(&conn, &known_publisher).expect("upsert publisher row");
        upsert_import_memory(&conn, &known_medium_absent).expect("upsert medium-absent row");
        upsert_import_memory(&conn, &unrelated_rejection).expect("upsert unrelated rejection");

        let lookup = load_known_import_memory(&conn, &[10, 11, 12, 13, 14]);

        assert!(known_skip_kind(lookup.get(&10), true, false).is_some());
        assert!(known_skip_kind(lookup.get(&11), true, false).is_none());
        assert!(known_skip_kind(lookup.get(&12), true, false).is_some());
        assert!(known_skip_kind(lookup.get(&13), true, false).is_none());
        assert!(known_skip_kind(lookup.get(&14), true, false).is_none());
    }

    #[test]
    fn medium_gate_rejections_with_absent_medium_are_skipped() {
        let tempdir = tempdir().expect("tempdir");
        let state_path = tempdir.path().join("import_state.db");
        let conn = open_state_connection(state_path.to_str().expect("utf-8 path"));
        let known_medium_absent = sample_memory_row(
            22,
            Some(200),
            "rejected",
            Some("[medium_music] podcast:medium absent — must be 'music' or 'publisher'"),
            false,
            None,
        );

        upsert_import_memory(&conn, &known_medium_absent).expect("upsert medium-absent row");

        let lookup = load_known_import_memory(&conn, &[22]);

        assert!(known_skip_kind(lookup.get(&22), true, false).is_some());
    }

    #[test]
    fn skipped_medium_gate_rows_remain_skippable_on_future_runs() {
        let tempdir = tempdir().expect("tempdir");
        let state_path = tempdir.path().join("import_state.db");
        let conn = open_state_connection(state_path.to_str().expect("utf-8 path"));
        let skipped_medium_absent = sample_memory_row(
            23,
            Some(200),
            "skipped_known_irrelevant",
            Some("[medium_music] podcast:medium absent — must be 'music' or 'publisher'"),
            false,
            None,
        );

        upsert_import_memory(&conn, &skipped_medium_absent).expect("upsert skipped row");

        let lookup = load_known_import_memory(&conn, &[23]);

        assert!(known_skip_kind(lookup.get(&23), true, false).is_some());
        assert_eq!(
            known_skip_kind(lookup.get(&23), true, false),
            Some(KnownSkipKind::Irrelevant)
        );
    }

    #[test]
    fn skip_helper_treats_music_and_publisher_as_accepted() {
        assert!(!is_skip_known_non_music("music"));
        assert!(!is_skip_known_non_music("publisher"));
        assert!(is_skip_known_non_music("podcast"));
    }

    #[tokio::test]
    async fn state_writer_persists_memory_and_cursor() {
        let tempdir = tempdir().expect("tempdir");
        let state_path = tempdir.path().join("import_state.db");
        let writer = ImportStateWriter::spawn(state_path.to_str().expect("utf-8 path"));
        let row = sample_memory_row(99, Some(429), "fetch_error", None, true, None);

        enqueue_import_memory(&writer.tx, row.clone());
        writer.set_last_id(ImportScope::AllFeeds, 99).await;
        writer.shutdown().await;

        let conn = open_state_connection(state_path.to_str().expect("utf-8 path"));
        let stored_attempts: i64 = conn
            .query_row(
                "SELECT attempt_count FROM import_feed_memory WHERE podcastindex_id = ?1",
                [row.podcastindex_id],
                |db_row| db_row.get(0),
            )
            .expect("query persisted memory row");
        let cursor: String = conn
            .query_row(
                "SELECT value FROM import_progress WHERE key = 'last_processed_id'",
                [],
                |db_row| db_row.get(0),
            )
            .or_else(|_| {
                conn.query_row(
                    "SELECT value FROM import_progress WHERE key = ?1",
                    [ImportScope::AllFeeds.cursor_key()],
                    |db_row| db_row.get(0),
                )
            })
            .expect("query persisted cursor");

        assert_eq!(stored_attempts, 1);
        assert_eq!(cursor, "99");
    }

    #[test]
    fn build_import_audit_row_uses_feed_audit_shape_for_200_fetches() {
        let candidate = sample_candidate_row();
        let report = sample_audit_report();

        let row = build_import_audit_row(&candidate, &report, 1_700_000_000)
            .expect("expected audit row for 200 fetch");

        assert_eq!(row.source_db.feed_guid, "feed-guid");
        assert_eq!(row.source_db.feed_url, "https://example.com/feed.xml");
        assert_eq!(row.source_db.title, "Feed Title");
        assert_eq!(
            row.fetch.final_url.as_deref(),
            Some("https://cdn.example.com/final.xml")
        );
        assert_eq!(row.fetch.http_status, Some(200));
        assert_eq!(row.fetch.content_sha256.as_deref(), Some("abc123"));
        assert_eq!(row.raw_xml, "<rss/>");
        assert!(row.parsed_feed.is_some());
        assert_eq!(row.parse_error, None);
    }

    #[test]
    fn build_import_audit_row_skips_parse_errors_for_200_bodies() {
        let candidate = sample_candidate_row();
        let report = CrawlReport {
            outcome: CrawlOutcome::ParseError("invalid xml".to_string()),
            fetch_http_status: Some(200),
            raw_medium: None,
            parsed_feed_guid: None,
            final_url: Some("https://example.com/feed.xml".to_string()),
            content_sha256: Some("abc123".to_string()),
            raw_xml: Some("<rss/>".to_string()),
            parsed_feed: None,
        };

        assert!(build_import_audit_row(&candidate, &report, 1_700_000_000).is_none());
    }

    #[test]
    fn build_import_audit_row_skips_non_200_fetches() {
        let candidate = sample_candidate_row();
        let report = CrawlReport {
            outcome: CrawlOutcome::FetchError {
                reason: "http 404 Not Found".to_string(),
                retryable: false,
                retry_after_secs: None,
            },
            fetch_http_status: Some(404),
            raw_medium: None,
            parsed_feed_guid: None,
            final_url: Some("https://example.com/feed.xml".to_string()),
            content_sha256: None,
            raw_xml: None,
            parsed_feed: None,
        };

        assert!(build_import_audit_row(&candidate, &report, 1_700_000_000).is_none());
    }

    #[test]
    fn build_import_audit_row_keeps_no_change_feeds() {
        let candidate = sample_candidate_row();
        let mut report = sample_audit_report();
        report.outcome = CrawlOutcome::NoChange;

        let row = build_import_audit_row(&candidate, &report, 1_700_000_000)
            .expect("expected audit row for no_change");

        assert_eq!(row.fetch.error, None);
        assert_eq!(row.parse_error, None);
    }

    #[tokio::test]
    async fn audit_writer_persists_ndjson_rows() {
        let tempdir = tempdir().expect("tempdir");
        let output_path = tempdir.path().join("feed_audit.ndjson");
        let writer = ImportAuditWriter::spawn(output_path.to_str().expect("utf-8 path"), false);
        let row = ImportAuditRow {
            source_db: ImportAuditSourceDb {
                feed_guid: "feed-guid".to_string(),
                feed_url: "https://example.com/feed.xml".to_string(),
                title: "Feed Title".to_string(),
            },
            fetched_at: 1_700_000_000,
            fetch: ImportAuditFetch {
                final_url: Some("https://example.com/feed.xml".to_string()),
                http_status: Some(200),
                content_sha256: Some("abc123".to_string()),
                error: None,
            },
            raw_xml: "<rss/>".to_string(),
            parsed_feed: None,
            podcast_namespace: None,
            parse_error: None,
        };

        enqueue_import_audit(&writer.tx, row);
        writer.shutdown().await;

        let written = fs::read_to_string(output_path).expect("read audit output");
        assert!(written.contains("\"raw_xml\":\"<rss/>\""));
        assert!(written.contains("\"feed_guid\":\"feed-guid\""));
    }

    #[test]
    #[should_panic(expected = "held by live process")]
    fn audit_writer_refuses_second_writer_for_same_path() {
        let tempdir = tempdir().expect("tempdir");
        let output_path = tempdir.path().join("feed_audit.ndjson");
        let _first = ImportAuditWriter::spawn(output_path.to_str().expect("utf-8 path"), true);
        let _second = ImportAuditWriter::spawn(output_path.to_str().expect("utf-8 path"), true);
    }

    #[tokio::test]
    async fn audit_writer_append_dedupes_existing_rows_by_feed_guid_and_hash() {
        let tempdir = tempdir().expect("tempdir");
        let output_path = tempdir.path().join("feed_audit.ndjson");
        fs::write(
            &output_path,
            "{\"source_db\":{\"feed_guid\":\"feed-guid\"},\"fetch\":{\"content_sha256\":\"abc123\"}}\n",
        )
        .expect("seed existing audit file");

        let writer = ImportAuditWriter::spawn(output_path.to_str().expect("utf-8 path"), true);
        let duplicate_row = ImportAuditRow {
            source_db: ImportAuditSourceDb {
                feed_guid: "feed-guid".to_string(),
                feed_url: "https://example.com/feed.xml".to_string(),
                title: "Feed Title".to_string(),
            },
            fetched_at: 1_700_000_000,
            fetch: ImportAuditFetch {
                final_url: Some("https://example.com/feed.xml".to_string()),
                http_status: Some(200),
                content_sha256: Some("abc123".to_string()),
                error: None,
            },
            raw_xml: "<rss/>".to_string(),
            parsed_feed: None,
            podcast_namespace: None,
            parse_error: None,
        };
        let distinct_row = ImportAuditRow {
            source_db: ImportAuditSourceDb {
                feed_guid: "feed-guid".to_string(),
                feed_url: "https://example.com/feed.xml".to_string(),
                title: "Feed Title".to_string(),
            },
            fetched_at: 1_700_000_001,
            fetch: ImportAuditFetch {
                final_url: Some("https://example.com/feed.xml".to_string()),
                http_status: Some(200),
                content_sha256: Some("def456".to_string()),
                error: None,
            },
            raw_xml: "<rss version=\"2.0\"/>".to_string(),
            parsed_feed: None,
            podcast_namespace: None,
            parse_error: None,
        };

        enqueue_import_audit(&writer.tx, duplicate_row);
        enqueue_import_audit(&writer.tx, distinct_row);
        writer.shutdown().await;

        let written = fs::read_to_string(output_path).expect("read audit output");
        assert_eq!(written.lines().count(), 2);
        assert!(written.contains("\"content_sha256\":\"abc123\""));
        assert!(written.contains("\"content_sha256\":\"def456\""));
    }

    #[test]
    fn build_import_timeout_report_marks_row_retryable() {
        let report = build_import_timeout_report(15);

        assert_eq!(report.fetch_http_status, None);
        assert_eq!(report.raw_medium, None);
        assert!(report.is_retryable());
        assert_eq!(report.outcome.label(), "fetch_error");
        assert_eq!(
            report.outcome.reason(),
            Some("import crawl exceeded hard deadline of 15s; marking row as failed")
        );
    }

    #[test]
    fn query_batch_wavlake_only_filters_snapshot_hosts() {
        let conn = Connection::open_in_memory().expect("open memory db");
        conn.execute_batch(
            "CREATE TABLE podcasts (
                id INTEGER PRIMARY KEY,
                url TEXT NOT NULL,
                host TEXT NOT NULL,
                podcastGuid TEXT NOT NULL
            );
            INSERT INTO podcasts (id, url, host, podcastGuid) VALUES
                (1, 'https://example.com/feed.xml', 'example.com', 'guid-1'),
                (2, 'https://wavlake.com/feed/music/abc', 'wavlake.com', 'guid-2'),
                (3, 'https://www.wavlake.com/feed/music/def', 'www.wavlake.com', 'guid-3'),
                (4, 'https://other.example/feed.xml', 'other.example', 'guid-4');",
        )
        .expect("seed podcasts");

        let wavlake_rows = query_batch(&conn, 0, 10, ImportScope::WavlakeOnly);
        let all_rows = query_batch(&conn, 0, 10, ImportScope::AllFeeds);

        assert_eq!(all_rows.len(), 2);
        assert_eq!(wavlake_rows.len(), 2);
        assert!(
            wavlake_rows
                .iter()
                .all(|row| { WAVLAKE_HOSTS.iter().any(|host| row.url.contains(host)) })
        );
        assert!(
            all_rows
                .iter()
                .all(|row| { WAVLAKE_HOSTS.iter().all(|host| !row.url.contains(host)) })
        );
    }

    #[test]
    fn wavlake_scope_forces_single_flight_fetches() {
        assert_eq!(ImportScope::AllFeeds.effective_concurrency(5), 5);
        assert_eq!(ImportScope::WavlakeOnly.effective_concurrency(5), 1);
    }

    #[test]
    fn wavlake_scope_skips_prior_successful_ingests() {
        let tempdir = tempdir().expect("tempdir");
        let state_path = tempdir.path().join("import_state.db");
        let conn = open_state_connection(state_path.to_str().expect("utf-8 path"));
        let accepted = sample_memory_row(50, Some(200), "accepted", None, false, Some("music"));
        let no_change = sample_memory_row(51, Some(200), "no_change", None, false, Some("music"));
        let fetch_error = sample_memory_row(52, Some(429), "fetch_error", None, true, None);

        upsert_import_memory(&conn, &accepted).expect("upsert accepted row");
        upsert_import_memory(&conn, &no_change).expect("upsert no_change row");
        upsert_import_memory(&conn, &fetch_error).expect("upsert fetch_error row");

        let lookup = load_known_import_memory(&conn, &[50, 51, 52]);

        assert_eq!(
            known_skip_kind(lookup.get(&50), false, true),
            Some(KnownSkipKind::Success)
        );
        assert_eq!(
            known_skip_kind(lookup.get(&51), false, true),
            Some(KnownSkipKind::Success)
        );
        assert!(known_skip_kind(lookup.get(&52), false, true).is_none());
    }

    #[test]
    fn skipped_success_rows_remain_skippable_when_success_skipping_is_enabled() {
        let tempdir = tempdir().expect("tempdir");
        let state_path = tempdir.path().join("import_state.db");
        let conn = open_state_connection(state_path.to_str().expect("utf-8 path"));
        let skipped_success = sample_memory_row(
            60,
            Some(200),
            "skipped_known_success",
            None,
            false,
            Some("music"),
        );

        upsert_import_memory(&conn, &skipped_success).expect("upsert skipped success row");

        let lookup = load_known_import_memory(&conn, &[60]);

        assert_eq!(
            known_skip_kind(lookup.get(&60), false, true),
            Some(KnownSkipKind::Success)
        );
        assert!(known_skip_kind(lookup.get(&60), false, false).is_none());
    }

    #[test]
    fn progress_store_scopes_cursors_by_import_scope() {
        let tempdir = tempdir().expect("tempdir");
        let state_path = tempdir.path().join("import_state.db");
        let progress = ProgressStore::open(state_path.to_str().expect("utf-8 path"));

        progress.set_last_id(ImportScope::AllFeeds, 111);
        progress.set_last_id(ImportScope::WavlakeOnly, 222);

        assert_eq!(progress.get_last_id(ImportScope::AllFeeds), 111);
        assert_eq!(progress.get_last_id(ImportScope::WavlakeOnly), 222);
    }

    #[test]
    fn audit_output_appends_by_default_unless_replaced() {
        assert!(effective_audit_append(
            false,
            Some("./feed_audit_wavlake.ndjson")
        ));
        assert!(effective_audit_append(false, Some("./feed_audit.ndjson")));
        assert!(!effective_audit_append(true, Some("./feed_audit.ndjson")));
        assert!(!effective_audit_append(false, None));
    }

    #[test]
    fn wavlake_throttle_respects_retry_after_for_429s() {
        let report = CrawlReport {
            outcome: CrawlOutcome::FetchError {
                reason: "http 429 Too Many Requests".to_string(),
                retryable: true,
                retry_after_secs: Some(300),
            },
            fetch_http_status: Some(429),
            raw_medium: None,
            parsed_feed_guid: None,
            final_url: None,
            content_sha256: None,
            raw_xml: None,
            parsed_feed: None,
        };

        assert_eq!(wavlake_throttle_delay(&report, 1), Duration::from_secs(301));
    }

    #[test]
    fn wavlake_throttle_uses_floor_delay_for_non_429_rows() {
        let report = CrawlReport {
            outcome: CrawlOutcome::NoChange,
            fetch_http_status: Some(200),
            raw_medium: None,
            parsed_feed_guid: None,
            final_url: None,
            content_sha256: None,
            raw_xml: None,
            parsed_feed: None,
        };

        assert_eq!(
            wavlake_throttle_delay(&report, 2),
            Duration::from_millis(super::WAVLAKE_IMPORT_MIN_DELAY_MS)
                .saturating_add(Duration::from_secs(2))
        );
    }
}
