use std::fs;
use std::io::{self, Read, Write};
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use chrono::Utc;
use flate2::read::GzDecoder;
use rusqlite::{Connection, params};
use tar::Archive;
use tokio::sync::oneshot;

use crate::crawl::{CrawlConfig, CrawlOutcome, CrawlReport, crawl_feed_report};
use crate::pool::run_pool;
use crate::url_queue::{host_key, interleave_by_host};

const IMPORT_FETCH_ATTEMPTS: u32 = 2;
const IMPORT_FETCH_TIMEOUT_SECS: u64 = 5;
const SNAPSHOT_CONNECT_TIMEOUT_SECS: u64 = 30;
const RESOLVERCTL_BIN_ENV: &str = "RESOLVERCTL_BIN";
const RESOLVER_DB_PATH_ENV: &str = "RESOLVER_DB_PATH";
const RESOLVER_HEARTBEAT_INTERVAL_SECS: u64 = 60;

struct CandidateRow {
    id: i64,
    url: String,
    podcast_guid: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct KnownImportMemory {
    fetch_http_status: Option<u16>,
    raw_medium: Option<String>,
    parsed_feed_guid: Option<String>,
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
}

impl ImportMemoryRow {
    fn from_crawl_report(
        candidate: &CandidateRow,
        report: &CrawlReport,
        attempted_at: i64,
    ) -> Self {
        Self {
            podcastindex_id: candidate.id,
            feed_url: candidate.url.clone(),
            podcastindex_guid: candidate.podcast_guid.clone(),
            fetch_http_status: report.fetch_http_status,
            fetch_outcome: report.outcome.label().to_string(),
            outcome_reason: report.outcome.reason().map(ToOwned::to_owned),
            retryable: report.is_retryable(),
            raw_medium: report.raw_medium.clone(),
            parsed_feed_guid: report.parsed_feed_guid.clone(),
            attempted_at,
        }
    }

    fn skipped_known_irrelevant(
        candidate: &CandidateRow,
        known: &KnownImportMemory,
        attempted_at: i64,
    ) -> Self {
        let outcome_reason = known.raw_medium.as_ref().map_or_else(
            || Some("known irrelevant medium".to_string()),
            |raw_medium| Some(format!("known raw_medium={raw_medium}")),
        );

        Self {
            podcastindex_id: candidate.id,
            feed_url: candidate.url.clone(),
            podcastindex_guid: candidate.podcast_guid.clone(),
            fetch_http_status: known.fetch_http_status,
            fetch_outcome: "skipped_known_irrelevant".to_string(),
            outcome_reason,
            retryable: false,
            raw_medium: known.raw_medium.clone(),
            parsed_feed_guid: known.parsed_feed_guid.clone(),
            attempted_at,
        }
    }
}

enum ImportStateCommand {
    UpsertMemory(ImportMemoryRow),
    SetLastId { id: i64, ack: oneshot::Sender<()> },
    Shutdown { ack: oneshot::Sender<()> },
}

struct ImportStateWriter {
    tx: mpsc::Sender<ImportStateCommand>,
    handle: Option<thread::JoinHandle<()>>,
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
                        upsert_import_memory(&conn, &row).unwrap_or_else(|e| {
                            panic!(
                                "failed to upsert import memory for podcastindex_id={}: {e}",
                                row.podcastindex_id
                            )
                        });
                    }
                    ImportStateCommand::SetLastId { id, ack } => {
                        set_last_processed_id(&conn, id).unwrap_or_else(|e| {
                            panic!("failed to persist import cursor at id={id}: {e}");
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

    async fn set_last_id(&self, id: i64) {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.tx
            .send(ImportStateCommand::SetLastId { id, ack: ack_tx })
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

fn enqueue_import_memory(tx: &mpsc::Sender<ImportStateCommand>, row: ImportMemoryRow) {
    tx.send(ImportStateCommand::UpsertMemory(row))
        .expect("import state writer channel closed unexpectedly");
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

    fn get_last_id(&self) -> i64 {
        self.conn
            .query_row(
                "SELECT value FROM import_progress WHERE key = 'last_processed_id'",
                [],
                |row| row.get::<_, String>(0),
            )
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    }

    fn reset(&self) {
        self.conn
            .execute(
                "DELETE FROM import_progress WHERE key = 'last_processed_id'",
                [],
            )
            .expect("failed to reset cursor");
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
            attempt_count      INTEGER NOT NULL DEFAULT 1
        );
        CREATE INDEX IF NOT EXISTS idx_import_feed_memory_status
            ON import_feed_memory(fetch_http_status);
        CREATE INDEX IF NOT EXISTS idx_import_feed_memory_medium
            ON import_feed_memory(raw_medium);",
    )
    .expect("failed to initialize import state schema");
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

fn set_last_processed_id(conn: &Connection, id: i64) -> rusqlite::Result<usize> {
    conn.execute(
        "INSERT INTO import_progress (key, value) VALUES ('last_processed_id', ?1)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        [id.to_string()],
    )
}

fn upsert_import_memory(conn: &Connection, row: &ImportMemoryRow) -> rusqlite::Result<usize> {
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
            attempt_count
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?10, 1)
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
            attempt_count = import_feed_memory.attempt_count + 1",
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
        "SELECT podcastindex_id, fetch_http_status, raw_medium, parsed_feed_guid
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
                    raw_medium: row.get(2)?,
                    parsed_feed_guid: row.get(3)?,
                },
            ))
        })
        .expect("failed to query import memory");

    rows.filter_map(Result::ok).collect()
}

fn is_skip_known_non_music(raw_medium: &str) -> bool {
    !raw_medium.eq_ignore_ascii_case("music") && !raw_medium.eq_ignore_ascii_case("publisher")
}

fn should_skip_known_row(known: Option<&KnownImportMemory>) -> bool {
    known.is_some_and(|memory| {
        memory.fetch_http_status == Some(200)
            && memory
                .raw_medium
                .as_deref()
                .is_some_and(is_skip_known_non_music)
    })
}

fn query_batch(db: &Connection, start_id: i64, batch_size: usize) -> Vec<CandidateRow> {
    let mut stmt = db
        .prepare_cached(
            "SELECT id, url, podcastGuid
             FROM   podcasts
             WHERE  id > ?1
             ORDER BY id ASC
             LIMIT ?2",
        )
        .expect("failed to prepare query");

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

async fn crawl_feed_with_import_retries(
    client: &reqwest::Client,
    url: &str,
    fallback_guid: Option<&str>,
    config: &CrawlConfig,
    row_id: i64,
) -> CrawlReport {
    let mut attempt = 1;

    loop {
        let report = crawl_feed_report(client, url, fallback_guid, config).await;
        match &report {
            report if report.is_retryable() && attempt < IMPORT_FETCH_ATTEMPTS => {
                let backoff = report
                    .retry_delay(attempt)
                    .unwrap_or_else(|| Duration::from_secs(1_u64 << (attempt - 1)));
                eprintln!(
                    "  import: retrying crawl after attempt {attempt}/{IMPORT_FETCH_ATTEMPTS} for id={row_id} {url}: {}",
                    report.outcome
                );
                tokio::time::sleep(backoff).await;
                attempt += 1;
            }
            _ => return report,
        }
    }
}

fn append_failed_feeds(path: &str, urls: &[String]) {
    if urls.is_empty() {
        return;
    }

    let path = Path::new(path);
    ensure_parent_dir(path).unwrap_or_else(|e| {
        panic!(
            "failed to create failed feed output directory {}: {e}",
            path.display()
        )
    });

    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap_or_else(|e| panic!("failed to open failed feed output {}: {e}", path.display()));

    for url in urls {
        writeln!(file, "{url}").unwrap_or_else(|e| {
            panic!("failed writing failed feed output {}: {e}", path.display())
        });
    }
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
    failed_feeds_output: String,
    skip_known_non_music: bool,
    dry_run: bool,
    reset: bool,
) {
    let progress = ProgressStore::open(&state_path);
    let state_writer = (!dry_run).then(|| ImportStateWriter::spawn(&state_path));

    ensure_snapshot_db(&db_path, &db_url, refresh_db).await;

    if reset {
        progress.reset();
        eprintln!("import: cursor reset to 0");
    }

    let mut cursor = progress.get_last_id();
    eprintln!(
        "import: starting from id={cursor}, batch={batch_size}, concurrency={concurrency}, fetch_timeout={IMPORT_FETCH_TIMEOUT_SECS}s, skip_known_non_music={skip_known_non_music}"
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
        // Dry run still needs a config but won't POST
        CrawlConfig {
            crawl_token: String::new(),
            ingest_url: String::new(),
            user_agent: "stophammer-crawler/0.1 (dry-run)".to_string(),
            fetch_timeout: std::time::Duration::from_secs(IMPORT_FETCH_TIMEOUT_SECS),
            ingest_timeout: std::time::Duration::from_secs(10),
        }
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
    let mut total_processed: u64 = 0;

    loop {
        let batch = query_batch(&pi_db, cursor, batch_size);
        if batch.is_empty() {
            eprintln!("import: no more candidates after id={cursor}");
            break;
        }

        let batch_max_id = batch.last().map_or(cursor, |r| r.id);
        let batch_len = batch.len();
        let known_memory = if skip_known_non_music {
            progress.known_memory_for_ids(&batch.iter().map(|row| row.id).collect::<Vec<_>>())
        } else {
            std::collections::HashMap::default()
        };

        if dry_run {
            for row in &batch {
                let guid_display = row.podcast_guid.as_deref().unwrap_or("(none)");
                if should_skip_known_row(known_memory.get(&row.id)) {
                    eprintln!(
                        "  [dry-run] would_skip_known_irrelevant id={} guid={guid_display} {}",
                        row.id, row.url
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
            let failed_feeds = Arc::new(Mutex::new(Vec::new()));
            let state_tx = state_writer
                .as_ref()
                .expect("import state writer missing")
                .tx
                .clone();
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
                    let failed_feeds = Arc::clone(&failed_feeds);
                    let state_tx = state_tx.clone();
                    let known_memory = Arc::clone(&known_memory);
                    move || async move {
                        let attempted_at = Utc::now().timestamp();
                        if should_skip_known_row(known_memory.get(&row.id)) {
                            skipped.fetch_add(1, Ordering::Relaxed);
                            let memory_row = ImportMemoryRow::skipped_known_irrelevant(
                                &row,
                                known_memory
                                    .get(&row.id)
                                    .expect("known memory missing for skipped row"),
                                attempted_at,
                            );
                            enqueue_import_memory(&state_tx, memory_row);
                            eprintln!("  skipped_known_irrelevant: id={} {}", row.id, row.url);
                            return;
                        }

                        let fallback = row.podcast_guid.as_deref();
                        let report = crawl_feed_with_import_retries(
                            &client, &row.url, fallback, &config, row.id,
                        )
                        .await;
                        let outcome = &report.outcome;
                        let memory_row =
                            ImportMemoryRow::from_crawl_report(&row, &report, attempted_at);
                        enqueue_import_memory(&state_tx, memory_row);

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

                        if outcome.is_retryable() {
                            failed_feeds
                                .lock()
                                .expect("failed feed retry list mutex poisoned")
                                .push(row.url.clone());
                        }

                        eprintln!("  {outcome}: id={} {}", row.id, row.url);
                    }
                })
                .collect();

            run_pool(tasks, concurrency).await;
            let mut failed_batch = failed_feeds
                .lock()
                .expect("failed feed retry list mutex poisoned");
            failed_batch.sort();
            failed_batch.dedup();
            append_failed_feeds(&failed_feeds_output, &failed_batch);

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
            writer.set_last_id(cursor).await;
        }
        total_processed += batch_len as u64;
        eprintln!("import: cursor={cursor} total_processed={total_processed}");
    }

    if let Some(writer) = state_writer {
        writer.shutdown().await;
    }

    eprintln!("import: complete — {total_processed} feeds processed");
}

#[cfg(test)]
mod tests {
    use super::{
        ImportMemoryRow, ImportStateWriter, ProgressStore, enqueue_import_memory,
        extract_snapshot_archive, is_skip_known_non_music, load_known_import_memory,
        open_state_connection, should_skip_known_row, upsert_import_memory,
    };
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use std::fs;
    use std::io::{self, Cursor};
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
        retryable: bool,
        raw_medium: Option<&str>,
    ) -> ImportMemoryRow {
        ImportMemoryRow {
            podcastindex_id,
            feed_url: format!("https://example.com/{podcastindex_id}.xml"),
            podcastindex_guid: Some(format!("pi-guid-{podcastindex_id}")),
            fetch_http_status,
            fetch_outcome: fetch_outcome.to_string(),
            outcome_reason: None,
            retryable,
            raw_medium: raw_medium.map(ToOwned::to_owned),
            parsed_feed_guid: Some(format!("feed-guid-{podcastindex_id}")),
            attempted_at: 1_700_000_000 + podcastindex_id,
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
    fn upsert_import_memory_stores_status_and_medium() {
        let tempdir = tempdir().expect("tempdir");
        let state_path = tempdir.path().join("import_state.db");
        let conn = open_state_connection(state_path.to_str().expect("utf-8 path"));
        let row = sample_memory_row(4630863, Some(200), "accepted", false, Some("music"));

        upsert_import_memory(&conn, &row).expect("upsert import memory");

        let stored: (Option<u16>, String, Option<String>) = conn
            .query_row(
                "SELECT fetch_http_status, fetch_outcome, raw_medium
                 FROM import_feed_memory
                 WHERE podcastindex_id = ?1",
                [row.podcastindex_id],
                |db_row| Ok((db_row.get(0)?, db_row.get(1)?, db_row.get(2)?)),
            )
            .expect("query stored import memory");

        assert_eq!(stored.0, Some(200));
        assert_eq!(stored.1, "accepted");
        assert_eq!(stored.2.as_deref(), Some("music"));
    }

    #[test]
    fn upsert_import_memory_increments_attempt_count() {
        let tempdir = tempdir().expect("tempdir");
        let state_path = tempdir.path().join("import_state.db");
        let conn = open_state_connection(state_path.to_str().expect("utf-8 path"));
        let first = sample_memory_row(4630863, Some(404), "fetch_error", false, None);
        let second = sample_memory_row(4630863, Some(429), "fetch_error", true, None);

        upsert_import_memory(&conn, &first).expect("first upsert");
        upsert_import_memory(&conn, &second).expect("second upsert");

        let stored: (Option<u16>, i64) = conn
            .query_row(
                "SELECT fetch_http_status, attempt_count
                 FROM import_feed_memory
                 WHERE podcastindex_id = ?1",
                [first.podcastindex_id],
                |db_row| Ok((db_row.get(0)?, db_row.get(1)?)),
            )
            .expect("query attempt count");

        assert_eq!(stored.0, Some(429));
        assert_eq!(stored.1, 2, "expected retry to increment attempt_count");
    }

    #[test]
    fn known_memory_lookup_supports_skip_mode() {
        let tempdir = tempdir().expect("tempdir");
        let state_path = tempdir.path().join("import_state.db");
        let conn = open_state_connection(state_path.to_str().expect("utf-8 path"));
        let known_non_music = sample_memory_row(10, Some(200), "accepted", false, Some("podcast"));
        let known_publisher =
            sample_memory_row(11, Some(200), "accepted", false, Some("publisher"));

        upsert_import_memory(&conn, &known_non_music).expect("upsert non-music row");
        upsert_import_memory(&conn, &known_publisher).expect("upsert publisher row");

        let lookup = load_known_import_memory(&conn, &[10, 11, 12]);

        assert!(should_skip_known_row(lookup.get(&10)));
        assert!(!should_skip_known_row(lookup.get(&11)));
        assert!(!should_skip_known_row(lookup.get(&12)));
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
        let row = sample_memory_row(99, Some(429), "fetch_error", true, None);

        enqueue_import_memory(&writer.tx, row.clone());
        writer.set_last_id(99).await;
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
            .expect("query persisted cursor");

        assert_eq!(stored_attempts, 1);
        assert_eq!(cursor, "99");
    }
}
