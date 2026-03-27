#![allow(
    clippy::too_many_lines,
    reason = "offline musicL backfill keeps discovery, diffing, and replay orchestration in one executable"
)]

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use clap::Parser;
use rusqlite::{Connection, OpenFlags};

#[allow(dead_code)]
#[path = "../../src/crawl.rs"]
mod crawl;
#[path = "../../src/pool.rs"]
mod pool;

use crawl::{CrawlConfig, CrawlOutcome, crawl_feed_report};
use pool::run_pool;

const DEFAULT_STATE_DBS: [&str; 2] = ["./import_state.db", "./import_state_wavlake.db"];
const BACKFILL_FETCH_TIMEOUT_SECS: u64 = 5;
const BACKFILL_TASK_HARD_TIMEOUT_SECS: u64 = 15;
const BACKFILL_SAMPLE_SIZE: usize = 10;

#[derive(Parser, Debug)]
#[command(
    name = "musicl_backfill",
    about = "Backfill missing musicL feeds from crawler import state DBs"
)]
struct Cli {
    /// Path to the primary stophammer database to compare against
    #[arg(long, env = "STOPHAMMER_DB_PATH", default_value = "./stophammer.db")]
    stophammer_db: String,

    /// Import state DB path(s) to scan; defaults to `./import_state.db` and `./import_state_wavlake.db`
    #[arg(long = "state-db")]
    state_dbs: Vec<String>,

    /// Parallel fetch+ingest workers
    #[arg(long, env = "CONCURRENCY", default_value_t = 5)]
    concurrency: usize,

    /// Log candidates without fetching/ingesting
    #[arg(long)]
    dry_run: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MusiclCandidate {
    feed_guid: String,
    feed_url: String,
    last_attempted_at: i64,
    state_db_path: String,
}

fn default_state_paths() -> Vec<String> {
    DEFAULT_STATE_DBS.iter().map(ToString::to_string).collect()
}

fn open_existing_readonly_db(path: &str) -> Option<Connection> {
    if !Path::new(path).exists() {
        eprintln!("musicl_backfill: skipping missing state DB {path}");
        return None;
    }

    Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|err| {
        eprintln!("musicl_backfill: skipping unreadable state DB {path}: {err}");
        err
    })
    .ok()
}

fn has_import_feed_memory(conn: &Connection) -> Option<bool> {
    conn.query_row(
        "SELECT EXISTS(
            SELECT 1
            FROM sqlite_master
            WHERE type = 'table' AND name = 'import_feed_memory'
        )",
        [],
        |row| row.get(0),
    )
    .map_err(|err| {
        eprintln!("musicl_backfill: failed to inspect state DB schema: {err}");
        err
    })
    .ok()
}

fn load_musicl_candidates_from_state_db(path: &str) -> Vec<MusiclCandidate> {
    let Some(conn) = open_existing_readonly_db(path) else {
        return Vec::new();
    };

    let Some(has_memory) = has_import_feed_memory(&conn) else {
        return Vec::new();
    };
    if !has_memory {
        eprintln!("musicl_backfill: skipping non-import state DB {path}");
        return Vec::new();
    }

    let mut stmt = match conn.prepare(
        "SELECT parsed_feed_guid, feed_url, last_attempted_at
         FROM import_feed_memory
         WHERE fetch_http_status = 200
           AND parsed_feed_guid IS NOT NULL
           AND lower(raw_medium) = 'musicl'
         ORDER BY last_attempted_at DESC, podcastindex_id DESC",
    ) {
        Ok(stmt) => stmt,
        Err(err) => {
            eprintln!("musicl_backfill: failed to prepare candidate query for {path}: {err}");
            return Vec::new();
        }
    };

    let rows = match stmt.query_map([], |row| {
        Ok(MusiclCandidate {
            feed_guid: row.get(0)?,
            feed_url: row.get(1)?,
            last_attempted_at: row.get(2)?,
            state_db_path: path.to_string(),
        })
    }) {
        Ok(rows) => rows,
        Err(err) => {
            eprintln!("musicl_backfill: failed to query musicL candidates from {path}: {err}");
            return Vec::new();
        }
    };

    rows.filter_map(Result::ok).collect()
}

fn dedupe_candidates(candidates: Vec<MusiclCandidate>) -> Vec<MusiclCandidate> {
    let mut deduped = HashMap::<String, MusiclCandidate>::new();
    for candidate in candidates {
        deduped
            .entry(candidate.feed_guid.clone())
            .and_modify(|existing| {
                if candidate.last_attempted_at > existing.last_attempted_at {
                    *existing = candidate.clone();
                }
            })
            .or_insert(candidate);
    }

    let mut deduped = deduped.into_values().collect::<Vec<_>>();
    deduped.sort_by(|a, b| a.feed_guid.cmp(&b.feed_guid));
    deduped
}

fn load_existing_feed_guids(path: &str, candidate_guids: &[String]) -> HashSet<String> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .unwrap_or_else(|err| panic!("failed to open stophammer DB {path}: {err}"));

    if candidate_guids.is_empty() {
        return HashSet::new();
    }

    let mut existing = HashSet::new();
    for chunk in candidate_guids.chunks(500) {
        let placeholders = std::iter::repeat_n("?", chunk.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("SELECT feed_guid FROM feeds WHERE feed_guid IN ({placeholders})");
        let mut stmt = conn
            .prepare(&sql)
            .unwrap_or_else(|err| panic!("failed to prepare feed existence query: {err}"));
        let rows = stmt
            .query_map(rusqlite::params_from_iter(chunk.iter()), |row| {
                row.get::<_, String>(0)
            })
            .expect("failed to query existing feed_guids");
        existing.extend(rows.filter_map(Result::ok));
    }
    existing
}

fn print_candidate_summary(
    state_paths: &[String],
    total_candidates: usize,
    missing_candidates: &[MusiclCandidate],
) {
    eprintln!(
        "musicl_backfill: scanned {} state DB(s), discovered {} deduped musicL candidate(s), {} missing from stophammer.db",
        state_paths.len(),
        total_candidates,
        missing_candidates.len(),
    );
    for candidate in missing_candidates.iter().take(BACKFILL_SAMPLE_SIZE) {
        eprintln!(
            "  candidate: guid={} state_db={} {}",
            candidate.feed_guid, candidate.state_db_path, candidate.feed_url
        );
    }
    if missing_candidates.len() > BACKFILL_SAMPLE_SIZE {
        eprintln!(
            "  ... {} more candidate(s) omitted from preview",
            missing_candidates.len() - BACKFILL_SAMPLE_SIZE
        );
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "tool entrypoint intentionally keeps discovery, diffing, and replay orchestration together"
)]
async fn run(
    stophammer_db_path: String,
    state_dbs: Vec<String>,
    concurrency: usize,
    dry_run: bool,
) {
    let state_paths = if state_dbs.is_empty() {
        default_state_paths()
    } else {
        state_dbs
    };

    let discovered = state_paths
        .iter()
        .flat_map(|path| load_musicl_candidates_from_state_db(path))
        .collect::<Vec<_>>();
    let deduped = dedupe_candidates(discovered);
    let candidate_guids = deduped
        .iter()
        .map(|candidate| candidate.feed_guid.clone())
        .collect::<Vec<_>>();
    let existing = load_existing_feed_guids(&stophammer_db_path, &candidate_guids);
    let missing = deduped
        .into_iter()
        .filter(|candidate| !existing.contains(&candidate.feed_guid))
        .collect::<Vec<_>>();

    print_candidate_summary(&state_paths, candidate_guids.len(), &missing);

    if dry_run {
        eprintln!("musicl_backfill: dry-run complete; no feeds fetched");
        return;
    }
    if missing.is_empty() {
        eprintln!("musicl_backfill: nothing to backfill");
        return;
    }

    let client = Arc::new(reqwest::Client::new());
    let mut config = CrawlConfig::from_env();
    config.fetch_timeout = Duration::from_secs(BACKFILL_FETCH_TIMEOUT_SECS);
    let config = Arc::new(config);

    let accepted = Arc::new(AtomicU64::new(0));
    let rejected = Arc::new(AtomicU64::new(0));
    let no_change = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));

    let tasks = missing
        .into_iter()
        .map(|candidate| {
            let client = Arc::clone(&client);
            let config = Arc::clone(&config);
            let accepted = Arc::clone(&accepted);
            let rejected = Arc::clone(&rejected);
            let no_change = Arc::clone(&no_change);
            let errors = Arc::clone(&errors);
            move || async move {
                let Ok(report) = tokio::time::timeout(
                    Duration::from_secs(BACKFILL_TASK_HARD_TIMEOUT_SECS),
                    crawl_feed_report(
                        &client,
                        &candidate.feed_url,
                        Some(&candidate.feed_guid),
                        &config,
                    ),
                )
                .await
                else {
                    eprintln!(
                        "  musicl_backfill: ERROR hard timeout after {}s guid={} {}",
                        BACKFILL_TASK_HARD_TIMEOUT_SECS, candidate.feed_guid, candidate.feed_url
                    );
                    errors.fetch_add(1, Ordering::Relaxed);
                    return;
                };

                match &report.outcome {
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

                eprintln!(
                    "  {}: guid={} {}",
                    report.outcome, candidate.feed_guid, candidate.feed_url
                );
            }
        })
        .collect::<Vec<_>>();

    run_pool(tasks, concurrency).await;

    eprintln!(
        "musicl_backfill: done — accepted={} rejected={} no_change={} errors={}",
        accepted.load(Ordering::Relaxed),
        rejected.load(Ordering::Relaxed),
        no_change.load(Ordering::Relaxed),
        errors.load(Ordering::Relaxed),
    );
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    run(
        cli.stophammer_db,
        cli.state_dbs,
        cli.concurrency,
        cli.dry_run,
    )
    .await;
}

#[cfg(test)]
mod tests {
    use super::{
        MusiclCandidate, dedupe_candidates, default_state_paths, load_existing_feed_guids,
        load_musicl_candidates_from_state_db,
    };
    use rusqlite::Connection;

    fn seed_import_state(path: &str) -> Connection {
        let conn = Connection::open(path).expect("open state db");
        conn.execute_batch(
            "CREATE TABLE import_feed_memory (
                podcastindex_id INTEGER PRIMARY KEY,
                feed_url TEXT NOT NULL,
                podcastindex_guid TEXT,
                fetch_http_status INTEGER,
                fetch_outcome TEXT NOT NULL,
                outcome_reason TEXT,
                retryable INTEGER NOT NULL DEFAULT 0,
                raw_medium TEXT,
                parsed_feed_guid TEXT,
                first_attempted_at INTEGER NOT NULL,
                last_attempted_at INTEGER NOT NULL,
                attempt_duration_ms INTEGER NOT NULL DEFAULT 0,
                attempt_count INTEGER NOT NULL DEFAULT 1
            );",
        )
        .expect("create import_feed_memory");
        conn
    }

    #[test]
    fn state_paths_default_to_import_and_wavlake_state() {
        assert_eq!(
            default_state_paths(),
            vec![
                "./import_state.db".to_string(),
                "./import_state_wavlake.db".to_string()
            ]
        );
    }

    #[test]
    fn load_musicl_candidates_only_selects_200_rows_with_guid() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let path = tempdir.path().join("import_state.db");
        let conn = seed_import_state(path.to_str().expect("utf-8 path"));
        conn.execute(
            "INSERT INTO import_feed_memory (
                podcastindex_id, feed_url, fetch_http_status, fetch_outcome, raw_medium,
                parsed_feed_guid, first_attempted_at, last_attempted_at
            ) VALUES (?1, ?2, ?3, 'accepted', ?4, ?5, 1, ?6)",
            rusqlite::params![
                1_i64,
                "https://example.com/musicl.xml",
                200_i64,
                "musicL",
                "guid-musicl",
                10_i64
            ],
        )
        .expect("insert musicL row");
        conn.execute(
            "INSERT INTO import_feed_memory (
                podcastindex_id, feed_url, fetch_http_status, fetch_outcome, raw_medium,
                parsed_feed_guid, first_attempted_at, last_attempted_at
            ) VALUES (?1, ?2, ?3, 'accepted', ?4, NULL, 1, ?5)",
            rusqlite::params![
                2_i64,
                "https://example.com/missing-guid.xml",
                200_i64,
                "musicL",
                11_i64
            ],
        )
        .expect("insert missing guid row");
        conn.execute(
            "INSERT INTO import_feed_memory (
                podcastindex_id, feed_url, fetch_http_status, fetch_outcome, raw_medium,
                parsed_feed_guid, first_attempted_at, last_attempted_at
            ) VALUES (?1, ?2, ?3, 'accepted', ?4, ?5, 1, ?6)",
            rusqlite::params![
                3_i64,
                "https://example.com/music.xml",
                200_i64,
                "music",
                "guid-music",
                12_i64
            ],
        )
        .expect("insert music row");
        conn.execute(
            "INSERT INTO import_feed_memory (
                podcastindex_id, feed_url, fetch_http_status, fetch_outcome, raw_medium,
                parsed_feed_guid, first_attempted_at, last_attempted_at
            ) VALUES (?1, ?2, ?3, 'accepted', ?4, ?5, 1, ?6)",
            rusqlite::params![
                4_i64,
                "https://example.com/error.xml",
                404_i64,
                "musicL",
                "guid-error",
                13_i64
            ],
        )
        .expect("insert 404 row");

        let candidates = load_musicl_candidates_from_state_db(path.to_str().expect("utf-8 path"));
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].feed_guid, "guid-musicl");
    }

    #[test]
    fn dedupe_candidates_keeps_most_recent_attempt_per_guid() {
        let deduped = dedupe_candidates(vec![
            MusiclCandidate {
                feed_guid: "guid-1".to_string(),
                feed_url: "https://example.com/older.xml".to_string(),
                last_attempted_at: 10,
                state_db_path: "a.db".to_string(),
            },
            MusiclCandidate {
                feed_guid: "guid-1".to_string(),
                feed_url: "https://example.com/newer.xml".to_string(),
                last_attempted_at: 20,
                state_db_path: "b.db".to_string(),
            },
        ]);

        assert_eq!(deduped.len(), 1);
        assert_eq!(deduped[0].feed_url, "https://example.com/newer.xml");
        assert_eq!(deduped[0].state_db_path, "b.db");
    }

    #[test]
    fn existing_feed_lookup_only_returns_present_guids() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let path = tempdir.path().join("stophammer.db");
        let conn = Connection::open(&path).expect("open stophammer db");
        conn.execute_batch("CREATE TABLE feeds (feed_guid TEXT PRIMARY KEY);")
            .expect("create feeds");
        conn.execute("INSERT INTO feeds(feed_guid) VALUES (?1)", ["guid-present"])
            .expect("insert feed");

        let existing = load_existing_feed_guids(
            path.to_str().expect("utf-8 path"),
            &["guid-present".to_string(), "guid-missing".to_string()],
        );
        assert!(existing.contains("guid-present"));
        assert!(!existing.contains("guid-missing"));
    }
}
