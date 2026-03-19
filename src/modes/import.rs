use std::fs;
use std::io::{self, Read, Write};
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use flate2::read::GzDecoder;
use rusqlite::Connection;
use tar::Archive;

use crate::crawl::{CrawlConfig, CrawlOutcome, crawl_feed};
use crate::pool::run_pool;

const IMPORT_FETCH_ATTEMPTS: u32 = 2;
const IMPORT_FETCH_TIMEOUT_SECS: u64 = 5;
const SNAPSHOT_CONNECT_TIMEOUT_SECS: u64 = 30;
const RESOLVERCTL_BIN_ENV: &str = "RESOLVERCTL_BIN";
const RESOLVER_DB_PATH_ENV: &str = "RESOLVER_DB_PATH";

struct CandidateRow {
    id: i64,
    url: String,
    podcast_guid: Option<String>,
}

struct ResolverImportGuard {
    resolverctl_bin: String,
    resolver_db_path: String,
}

impl ResolverImportGuard {
    fn maybe_activate() -> Option<Self> {
        let resolver_db_path = std::env::var(RESOLVER_DB_PATH_ENV).ok()?;
        let resolverctl_bin =
            std::env::var(RESOLVERCTL_BIN_ENV).unwrap_or_else(|_| "resolverctl".to_string());

        set_import_active(&resolverctl_bin, &resolver_db_path, true);
        Some(Self {
            resolverctl_bin,
            resolver_db_path,
        })
    }
}

impl Drop for ResolverImportGuard {
    fn drop(&mut self) {
        set_import_active(&self.resolverctl_bin, &self.resolver_db_path, false);
    }
}

/// Resume cursor stored in a separate state DB.
struct ProgressStore {
    conn: Connection,
}

impl ProgressStore {
    fn open(path: &str) -> Self {
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
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS import_progress (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )",
        )
        .expect("failed to create progress table");
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

    fn set_last_id(&self, id: i64) {
        if let Err(e) = self.conn.execute(
            "INSERT INTO import_progress (key, value) VALUES ('last_processed_id', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            [id.to_string()],
        ) {
            eprintln!("import: WARNING: failed to persist cursor at id={id}: {e}");
        }
    }

    fn reset(&self) {
        self.conn
            .execute(
                "DELETE FROM import_progress WHERE key = 'last_processed_id'",
                [],
            )
            .expect("failed to reset cursor");
    }
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

    stmt.query_map(rusqlite::params![start_id, batch_size], |row| {
        Ok(CandidateRow {
            id: row.get(0)?,
            url: row.get(1)?,
            podcast_guid: row.get(2)?,
        })
    })
    .expect("query failed")
    .filter_map(Result::ok)
    .collect()
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
) -> CrawlOutcome {
    let mut attempt = 1;

    loop {
        let outcome = crawl_feed(client, url, fallback_guid, config).await;
        match &outcome {
            CrawlOutcome::FetchError(err) if attempt < IMPORT_FETCH_ATTEMPTS => {
                let backoff = Duration::from_secs(1_u64 << (attempt - 1));
                eprintln!(
                    "  import: retrying fetch after attempt {attempt}/{IMPORT_FETCH_ATTEMPTS} for id={row_id} {url}: {err}"
                );
                tokio::time::sleep(backoff).await;
                attempt += 1;
            }
            _ => return outcome,
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "import mode keeps batch resume, fetch, and ingest orchestration in one async entrypoint"
)]
pub async fn run(
    db_path: String,
    db_url: String,
    refresh_db: bool,
    state_path: String,
    batch_size: usize,
    concurrency: usize,
    dry_run: bool,
    reset: bool,
) {
    let progress = ProgressStore::open(&state_path);

    ensure_snapshot_db(&db_path, &db_url, refresh_db).await;

    if reset {
        progress.reset();
        eprintln!("import: cursor reset to 0");
    }

    let mut cursor = progress.get_last_id();
    eprintln!(
        "import: starting from id={cursor}, batch={batch_size}, concurrency={concurrency}, fetch_timeout={IMPORT_FETCH_TIMEOUT_SECS}s"
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

        if dry_run {
            for row in &batch {
                let guid_display = row.podcast_guid.as_deref().unwrap_or("(none)");
                eprintln!("  [dry-run] id={} guid={guid_display} {}", row.id, row.url);
            }
        } else {
            let accepted = Arc::new(AtomicU64::new(0));
            let rejected = Arc::new(AtomicU64::new(0));
            let no_change = Arc::new(AtomicU64::new(0));
            let errors = Arc::new(AtomicU64::new(0));

            let tasks: Vec<_> = batch
                .into_iter()
                .map(|row| {
                    let client = Arc::clone(&client);
                    let config = Arc::clone(&config);
                    let accepted = Arc::clone(&accepted);
                    let rejected = Arc::clone(&rejected);
                    let no_change = Arc::clone(&no_change);
                    let errors = Arc::clone(&errors);
                    move || async move {
                        let fallback = row.podcast_guid.as_deref();
                        let outcome = crawl_feed_with_import_retries(
                            &client, &row.url, fallback, &config, row.id,
                        )
                        .await;

                        match &outcome {
                            CrawlOutcome::Accepted { .. } => {
                                accepted.fetch_add(1, Ordering::Relaxed);
                            }
                            CrawlOutcome::Rejected { .. } => {
                                rejected.fetch_add(1, Ordering::Relaxed);
                            }
                            CrawlOutcome::NoChange => {
                                no_change.fetch_add(1, Ordering::Relaxed);
                            }
                            CrawlOutcome::FetchError(_)
                            | CrawlOutcome::ParseError(_)
                            | CrawlOutcome::IngestError(_) => {
                                errors.fetch_add(1, Ordering::Relaxed);
                            }
                        }

                        eprintln!("  {outcome}: id={} {}", row.id, row.url);
                    }
                })
                .collect();

            run_pool(tasks, concurrency).await;

            eprintln!(
                "import: batch done — accepted={} rejected={} no_change={} errors={}",
                accepted.load(Ordering::Relaxed),
                rejected.load(Ordering::Relaxed),
                no_change.load(Ordering::Relaxed),
                errors.load(Ordering::Relaxed),
            );
        }

        cursor = batch_max_id;
        progress.set_last_id(cursor);
        total_processed += batch_len as u64;
        eprintln!("import: cursor={cursor} total_processed={total_processed}");
    }

    eprintln!("import: complete — {total_processed} feeds processed");
}

#[cfg(test)]
mod tests {
    use super::extract_snapshot_archive;
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use std::fs;
    use std::io::{self, Cursor};
    use tar::{Builder, Header};

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

    #[test]
    fn extract_snapshot_archive_writes_db_file() {
        let tempdir = tempfile::tempdir().expect("tempdir");
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
        let tempdir = tempfile::tempdir().expect("tempdir");
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
        let tempdir = tempfile::tempdir().expect("tempdir");
        let db_path = tempdir.path().join("podcastindex_feeds.db");
        let archive = make_snapshot_archive(&[("README.txt", b"ignore me")]);

        let err = extract_snapshot_archive(Cursor::new(archive), &db_path).expect_err("missing db");

        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(!db_path.exists());
    }
}
