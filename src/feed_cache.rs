//! Fetch cache for conditional GET (ADR 0050, `stophammer` repository).
//!
//! The store keeps one row for each URL that the crawler has fetched. A row
//! holds the validators from the last `200` answer, the compressed body, the
//! hash, the final URL after redirects, the fetch time, and the last node
//! answer. Task 002 wires `crawl_feed_report` as the caller.

use std::io::{Read, Write};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use rusqlite::{Connection, OptionalExtension, params};

/// The current time, as Unix seconds.
pub(crate) fn unix_now() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before epoch")
            .as_secs(),
    )
    .unwrap_or(i64::MAX)
}

/// Compress a body with gzip, for storage.
fn compress(body: &str) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(body.as_bytes())
        .expect("gzip encoder write must not fail for an in-memory buffer");
    encoder
        .finish()
        .expect("gzip encoder finish must not fail for an in-memory buffer")
}

/// Decompress a gzip body back to text.
fn decompress(body_gzip: &[u8]) -> std::io::Result<String> {
    let mut decoder = GzDecoder::new(body_gzip);
    let mut body = String::new();
    decoder.read_to_string(&mut body)?;
    Ok(body)
}

/// A freshly fetched feed, ready to enter the cache.
///
/// `put` reads each field and writes one row. It does not read `fetched_at`
/// from the clock; the caller supplies it.
pub struct FetchedFeed<'a> {
    pub final_url: &'a str,
    pub etag: Option<&'a str>,
    pub last_modified: Option<&'a str>,
    pub content_sha256: &'a str,
    pub body: &'a str,
    pub fetched_at: i64,
}

/// One cached row, with the body decompressed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedFeed {
    pub url: String,
    pub final_url: String,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub content_sha256: String,
    pub body: String,
    pub fetched_at: i64,
    pub node_answer: Option<String>,
    pub node_reason: Option<String>,
    pub answered_at: Option<i64>,
}

type CacheRow = (
    String,
    Option<String>,
    Option<String>,
    String,
    Vec<u8>,
    i64,
    Option<String>,
    Option<String>,
    Option<i64>,
);

/// Shared cross-mode fetch cache (ADR 0050 §1, `stophammer` repository).
///
/// Each mode reads and writes it through a conditional GET. A container
/// crash can leave a row half up to date only across two writes; each write
/// here is one statement, so a single write always leaves a consistent row.
pub struct FeedCacheDb {
    conn: Connection,
}

impl FeedCacheDb {
    /// Open (or create) the shared fetch cache at `path`.
    /// Use WAL journal mode and a 5-second busy timeout, for safe
    /// concurrent access from more than one crawler process.
    pub fn open(path: &str) -> Self {
        if let Some(parent) = std::path::Path::new(path).parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).unwrap_or_else(|e| {
                panic!(
                    "failed to create feed cache DB directory {}: {e}",
                    parent.display()
                )
            });
        }

        let conn = Connection::open(path).expect("failed to open feed cache DB");
        conn.pragma_update(None, "journal_mode", "WAL")
            .expect("failed to set WAL journal mode on feed cache DB");
        conn.busy_timeout(Duration::from_secs(5))
            .expect("failed to set busy timeout on feed cache DB");
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS feed_cache (
                url            TEXT PRIMARY KEY,
                final_url      TEXT NOT NULL,
                etag           TEXT,
                last_modified  TEXT,
                content_sha256 TEXT NOT NULL,
                body_gzip      BLOB NOT NULL,
                fetched_at     INTEGER NOT NULL,
                node_answer    TEXT,
                node_reason    TEXT,
                answered_at    INTEGER
            ) STRICT",
        )
        .expect("failed to create feed_cache table");

        Self { conn }
    }

    /// Read the cached row for `url`, with the body decompressed.
    /// Give `None` when no row exists, or when the store cannot answer.
    pub fn get(&self, url: &str) -> Option<CachedFeed> {
        let row: Option<CacheRow> = match self
            .conn
            .query_row(
                "SELECT final_url, etag, last_modified, content_sha256, body_gzip,
                        fetched_at, node_answer, node_reason, answered_at
                 FROM feed_cache WHERE url = ?1",
                params![url],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                        row.get(8)?,
                    ))
                },
            )
            .optional()
        {
            Ok(row) => row,
            Err(e) => {
                eprintln!("feed_cache: WARNING: failed to read cache row for {url}: {e}");
                return None;
            }
        };

        let (
            final_url,
            etag,
            last_modified,
            content_sha256,
            body_gzip,
            fetched_at,
            node_answer,
            node_reason,
            answered_at,
        ) = row?;

        let body = match decompress(&body_gzip) {
            Ok(body) => body,
            Err(e) => {
                eprintln!("feed_cache: WARNING: failed to decompress cached body for {url}: {e}");
                return None;
            }
        };

        Some(CachedFeed {
            url: url.to_string(),
            final_url,
            etag,
            last_modified,
            content_sha256,
            body,
            fetched_at,
            node_answer,
            node_reason,
            answered_at,
        })
    }

    /// Replace the row for `url` with a freshly fetched feed.
    /// Compress the body with gzip. Clear the node answer, because a fresh
    /// body has no answer yet.
    pub fn put(&self, url: &str, entry: &FetchedFeed<'_>) {
        let body_gzip = compress(entry.body);

        if let Err(e) = self.conn.execute(
            "INSERT INTO feed_cache (
                url, final_url, etag, last_modified, content_sha256, body_gzip,
                fetched_at, node_answer, node_reason, answered_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, NULL, NULL)
            ON CONFLICT(url) DO UPDATE SET
                final_url      = excluded.final_url,
                etag           = excluded.etag,
                last_modified  = excluded.last_modified,
                content_sha256 = excluded.content_sha256,
                body_gzip      = excluded.body_gzip,
                fetched_at     = excluded.fetched_at,
                node_answer    = NULL,
                node_reason    = NULL,
                answered_at    = NULL",
            params![
                url,
                entry.final_url,
                entry.etag,
                entry.last_modified,
                entry.content_sha256,
                body_gzip,
                entry.fetched_at,
            ],
        ) {
            eprintln!("feed_cache: WARNING: failed to store cache row for {url}: {e}");
        }
    }

    /// Record the node's answer to the last submission for `url`.
    /// Update the row in place. Do nothing when no row exists for `url`.
    pub fn record_node_answer(&self, url: &str, label: &str, reason: Option<&str>, at: i64) {
        if let Err(e) = self.conn.execute(
            "UPDATE feed_cache SET node_answer = ?2, node_reason = ?3, answered_at = ?4
             WHERE url = ?1",
            params![url, label, reason, at],
        ) {
            eprintln!("feed_cache: WARNING: failed to record node answer for {url}: {e}");
        }
    }

    /// Delete the row for `url`. Do nothing when no row exists.
    ///
    /// The crawler calls this when the node rejects a feed for its medium.
    /// The shared skip list keeps that feed out of later fetches, so its body
    /// is never used again, and a podcast body can hold tens of MiB.
    pub fn remove(&self, url: &str) {
        if let Err(e) = self
            .conn
            .execute("DELETE FROM feed_cache WHERE url = ?1", params![url])
        {
            eprintln!("feed_cache: WARNING: failed to remove cache row for {url}: {e}");
        }
    }

    /// Clear the node's answer for `url`, so a later `304` submits the kept
    /// body again (`stophammer` ADR 0051 §5). Do nothing when no row exists.
    pub fn clear_node_answer(&self, url: &str) {
        if let Err(e) = self.conn.execute(
            "UPDATE feed_cache SET node_answer = NULL, node_reason = NULL, answered_at = NULL
             WHERE url = ?1",
            params![url],
        ) {
            eprintln!("feed_cache: WARNING: failed to clear node answer for {url}: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_db() -> String {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("feed_cache.db");
        let s = path.to_str().unwrap().to_string();
        // Leak so the dir survives the test.
        std::mem::forget(dir);
        s
    }

    fn sample_entry(body: &str) -> FetchedFeed<'_> {
        FetchedFeed {
            final_url: "https://example.com/feed.xml",
            etag: Some("\"abc123\""),
            last_modified: Some("Wed, 21 Oct 2026 07:28:00 GMT"),
            content_sha256: "deadbeef",
            body,
            fetched_at: unix_now(),
        }
    }

    #[test]
    fn open_creates_schema() {
        let db = FeedCacheDb::open(&temp_db());
        let count: i64 = db
            .conn
            .query_row("SELECT COUNT(*) FROM feed_cache", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn get_on_missing_url_gives_none() {
        let db = FeedCacheDb::open(&temp_db());
        assert_eq!(db.get("https://example.com/feed.xml"), None);
    }

    #[test]
    fn put_then_get_round_trips_each_field() {
        let db = FeedCacheDb::open(&temp_db());
        let body = "<rss><channel><title>Feed</title></channel></rss>";
        let entry = sample_entry(body);
        let url = "https://example.com/feed.xml";

        db.put(url, &entry);
        let cached = db.get(url).expect("row must exist after put");

        assert_eq!(cached.url, url);
        assert_eq!(cached.final_url, entry.final_url);
        assert_eq!(cached.etag.as_deref(), entry.etag);
        assert_eq!(cached.last_modified.as_deref(), entry.last_modified);
        assert_eq!(cached.content_sha256, entry.content_sha256);
        assert_eq!(cached.body, body, "the body must round trip byte for byte");
        assert_eq!(cached.fetched_at, entry.fetched_at);
        assert_eq!(cached.node_answer, None);
        assert_eq!(cached.node_reason, None);
        assert_eq!(cached.answered_at, None);
    }

    #[test]
    fn second_put_replaces_the_row_and_clears_the_answer() {
        let db = FeedCacheDb::open(&temp_db());
        let url = "https://example.com/feed.xml";
        let first = sample_entry("<rss>one</rss>");
        db.put(url, &first);
        db.record_node_answer(url, "accepted", None, unix_now());

        let second = FetchedFeed {
            final_url: "https://example.com/feed-2.xml",
            etag: Some("\"xyz789\""),
            last_modified: None,
            content_sha256: "cafef00d",
            body: "<rss>two</rss>",
            fetched_at: unix_now(),
        };
        db.put(url, &second);

        let cached = db.get(url).expect("row must still exist");
        assert_eq!(cached.final_url, second.final_url);
        assert_eq!(cached.body, "<rss>two</rss>");
        assert_eq!(
            cached.node_answer, None,
            "a fresh put must clear the prior node answer"
        );
        assert_eq!(cached.node_reason, None);
        assert_eq!(cached.answered_at, None);
    }

    #[test]
    fn record_node_answer_then_get_gives_the_answer() {
        let db = FeedCacheDb::open(&temp_db());
        let url = "https://example.com/feed.xml";
        db.put(url, &sample_entry("<rss/>"));

        let at = unix_now();
        db.record_node_answer(url, "rejected", Some("[medium_music] absent"), at);

        let cached = db.get(url).expect("row must exist");
        assert_eq!(cached.node_answer.as_deref(), Some("rejected"));
        assert_eq!(cached.node_reason.as_deref(), Some("[medium_music] absent"));
        assert_eq!(cached.answered_at, Some(at));
    }

    #[test]
    fn remove_deletes_the_row_and_a_missing_url_does_nothing() {
        let db = FeedCacheDb::open(&temp_db());
        db.put("https://example.com/a.xml", &sample_entry("<rss/>"));
        db.remove("https://example.com/a.xml");
        assert!(
            db.get("https://example.com/a.xml").is_none(),
            "remove must delete the row"
        );
        db.remove("https://example.com/missing.xml");
    }

    #[test]
    fn record_node_answer_on_missing_url_does_nothing() {
        let db = FeedCacheDb::open(&temp_db());
        db.record_node_answer(
            "https://example.com/missing.xml",
            "accepted",
            None,
            unix_now(),
        );
        assert_eq!(db.get("https://example.com/missing.xml"), None);
    }

    #[test]
    fn two_handles_on_one_file_see_each_others_writes() {
        let path = temp_db();
        let writer = FeedCacheDb::open(&path);
        let reader = FeedCacheDb::open(&path);
        let url = "https://example.com/feed.xml";

        writer.put(url, &sample_entry("<rss>shared</rss>"));

        let cached = reader
            .get(url)
            .expect("the second handle must see the first handle's write");
        assert_eq!(cached.body, "<rss>shared</rss>");
    }

    #[test]
    fn weak_etag_comes_back_unchanged() {
        let db = FeedCacheDb::open(&temp_db());
        let url = "https://example.com/feed.xml";
        let entry = FetchedFeed {
            final_url: url,
            etag: Some("W/\"abc\""),
            last_modified: None,
            content_sha256: "deadbeef",
            body: "<rss/>",
            fetched_at: unix_now(),
        };
        db.put(url, &entry);

        let cached = db.get(url).expect("row must exist");
        assert_eq!(cached.etag.as_deref(), Some("W/\"abc\""));
    }
}
