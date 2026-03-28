use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, params};

use crate::crawl::CrawlReport;

fn unix_now() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before epoch")
            .as_secs(),
    )
    .unwrap_or(i64::MAX)
}

/// Shared cross-mode database that records feed outcomes so gossip, import,
/// and batch modes can skip feeds already known to be irrelevant (non-music).
pub struct FeedSkipDb {
    conn: Connection,
}

impl FeedSkipDb {
    /// Open (or create) the shared skip database at `path`.
    /// Uses WAL journal mode and a 5-second busy timeout for safe concurrent
    /// access from multiple crawler processes.
    pub fn open(path: &str) -> Self {
        if let Some(parent) = std::path::Path::new(path).parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).unwrap_or_else(|e| {
                panic!(
                    "failed to create feed skip DB directory {}: {e}",
                    parent.display()
                )
            });
        }

        let conn = Connection::open(path).expect("failed to open feed skip DB");
        conn.pragma_update(None, "journal_mode", "WAL")
            .expect("failed to set WAL journal mode on feed skip DB");
        conn.busy_timeout(Duration::from_secs(5))
            .expect("failed to set busy timeout on feed skip DB");
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS feed_outcomes (
                feed_url          TEXT PRIMARY KEY,
                fetch_http_status INTEGER,
                raw_medium        TEXT,
                fetch_outcome     TEXT NOT NULL,
                outcome_reason    TEXT,
                parsed_feed_guid  TEXT,
                source_mode       TEXT NOT NULL,
                first_seen_at     INTEGER NOT NULL,
                last_seen_at      INTEGER NOT NULL,
                seen_count        INTEGER NOT NULL DEFAULT 1
            )",
        )
        .expect("failed to create feed_outcomes table");

        Self { conn }
    }

    /// Check if a feed URL is known to be irrelevant (non-music, non-publisher).
    /// Returns `Some(reason)` if the feed should be skipped, `None` to proceed.
    /// When `ttl_days` is set, entries older than that are ignored (re-evaluated).
    pub fn should_skip(&self, feed_url: &str, ttl_days: Option<u64>) -> Option<String> {
        let row: Option<(Option<i64>, String, Option<String>, Option<String>, i64)> = self
            .conn
            .query_row(
                "SELECT fetch_http_status, fetch_outcome, outcome_reason, raw_medium, last_seen_at
                 FROM feed_outcomes WHERE feed_url = ?1",
                params![feed_url],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
            )
            .ok();

        let (http_status, outcome, reason, medium, last_seen_at) = row?;

        if let Some(ttl) = ttl_days {
            let ttl_secs = i64::try_from(ttl.saturating_mul(86_400)).unwrap_or(i64::MAX);
            if unix_now().saturating_sub(last_seen_at) > ttl_secs {
                return None;
            }
        }

        // HTTP 200 with a known non-music, non-publisher medium
        if http_status == Some(200) {
            if let Some(ref m) = medium {
                if !m.eq_ignore_ascii_case("music") && !m.eq_ignore_ascii_case("publisher") {
                    return Some(format!("known non-music medium: {m}"));
                }
            }
        }

        // Prior medium-gate rejection
        if outcome == "rejected"
            && let Some(ref r) = reason
            && r.starts_with("[medium_music]")
        {
            return Some(format!("prior medium-gate rejection: {r}"));
        }

        None
    }

    /// Record a crawl outcome into the shared skip DB.
    ///
    /// Only records outcomes where the feed was successfully fetched (HTTP 200)
    /// and parsed, giving us reliable medium information. Transient errors
    /// (timeouts, 5xx, DNS failures) are not recorded.
    pub fn record_outcome(&self, feed_url: &str, report: &CrawlReport, source_mode: &str) {
        let dominated_by_fetch_error = report.fetch_http_status.is_none()
            || report.fetch_http_status.is_some_and(|s| s != 200);

        // Medium-gate rejections always have http_status 200 (the fetch
        // succeeded, but the ingest server rejected the feed's medium).
        // Anything without a 200 fetch is a transient failure we should not
        // remember as a skip signal.
        if dominated_by_fetch_error {
            return;
        }

        let now = unix_now();
        let http_status = report.fetch_http_status.map(i64::from);
        let label = report.outcome.label();
        let reason = report.outcome.reason();

        if let Err(e) = self.conn.execute(
            "INSERT INTO feed_outcomes (
                feed_url, fetch_http_status, raw_medium, fetch_outcome,
                outcome_reason, parsed_feed_guid, source_mode,
                first_seen_at, last_seen_at, seen_count
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8, 1)
            ON CONFLICT(feed_url) DO UPDATE SET
                fetch_http_status = excluded.fetch_http_status,
                raw_medium = excluded.raw_medium,
                fetch_outcome = excluded.fetch_outcome,
                outcome_reason = excluded.outcome_reason,
                parsed_feed_guid = excluded.parsed_feed_guid,
                source_mode = excluded.source_mode,
                last_seen_at = excluded.last_seen_at,
                seen_count = seen_count + 1",
            params![
                feed_url,
                http_status,
                report.raw_medium.as_deref(),
                label,
                reason,
                report.parsed_feed_guid.as_deref(),
                source_mode,
                now,
            ],
        ) {
            eprintln!("feed_skip: WARNING: failed to record outcome for {feed_url}: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crawl::{CrawlOutcome, CrawlReport};

    fn temp_db() -> String {
        let dir = tempfile::tempdir().expect("tmpdir");
        // Leak so the dir survives the test
        let path = dir.path().join("feed_skip.db");
        let s = path.to_str().unwrap().to_string();
        std::mem::forget(dir);
        s
    }

    fn report_with(
        outcome: CrawlOutcome,
        http_status: Option<u16>,
        medium: Option<&str>,
        guid: Option<&str>,
    ) -> CrawlReport {
        CrawlReport {
            outcome,
            fetch_http_status: http_status,
            raw_medium: medium.map(ToString::to_string),
            parsed_feed_guid: guid.map(ToString::to_string),
            final_url: None,
            content_sha256: None,
            raw_xml: None,
            parsed_feed: None,
        }
    }

    #[test]
    fn open_creates_schema() {
        let db = FeedSkipDb::open(&temp_db());
        let count: i64 = db
            .conn
            .query_row("SELECT COUNT(*) FROM feed_outcomes", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn unknown_url_returns_none() {
        let db = FeedSkipDb::open(&temp_db());
        assert_eq!(db.should_skip("https://example.com/feed.xml", None), None);
    }

    #[test]
    fn skips_non_music_medium() {
        let db = FeedSkipDb::open(&temp_db());
        let report = report_with(
            CrawlOutcome::Rejected {
                reason: "[medium_music] medium is absent".into(),
                warnings: vec![],
            },
            Some(200),
            Some("podcast"),
            None,
        );
        db.record_outcome("https://example.com/feed.xml", &report, "test");

        let reason = db
            .should_skip("https://example.com/feed.xml", None)
            .expect("should skip");
        assert!(reason.contains("known non-music medium: podcast"));
    }

    #[test]
    fn skips_medium_gate_rejection() {
        let db = FeedSkipDb::open(&temp_db());
        // A feed where raw_medium was absent — no medium field, but the server
        // rejected it with [medium_music].
        let report = report_with(
            CrawlOutcome::Rejected {
                reason: "[medium_music] medium is absent".into(),
                warnings: vec![],
            },
            Some(200),
            None,
            None,
        );
        db.record_outcome("https://example.com/feed.xml", &report, "test");

        let reason = db
            .should_skip("https://example.com/feed.xml", None)
            .expect("should skip");
        assert!(reason.contains("prior medium-gate rejection"));
    }

    #[test]
    fn does_not_skip_music() {
        let db = FeedSkipDb::open(&temp_db());
        let report = report_with(
            CrawlOutcome::Accepted { warnings: vec![] },
            Some(200),
            Some("music"),
            Some("guid-1"),
        );
        db.record_outcome("https://example.com/feed.xml", &report, "test");
        assert_eq!(db.should_skip("https://example.com/feed.xml", None), None);
    }

    #[test]
    fn does_not_skip_publisher() {
        let db = FeedSkipDb::open(&temp_db());
        let report = report_with(
            CrawlOutcome::Accepted { warnings: vec![] },
            Some(200),
            Some("publisher"),
            Some("guid-1"),
        );
        db.record_outcome("https://example.com/feed.xml", &report, "test");
        assert_eq!(db.should_skip("https://example.com/feed.xml", None), None);
    }

    #[test]
    fn does_not_record_fetch_errors() {
        let db = FeedSkipDb::open(&temp_db());
        let report = report_with(
            CrawlOutcome::FetchError {
                reason: "http 503 Service Unavailable".into(),
                retryable: true,
                retry_after_secs: None,
            },
            Some(503),
            None,
            None,
        );
        db.record_outcome("https://example.com/feed.xml", &report, "test");

        let count: i64 = db
            .conn
            .query_row("SELECT COUNT(*) FROM feed_outcomes", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn ttl_expires_old_decisions() {
        let db = FeedSkipDb::open(&temp_db());
        let report = report_with(
            CrawlOutcome::Rejected {
                reason: "[medium_music] medium is absent".into(),
                warnings: vec![],
            },
            Some(200),
            Some("podcast"),
            None,
        );
        db.record_outcome("https://example.com/feed.xml", &report, "test");

        // Backdate last_seen_at to 10 days ago
        db.conn
            .execute(
                "UPDATE feed_outcomes SET last_seen_at = ?1 WHERE feed_url = ?2",
                params![unix_now() - 10 * 86_400, "https://example.com/feed.xml"],
            )
            .unwrap();

        // TTL of 7 days — should expire
        assert_eq!(
            db.should_skip("https://example.com/feed.xml", Some(7)),
            None,
        );

        // TTL of 30 days — should still skip
        assert!(db
            .should_skip("https://example.com/feed.xml", Some(30))
            .is_some());
    }

    #[test]
    fn increments_seen_count() {
        let db = FeedSkipDb::open(&temp_db());
        let report = report_with(
            CrawlOutcome::Accepted { warnings: vec![] },
            Some(200),
            Some("music"),
            Some("guid-1"),
        );
        db.record_outcome("https://example.com/feed.xml", &report, "gossip");
        db.record_outcome("https://example.com/feed.xml", &report, "import");

        let (count, mode): (i64, String) = db
            .conn
            .query_row(
                "SELECT seen_count, source_mode FROM feed_outcomes WHERE feed_url = ?1",
                params!["https://example.com/feed.xml"],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(count, 2);
        assert_eq!(mode, "import"); // last writer wins
    }

    #[test]
    fn medium_check_is_case_insensitive() {
        let db = FeedSkipDb::open(&temp_db());
        for medium in &["Music", "MUSIC", "music"] {
            let report = report_with(
                CrawlOutcome::Accepted { warnings: vec![] },
                Some(200),
                Some(medium),
                None,
            );
            let url = format!("https://example.com/{medium}.xml");
            db.record_outcome(&url, &report, "test");
            assert_eq!(db.should_skip(&url, None), None, "should not skip {medium}");
        }
    }
}
