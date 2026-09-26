mod crawl;
mod dedup;
mod feed_cache;
mod feed_skip;
mod fetch_guard;
mod follow;
mod modes;
mod pool;
mod url_queue;

use clap::{Parser, Subcommand};

fn parse_force_reingest_value(raw: &str) -> Result<bool, String> {
    match raw.trim() {
        "1" => Ok(true),
        "0" => Ok(false),
        value
            if value.eq_ignore_ascii_case("true")
                || value.eq_ignore_ascii_case("yes")
                || value.eq_ignore_ascii_case("on") =>
        {
            Ok(true)
        }
        value
            if value.eq_ignore_ascii_case("false")
                || value.eq_ignore_ascii_case("no")
                || value.eq_ignore_ascii_case("off") =>
        {
            Ok(false)
        }
        _ => Err("expected one of: 1, 0, true, false, yes, no, on, off".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use clap::{CommandFactory, Parser};

    use super::{Cli, Mode, parse_force_reingest_value};

    #[test]
    fn force_reingest_env_accepts_boolish_values() {
        for enabled in ["1", "true", "TRUE", "yes", "on"] {
            assert_eq!(
                parse_force_reingest_value(enabled),
                Ok(true),
                "expected {enabled} to enable force reingest"
            );
        }

        for disabled in ["0", "false", "FALSE", "no", "off"] {
            assert_eq!(
                parse_force_reingest_value(disabled),
                Ok(false),
                "expected {disabled} to disable force reingest"
            );
        }

        assert!(
            parse_force_reingest_value("maybe").is_err(),
            "unexpected FORCE_REINGEST value should be rejected"
        );
    }

    #[test]
    fn gossip_host_delay_ms_defaults_to_1500() {
        let cli = Cli::try_parse_from(["stophammer-crawler", "gossip"])
            .expect("gossip must parse with no flags");
        let Mode::Gossip { host_delay_ms, .. } = cli.mode else {
            panic!("expected the gossip subcommand");
        };
        assert_eq!(
            host_delay_ms, 1500,
            "the default host_delay_ms must be 1500"
        );
    }

    #[test]
    fn gossip_host_delay_ms_reads_the_host_delay_ms_env_var() {
        let command = Cli::command();
        let gossip = command
            .find_subcommand("gossip")
            .expect("the gossip subcommand must exist");
        let arg = gossip
            .get_arguments()
            .find(|arg| arg.get_id().as_str() == "host_delay_ms")
            .expect("gossip must declare a host-delay-ms argument");
        assert_eq!(
            arg.get_env(),
            Some(std::ffi::OsStr::new("HOST_DELAY_MS")),
            "gossip host-delay-ms must read the HOST_DELAY_MS env var"
        );
    }

    /// Proves ADR 0050 §1 and the phase plan decision 7 (`stophammer`
    /// repository): `feed`, `refresh`, `gossip` and `import` each declare
    /// `--feed-cache`, with env `FEED_CACHE_DB` and default
    /// `./feed_cache.db`.
    #[test]
    fn feed_cache_flag_has_the_expected_env_and_default_on_each_fetching_mode() {
        let command = Cli::command();
        for mode in ["feed", "refresh", "gossip", "import"] {
            let subcommand = command
                .find_subcommand(mode)
                .unwrap_or_else(|| panic!("the {mode} subcommand must exist"));
            let arg = subcommand
                .get_arguments()
                .find(|arg| arg.get_id().as_str() == "feed_cache")
                .unwrap_or_else(|| panic!("{mode} must declare a feed-cache argument"));
            assert_eq!(
                arg.get_env(),
                Some(std::ffi::OsStr::new("FEED_CACHE_DB")),
                "{mode} --feed-cache must read the FEED_CACHE_DB env var"
            );
            assert_eq!(
                arg.get_default_values(),
                [std::ffi::OsStr::new("./feed_cache.db")],
                "{mode} --feed-cache must default to ./feed_cache.db"
            );
        }
    }

    /// The `ndjson` mode does not fetch, so it must declare no fetch cache
    /// (ADR 0050 phase plan, non-goals, `stophammer` repository).
    #[test]
    fn ndjson_declares_no_feed_cache_flag() {
        let command = Cli::command();
        let ndjson = command
            .find_subcommand("ndjson")
            .expect("the ndjson subcommand must exist");
        assert!(
            ndjson
                .get_arguments()
                .all(|arg| arg.get_id().as_str() != "feed_cache"),
            "ndjson must not declare a feed-cache argument; it does not fetch"
        );
    }

    /// Proves ADR 0050 §5 (`stophammer` repository): `--no-revalidate` is
    /// global, like `--force`, so every mode can read it.
    #[test]
    fn no_revalidate_is_a_global_flag_like_force() {
        let command = Cli::command();
        for flag in ["force", "no_revalidate"] {
            let arg = command
                .get_arguments()
                .find(|arg| arg.get_id().as_str() == flag)
                .unwrap_or_else(|| panic!("Cli must declare a {flag} argument"));
            assert!(
                arg.is_global_set(),
                "--{flag} must be global, so every mode can read it"
            );
        }
    }

    #[test]
    fn no_revalidate_defaults_to_false() {
        let cli = Cli::try_parse_from(["stophammer-crawler", "feed"])
            .expect("feed must parse with no flags");
        assert!(
            !cli.no_revalidate,
            "no_revalidate must default to false, so a plain run still revalidates"
        );
    }
}

fn force_reingest_from_env() -> bool {
    match std::env::var("FORCE_REINGEST") {
        Ok(raw) => parse_force_reingest_value(&raw).unwrap_or_else(|err| {
            eprintln!("error: invalid FORCE_REINGEST value {raw:?}: {err}");
            std::process::exit(2);
        }),
        Err(std::env::VarError::NotPresent) => false,
        Err(std::env::VarError::NotUnicode(_)) => {
            eprintln!("error: FORCE_REINGEST must be valid Unicode");
            std::process::exit(2);
        }
    }
}

fn parse_positive_usize(raw: &str) -> Result<usize, String> {
    let value = raw
        .parse::<usize>()
        .map_err(|err| format!("expected a positive integer: {err}"))?;
    if value == 0 {
        return Err("value must be greater than 0".to_string());
    }
    Ok(value)
}

fn parse_non_negative_i64(raw: &str) -> Result<i64, String> {
    let value = raw
        .parse::<i64>()
        .map_err(|err| format!("expected a non-negative integer: {err}"))?;
    if value < 0 {
        return Err("value must be greater than or equal to 0".to_string());
    }
    Ok(value)
}

#[derive(Parser)]
#[command(name = "stophammer-crawler", about = "Unified RSS feed crawler")]
struct Cli {
    /// Force re-ingestion even if the feed content has not changed
    #[arg(long, global = true)]
    force: bool,

    /// Send no conditional GET, even when the fetch cache holds a row for
    /// the URL (ADR 0050 §5, `stophammer` repository)
    #[arg(long, global = true)]
    no_revalidate: bool,

    #[command(subcommand)]
    mode: Mode,
}

#[derive(Subcommand)]
enum Mode {
    /// Fetch and ingest a list of feed URLs (file, args, env, or stdin)
    #[command(name = "feed", alias = "crawl")]
    Feed {
        /// Feed URLs or path to a file containing URLs
        urls: Vec<String>,

        #[arg(long, env = "CONCURRENCY", default_value_t = 5, value_parser = parse_positive_usize)]
        concurrency: usize,

        /// Minimum spacing between fetches to the same host
        #[arg(long, env = "HOST_DELAY_MS", default_value_t = 1500)]
        host_delay_ms: u64,

        /// Plain-text output file for retryable feed URLs
        #[arg(
            long,
            env = "FAILED_FEEDS_OUTPUT",
            default_value = "./failed_feeds.txt"
        )]
        failed_feeds_output: String,

        /// Path to the shared fetch cache (ADR 0050 §1, `stophammer`
        /// repository)
        #[arg(long, env = "FEED_CACHE_DB", default_value = "./feed_cache.db")]
        feed_cache: String,
    },

    /// Read the node's feed list, then re-run the crawl pipeline over it
    /// (a corrective pass; combine with `--force`)
    Refresh {
        #[arg(long, env = "CONCURRENCY", default_value_t = 5, value_parser = parse_positive_usize)]
        concurrency: usize,

        /// Minimum spacing between fetches to the same host
        #[arg(long, env = "HOST_DELAY_MS", default_value_t = 1500)]
        host_delay_ms: u64,

        /// Plain-text output file for retryable feed URLs
        #[arg(
            long,
            env = "FAILED_FEEDS_OUTPUT",
            default_value = "./failed_feeds.txt"
        )]
        failed_feeds_output: String,

        /// Path to the shared fetch cache (ADR 0050 §1, `stophammer`
        /// repository)
        #[arg(long, env = "FEED_CACHE_DB", default_value = "./feed_cache.db")]
        feed_cache: String,
    },

    /// Import from a `PodcastIndex` snapshot database
    Import {
        /// Path to the extracted `podcastindex_feeds.db`
        #[arg(long, default_value = "./podcastindex_feeds.db")]
        db: String,

        /// Download URL for the latest `PodcastIndex` snapshot archive
        #[arg(
            long,
            env = "PODCASTINDEX_DB_URL",
            default_value = "https://public.podcastindex.org/podcastindex_feeds.db.tgz"
        )]
        db_url: String,

        /// Conditionally refresh the local snapshot when the remote archive changed
        #[arg(long)]
        refresh_db: bool,

        /// Path to import state database (resume cursor)
        #[arg(long, default_value = "./import_state.db")]
        state: String,

        /// Path to shared feed skip database (cross-mode skip knowledge)
        #[arg(long, default_value = "./feed_skip.db")]
        skip_db: String,

        /// Path to the shared fetch cache (ADR 0050 §1, `stophammer`
        /// repository)
        #[arg(long, env = "FEED_CACHE_DB", default_value = "./feed_cache.db")]
        feed_cache: String,

        /// Feeds per database query batch
        #[arg(long, default_value_t = 100, value_parser = parse_positive_usize)]
        batch: usize,

        /// Parallel fetch+ingest workers
        #[arg(long, env = "CONCURRENCY", default_value_t = 5, value_parser = parse_positive_usize)]
        concurrency: usize,

        /// Optional cached-feed NDJSON output containing local RSS copies
        #[arg(long)]
        audit_output: Option<String>,

        /// Replace `--audit-output` instead of appending to it
        #[arg(long, requires = "audit_output")]
        audit_replace: bool,

        /// Skip rows already known to publish a non-music, non-publisher medium
        #[arg(long)]
        skip_known_non_music: bool,

        /// Skip rows already known to have been ingested successfully
        #[arg(long)]
        skip_known_success: bool,

        /// Restrict snapshot import to Wavlake-hosted feeds and apply conservative 429 backoff
        #[arg(long)]
        wavlake_only: bool,

        /// Log candidates without fetching
        #[arg(long)]
        dry_run: bool,

        /// Start from an explicit `PodcastIndex` id instead of the stored or music-first cursor
        #[arg(long, value_parser = parse_non_negative_i64)]
        cursor: Option<i64>,
    },

    /// Replay cached NDJSON rows into stophammer without re-fetching feeds
    Ndjson {
        /// Path to cached feed NDJSON file
        #[arg(long, default_value = "./stored-feeds.ndjson")]
        input: String,

        /// Path to resume-cursor state database
        #[arg(long, default_value = "./ndjson_state.db")]
        state: String,

        /// Rows per processing batch
        #[arg(long, default_value_t = 100, value_parser = parse_positive_usize)]
        batch: usize,

        /// Maximum number of NDJSON rows to process this run
        #[arg(long)]
        limit: Option<usize>,

        /// Parallel parse+ingest workers
        #[arg(long, env = "CONCURRENCY", default_value_t = 5, value_parser = parse_positive_usize)]
        concurrency: usize,

        /// Log candidates without posting to stophammer
        #[arg(long)]
        dry_run: bool,

        /// Clear resume cursor and start from the first row
        #[arg(long)]
        reset: bool,
    },

    /// Listen to gossip-listener SSE stream for real-time podping notifications
    Gossip {
        /// Path to gossip state database (latest seen timestamp cursor)
        #[arg(long, default_value = "./gossip_state.db")]
        state: String,

        /// Path to shared feed skip database (cross-mode skip knowledge)
        #[arg(long, default_value = "./feed_skip.db")]
        skip_db: String,

        /// Path to the shared fetch cache (ADR 0050 §1, `stophammer`
        /// repository)
        #[arg(long, env = "FEED_CACHE_DB", default_value = "./feed_cache.db")]
        feed_cache: String,

        /// SSE endpoint URL (default: <http://localhost:8089/events>)
        #[arg(long)]
        sse_url: Option<String>,

        /// Replay from gossip-listener archive database
        #[arg(long)]
        archive_db: Option<String>,

        /// Catch-up starting from N hours ago (requires --archive-db)
        #[arg(long, requires = "archive_db")]
        since_hours: Option<u64>,

        /// Parallel fetch+ingest workers
        #[arg(long, env = "CONCURRENCY", default_value_t = 3, value_parser = parse_positive_usize)]
        concurrency: usize,

        /// Minimum spacing between fetches to the same host, for a follow
        /// fetch (ADR 0049 §2, `stophammer` repository)
        #[arg(long, env = "HOST_DELAY_MS", default_value_t = 1500)]
        host_delay_ms: u64,

        /// Skip feeds already known to be non-music based on prior crawl results
        #[arg(long)]
        skip_known_non_music: bool,

        /// Re-evaluate skip decisions after N days (default: off, skips persist indefinitely)
        #[arg(long)]
        skip_ttl_days: Option<u64>,

        /// Quiet mode: hide `medium_music` rejections (non-music spam)
        #[arg(short, long)]
        quiet: bool,

        /// Optional cached-feed NDJSON output containing local RSS copies
        #[arg(long)]
        audit_output: Option<String>,

        /// Replace `--audit-output` instead of appending to it
        #[arg(long, requires = "audit_output")]
        audit_replace: bool,
    },
}

#[tokio::main]
#[expect(
    clippy::too_many_lines,
    reason = "one dispatch arm per crawler mode; grows by design as modes are added"
)]
async fn main() {
    let cli = Cli::parse();
    let force = cli.force || force_reingest_from_env();
    // ADR 0050 §5 (`stophammer` repository): `--no-revalidate` clears
    // `CrawlConfig::revalidate`, as `--force` sets `force_reingest`.
    let revalidate = !cli.no_revalidate;

    match cli.mode {
        Mode::Feed {
            urls,
            concurrency,
            host_delay_ms,
            failed_feeds_output,
            feed_cache,
        } => {
            modes::batch::run(
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
        Mode::Refresh {
            concurrency,
            host_delay_ms,
            failed_feeds_output,
            feed_cache,
        } => {
            modes::refresh::run(
                concurrency,
                host_delay_ms,
                failed_feeds_output,
                force,
                feed_cache,
                revalidate,
            )
            .await;
        }
        Mode::Import {
            db,
            db_url,
            refresh_db,
            state,
            skip_db,
            feed_cache,
            batch,
            concurrency,
            audit_output,
            audit_replace,
            skip_known_non_music,
            skip_known_success,
            wavlake_only,
            dry_run,
            cursor,
        } => {
            modes::import::run(
                db,
                db_url,
                refresh_db,
                state,
                skip_db,
                batch,
                concurrency,
                audit_output,
                audit_replace,
                skip_known_non_music,
                skip_known_success,
                wavlake_only,
                dry_run,
                cursor,
                force,
                feed_cache,
                revalidate,
            )
            .await;
        }
        Mode::Ndjson {
            input,
            state,
            batch,
            limit,
            concurrency,
            dry_run,
            reset,
        } => {
            modes::ndjson::run(
                input,
                state,
                batch,
                limit,
                concurrency,
                dry_run,
                reset,
                force,
            )
            .await;
        }
        Mode::Gossip {
            state,
            skip_db,
            feed_cache,
            sse_url,
            archive_db,
            since_hours,
            concurrency,
            host_delay_ms,
            skip_known_non_music,
            skip_ttl_days,
            quiet,
            audit_output,
            audit_replace,
        } => {
            modes::gossip::run(
                state,
                skip_db,
                feed_cache,
                sse_url,
                archive_db,
                since_hours,
                concurrency,
                host_delay_ms,
                skip_known_non_music,
                skip_ttl_days,
                quiet,
                force,
                revalidate,
                audit_output,
                audit_replace,
            )
            .await;
        }
    }
}
