mod crawl;
mod dedup;
mod feed_skip;
mod modes;
mod pool;
mod url_queue;

use clap::{Parser, Subcommand};

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
    #[command(subcommand)]
    mode: Mode,
}

#[derive(Subcommand)]
enum Mode {
    /// Crawl a list of feed URLs (file, args, env, or stdin)
    #[command(name = "crawl", alias = "batch")]
    Batch {
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

        /// Feeds per database query batch
        #[arg(long, default_value_t = 100, value_parser = parse_positive_usize)]
        batch: usize,

        /// Parallel fetch+ingest workers
        #[arg(long, env = "CONCURRENCY", default_value_t = 5, value_parser = parse_positive_usize)]
        concurrency: usize,

        /// Optional NDJSON output containing local RSS copies in `feed_audit` format
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

    /// Listen to gossip-listener SSE stream for real-time podping notifications
    Gossip {
        /// Path to gossip state database (latest seen timestamp cursor)
        #[arg(long, default_value = "./gossip_state.db")]
        state: String,

        /// Path to shared feed skip database (cross-mode skip knowledge)
        #[arg(long, default_value = "./feed_skip.db")]
        skip_db: String,

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

        /// Skip feeds already known to be non-music based on prior crawl results
        #[arg(long)]
        skip_known_non_music: bool,

        /// Re-evaluate skip decisions after N days (default: off, skips persist indefinitely)
        #[arg(long)]
        skip_ttl_days: Option<u64>,

        /// Quiet mode: hide `medium_music` rejections (non-music spam)
        #[arg(short, long)]
        quiet: bool,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.mode {
        Mode::Batch {
            urls,
            concurrency,
            host_delay_ms,
            failed_feeds_output,
        } => {
            modes::batch::run(urls, concurrency, host_delay_ms, failed_feeds_output).await;
        }
        Mode::Import {
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
            )
            .await;
        }
        Mode::Gossip {
            state,
            skip_db,
            sse_url,
            archive_db,
            since_hours,
            concurrency,
            skip_known_non_music,
            skip_ttl_days,
            quiet,
        } => {
            modes::gossip::run(
                state,
                skip_db,
                sse_url,
                archive_db,
                since_hours,
                concurrency,
                skip_known_non_music,
                skip_ttl_days,
                quiet,
            )
            .await;
        }
    }
}
