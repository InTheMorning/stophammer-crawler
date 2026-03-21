mod crawl;
mod dedup;
mod modes;
mod pool;
mod url_queue;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "stophammer-crawler", about = "Unified RSS feed crawler")]
struct Cli {
    #[command(subcommand)]
    mode: Mode,
}

#[derive(Subcommand)]
enum Mode {
    /// Crawl a list of feed URLs (file, args, env, or stdin)
    Crawl {
        /// Feed URLs or path to a file containing URLs
        urls: Vec<String>,

        #[arg(long, env = "CONCURRENCY", default_value_t = 5)]
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

        /// Re-download the latest snapshot even if `--db` already exists
        #[arg(long)]
        refresh_db: bool,

        /// Path to import state database (resume cursor)
        #[arg(long, default_value = "./import_state.db")]
        state: String,

        /// Feeds per database query batch
        #[arg(long, default_value_t = 100)]
        batch: usize,

        /// Parallel fetch+ingest workers
        #[arg(long, env = "CONCURRENCY", default_value_t = 5)]
        concurrency: usize,

        /// Plain-text output file for retryable feed URLs
        #[arg(
            long,
            env = "FAILED_FEEDS_OUTPUT",
            default_value = "./failed_feeds.txt"
        )]
        failed_feeds_output: String,

        /// Log candidates without fetching
        #[arg(long)]
        dry_run: bool,

        /// Clear resume cursor and start from id=0
        #[arg(long)]
        reset: bool,
    },

    /// Listen to Podping WebSocket stream for music feeds
    Podping {
        /// Path to Podping state database (latest seen block cursor)
        #[arg(long, default_value = "./podping_state.db")]
        state: String,

        /// Replay starting from an explicit Hive block number
        #[arg(long, conflicts_with_all = ["old", "time"])]
        block: Option<u64>,

        /// Replay starting from N hours ago (estimated at one block per 3 seconds)
        #[arg(long, conflicts_with_all = ["block", "time"])]
        old: Option<u64>,

        /// Replay starting from an RFC 3339 timestamp (estimated at one block per 3 seconds)
        #[arg(long, conflicts_with_all = ["block", "old"])]
        time: Option<String>,

        /// Parallel fetch+ingest workers
        #[arg(long, env = "CONCURRENCY", default_value_t = 3)]
        concurrency: usize,
    },

    /// Listen to gossip-listener SSE stream for real-time podping notifications
    Gossip {
        /// Path to gossip state database (latest seen timestamp cursor)
        #[arg(long, default_value = "./gossip_state.db")]
        state: String,

        /// SSE endpoint URL (default: http://localhost:8089/events)
        #[arg(long)]
        sse_url: Option<String>,

        /// Replay from gossip-listener archive database
        #[arg(long)]
        archive_db: Option<String>,

        /// Catch-up starting from N hours ago (requires --archive-db)
        #[arg(long, requires = "archive_db")]
        since_hours: Option<u64>,

        /// Parallel fetch+ingest workers
        #[arg(long, env = "CONCURRENCY", default_value_t = 3)]
        concurrency: usize,

        /// Quiet mode: hide medium_music rejections (non-music spam)
        #[arg(short, long)]
        quiet: bool,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.mode {
        Mode::Crawl {
            urls,
            concurrency,
            host_delay_ms,
            failed_feeds_output,
        } => {
            modes::crawl::run(urls, concurrency, host_delay_ms, failed_feeds_output).await;
        }
        Mode::Import {
            db,
            db_url,
            refresh_db,
            state,
            batch,
            concurrency,
            failed_feeds_output,
            dry_run,
            reset,
        } => {
            modes::import::run(
                db,
                db_url,
                refresh_db,
                state,
                batch,
                concurrency,
                failed_feeds_output,
                dry_run,
                reset,
            )
            .await;
        }
        Mode::Podping {
            state,
            block,
            old,
            time,
            concurrency,
        } => {
            modes::podping::run(state, block, old, time, concurrency).await;
        }
        Mode::Gossip {
            state,
            sse_url,
            archive_db,
            since_hours,
            concurrency,
            quiet,
        } => {
            modes::gossip::run(state, sse_url, archive_db, since_hours, concurrency, quiet).await;
        }
    }
}
