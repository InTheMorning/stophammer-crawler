mod crawl;
mod dedup;
mod modes;
mod pool;

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
    },

    /// Import from a `PodcastIndex` snapshot database
    Import {
        /// Path to `podcastindex_feeds.db`
        #[arg(long)]
        db: String,

        /// Path to import state database (resume cursor)
        #[arg(long, default_value = "./import_state.db")]
        state: String,

        /// Feeds per database query batch
        #[arg(long, default_value_t = 100)]
        batch: usize,

        /// Parallel fetch+ingest workers
        #[arg(long, env = "CONCURRENCY", default_value_t = 5)]
        concurrency: usize,

        /// Log candidates without fetching
        #[arg(long)]
        dry_run: bool,

        /// Clear resume cursor and start from id=0
        #[arg(long)]
        reset: bool,
    },

    /// Listen to Podping WebSocket stream for music feeds
    Podping {
        /// Parallel fetch+ingest workers
        #[arg(long, env = "CONCURRENCY", default_value_t = 3)]
        concurrency: usize,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.mode {
        Mode::Crawl { urls, concurrency } => {
            modes::crawl::run(urls, concurrency).await;
        }
        Mode::Import {
            db,
            state,
            batch,
            concurrency,
            dry_run,
            reset,
        } => {
            modes::import::run(db, state, batch, concurrency, dry_run, reset).await;
        }
        Mode::Podping { concurrency } => {
            modes::podping::run(concurrency).await;
        }
    }
}
