use std::io::IsTerminal;
use std::sync::Arc;

use crate::crawl::{crawl_feed, CrawlConfig};
use crate::pool::run_pool;

/// Load URLs from: positional args → `FEED_URLS` env → stdin (one per line).
fn load_urls(args: &[String]) -> Vec<String> {
    // 1. Positional args: if first arg is a file, read it; else treat all as URLs
    if !args.is_empty() {
        let first = &args[0];
        if args.len() == 1 && std::path::Path::new(first).is_file() {
            let content = std::fs::read_to_string(first).expect("failed to read URL file");
            return content
                .lines()
                .map(str::trim)
                .filter(|l| !l.is_empty() && !l.starts_with('#'))
                .map(String::from)
                .collect();
        }
        return args.to_vec();
    }

    // 2. FEED_URLS env
    if let Ok(env_urls) = std::env::var("FEED_URLS") {
        let urls: Vec<String> = env_urls
            .lines()
            .flat_map(|l| l.split(','))
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .map(String::from)
            .collect();
        if !urls.is_empty() {
            return urls;
        }
    }

    // 3. stdin (only if not a tty)
    if !std::io::stdin().is_terminal() {
        use std::io::BufRead;
        return std::io::stdin()
            .lock()
            .lines()
            .map_while(Result::ok)
            .map(|l| l.trim().to_string())
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .collect();
    }

    Vec::new()
}

pub async fn run(urls_arg: Vec<String>, concurrency: usize) {
    let urls = load_urls(&urls_arg);

    if urls.is_empty() {
        eprintln!("no URLs provided (pass as args, set FEED_URLS, or pipe to stdin)");
        std::process::exit(1);
    }

    eprintln!("crawl: {} URLs, concurrency={concurrency}", urls.len());

    let config = Arc::new(CrawlConfig::from_env());
    let client = Arc::new(reqwest::Client::new());

    let tasks: Vec<_> = urls
        .into_iter()
        .map(|url| {
            let client = Arc::clone(&client);
            let config = Arc::clone(&config);
            move || async move {
                let outcome = crawl_feed(&client, &url, None, &config).await;
                eprintln!("  {outcome}: {url}");
            }
        })
        .collect();

    run_pool(tasks, concurrency).await;

    eprintln!("crawl: done");
}
