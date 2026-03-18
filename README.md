# stophammer-crawler

Unified feed crawler for the [stophammer](https://github.com/dardevelin/stophammer)
V4V music index. Fetches RSS feeds, hashes content, parses with
[stophammer-parser](https://github.com/dardevelin/stophammer-parser) as a native
library, and submits results to a stophammer node's `/ingest/feed` endpoint.

Three subcommands cover every discovery path:

- **crawl** — one-shot URL list (file, args, env, or stdin)
- **import** — batch scan a PodcastIndex SQLite snapshot with resume cursor
- **podping** — long-running WebSocket listener for real-time discovery

## Requirements

- Rust 1.85+ (edition 2024)
- For import mode: a [PodcastIndex](https://podcastindex.org) database snapshot

## Build and run

From this checkout:

```bash
# Operational crawler modes
cargo run --manifest-path stophammer-crawler/Cargo.toml -- crawl --help
cargo run --manifest-path stophammer-crawler/Cargo.toml -- import --help
cargo run --manifest-path stophammer-crawler/Cargo.toml -- podping --help

# Analysis / replay tools
cargo run --manifest-path stophammer-crawler/Cargo.toml --bin feed_audit -- --help
cargo run --manifest-path stophammer-crawler/Cargo.toml --bin audit_analyzer -- --help
cargo run --manifest-path stophammer-crawler/Cargo.toml --bin audit_import -- --help
```

Or build binaries once:

```bash
cargo build --manifest-path stophammer-crawler/Cargo.toml --release --bins
```

## Usage

### crawl

Fetch and ingest a list of feed URLs:

```bash
# From arguments
CRAWL_TOKEN=secret \
INGEST_URL=http://127.0.0.1:8008/ingest/feed \
cargo run --manifest-path stophammer-crawler/Cargo.toml -- \
  crawl https://example.com/feed.xml

# From a file
CRAWL_TOKEN=secret \
INGEST_URL=http://127.0.0.1:8008/ingest/feed \
cargo run --manifest-path stophammer-crawler/Cargo.toml -- \
  crawl feeds.txt

# From env
CRAWL_TOKEN=secret \
INGEST_URL=http://127.0.0.1:8008/ingest/feed \
FEED_URLS="https://a.com/feed,https://b.com/feed" \
cargo run --manifest-path stophammer-crawler/Cargo.toml -- \
  crawl

# From stdin
cat urls.txt | CRAWL_TOKEN=secret \
  INGEST_URL=http://127.0.0.1:8008/ingest/feed \
  cargo run --manifest-path stophammer-crawler/Cargo.toml -- crawl
```

### import

Batch-scan a PodcastIndex snapshot for music feeds:

```bash
CRAWL_TOKEN=secret \
INGEST_URL=http://127.0.0.1:8008/ingest/feed \
cargo run --manifest-path stophammer-crawler/Cargo.toml -- import \
  --db /path/to/podcastindex_feeds.db \
  --batch 100 --concurrency 5
```

#### Import options

| Flag | Default | Description |
|---|---|---|
| `--db <path>` | — (required) | Path to the PodcastIndex snapshot |
| `--state <path>` | `./import_state.db` | Progress cursor database (created automatically) |
| `--batch <n>` | `100` | Feeds per DB query batch |
| `--concurrency <n>` | `5` | Parallel fetch + ingest workers |
| `--dry-run` | off | Log candidates without fetching or ingesting |
| `--reset` | off | Clear cursor and restart from `id=0` |

Progress is stored in `--state`. If the process is interrupted, the next run resumes
from the last completed batch. A crash mid-batch re-processes that batch — safe because
stophammer deduplicates on content hash.

### podping

Listen to the Podping WebSocket stream for music feed updates:

```bash
CRAWL_TOKEN=secret \
INGEST_URL=http://127.0.0.1:8008/ingest/feed \
cargo run --manifest-path stophammer-crawler/Cargo.toml -- \
  podping --concurrency 3
```

Filters: accepts `medium=music` or absent medium; drops `reason=newValueBlock`.
Includes in-memory dedup with cooldown to avoid re-crawling the same URL within
5 minutes (30 minutes for spammy feeds). Reconnects with exponential backoff
(1s to 60s) on disconnect.

## Environment variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `CRAWL_TOKEN` | yes | — | Shared secret for stophammer ingest auth |
| `INGEST_URL` | no | `http://localhost:8008/ingest/feed` | Stophammer ingest endpoint |
| `CONCURRENCY` | no | `5` (crawl/import) / `3` (podping) | Worker pool size |
| `FEED_URLS` | no | — | Comma or newline-separated URLs (crawl mode) |
| `PODPING_WS_URL` | no | `wss://api.livewire.io/ws/podping` | Podping WebSocket endpoint |

## Architecture

```
stophammer-crawler
  src/
    main.rs           CLI dispatcher (clap subcommands)
    crawl.rs          Shared pipeline: fetch → SHA-256 → parse → POST
    pool.rs           Bounded concurrency pool (tokio semaphore)
    dedup.rs          In-memory cooldown map (podping mode)
    modes/
      crawl.rs        Load URLs from file/env/stdin, run pool
      import.rs       PodcastIndex DB batches, resume cursor, fallback GUID
      podping.rs      WebSocket listener, music filter, dedup, persistent workers
```

The core pipeline in `crawl.rs` calls `stophammer-parser` as a Rust library — no
subprocess spawning. Every mode feeds URLs into the same `crawl_feed()` function:

1. **Fetch** the RSS feed via `reqwest`
2. **Hash** the raw response body with SHA-256
3. **Parse** the XML with `stophammer-parser::profile::stophammer()` (or
   `stophammer_with_fallback()` for import mode with PodcastIndex GUIDs)
4. **POST** the result as JSON to the stophammer `/ingest/feed` endpoint

Concurrency is bounded by a tokio semaphore — crawl and import modes drain a
fixed task list; podping mode runs an unbounded stream with a permit-based cap.

## Analysis Tools

Audit and corpus-analysis tools live under `analysis/` so they stay separate from
the crawler's operational code and runtime data:

```text
analysis/
  bin/                 Rust binaries for feed audits and corpus analysis
  data/                Local audit inputs/outputs (NDJSON, SQLite snapshots)
  reports/             Generated Markdown/JSON reports and clustering artifacts
```

Available binaries:

- `cargo run --bin feed_audit -- ...`
- `cargo run --bin audit_analyzer -- ...`
- `cargo run --bin audit_import -- ...`

By default:

- `feed_audit` reads `./analysis/data/stophammer-feeds.db` and writes
  `./analysis/data/feed_audit.ndjson`
- `audit_analyzer` reads `./analysis/data/feed_audit.ndjson` and writes reports
  under `./analysis/reports/`
- `audit_import` reads `./analysis/data/feed_audit.ndjson` and replays cached
  feeds into `/ingest/feed` using `./analysis/data/audit_import_state.db` as its
  resume cursor

Examples:

```bash
# Create / refresh the cached NDJSON corpus
cargo run --manifest-path stophammer-crawler/Cargo.toml --bin feed_audit -- \
  --db ./analysis/data/stophammer-feeds.db \
  --output ./analysis/data/feed_audit.ndjson

# Re-analyze the cached corpus
cargo run --manifest-path stophammer-crawler/Cargo.toml --bin audit_analyzer -- \
  --input ./analysis/data/feed_audit.ndjson

# Replay cached feeds into a running primary
CRAWL_TOKEN=secret \
INGEST_URL=http://127.0.0.1:8008/ingest/feed \
cargo run --manifest-path stophammer-crawler/Cargo.toml --bin audit_import -- \
  --input ./analysis/data/feed_audit.ndjson \
  --reset
```

## Docker

```bash
# This repo does not ship a general-purpose crawler compose stack.
# The top-level repo only includes test environments:
docker compose -f ../docker-compose.e2e.yml up -d --build --wait
docker compose -f ../docker-compose.e2e.yml down -v
```

For day-to-day crawler operation, run the binary directly or supply your own
compose file / scheduler. The available top-level compose files are:

- `../docker-compose.e2e.yml`
- `../docker-compose.e2e-tls.yml`

## License

AGPL-3.0-only — see [LICENSE](LICENSE).
