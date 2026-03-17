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

## Usage

### crawl

Fetch and ingest a list of feed URLs:

```bash
# From arguments
CRAWL_TOKEN=secret stophammer-crawler crawl https://example.com/feed.xml

# From a file
CRAWL_TOKEN=secret stophammer-crawler crawl feeds.txt

# From env
CRAWL_TOKEN=secret FEED_URLS="https://a.com/feed,https://b.com/feed" stophammer-crawler crawl

# From stdin
cat urls.txt | CRAWL_TOKEN=secret stophammer-crawler crawl
```

### import

Batch-scan a PodcastIndex snapshot for music feeds:

```bash
CRAWL_TOKEN=secret stophammer-crawler import \
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
CRAWL_TOKEN=secret stophammer-crawler podping --concurrency 3
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

By default:

- `feed_audit` reads `./analysis/data/stophammer-feeds.db` and writes
  `./analysis/data/feed_audit.ndjson`
- `audit_analyzer` reads `./analysis/data/feed_audit.ndjson` and writes reports
  under `./analysis/reports/`

## Docker

```bash
# One-shot crawl
docker compose run --rm crawler

# Long-running podping listener
docker compose up -d podping
```

See the main [docker-compose.yml](https://github.com/dardevelin/stophammer/blob/main/docker-compose.yml)
for full service definitions.

## License

AGPL-3.0-only — see [LICENSE](LICENSE).
