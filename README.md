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
