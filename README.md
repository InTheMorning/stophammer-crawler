# stophammer-crawler

Unified feed crawler for the
[stophammer](https://github.com/inthemorning/stophammer) V4V music index.
Fetches RSS feeds, hashes content, parses with the `stophammer-parser` Rust
library dependency, and submits results to a stophammer node's
`/ingest/feed` endpoint.

Three subcommands cover every discovery path:

- **crawl** — one-shot URL list (file, args, env, or stdin)
- **import** — batch scan a PodcastIndex SQLite snapshot with resume cursor
- **gossip** — long-running gossip-listener SSE consumer with optional archive replay

## Requirements

- Rust 1.85+ (edition 2024)
- For import mode: a [PodcastIndex](https://podcastindex.org) database snapshot

## Installation

### Published artifacts

The recommended operator install surfaces are published from the main
[`stophammer`](https://github.com/inthemorning/stophammer) release pipeline:

- `stophammer-crawler-<version>.tar.gz`
- `ghcr.io/<owner>/stophammer-crawler`
- the Arch `stophammer-crawler` package built from the main repo packaging assets

The crawler release bundle includes:

- `stophammer-crawler`
- systemd units for long-running `gossip` and optional one-shot `crawl` /
  `import` runs
- example env files
- `sysusers.d` / `tmpfiles.d` snippets

### Build from source

Source builds require a sibling `stophammer-parser` checkout because the crawler
depends on it via a local Cargo path dependency:

```bash
git clone https://github.com/inthemorning/stophammer-parser
git clone https://github.com/inthemorning/stophammer-crawler

cd stophammer-crawler
cargo build --release --bins
```

The container build does not require that sibling checkout in the Docker
context. [Dockerfile](Dockerfile) clones `stophammer-parser` from Git during
the builder stage and accepts optional build args:

```bash
docker build \
  --build-arg STOPHAMMER_PARSER_REF=main \
  -t stophammer-crawler .
```

That produces:

- `target/release/stophammer-crawler`
- analysis binaries such as `feed_audit`, `audit_import`,
  `audit_expand_publishers`, and `musicl_backfill`

Quick checks from this checkout:

```bash
cargo run -- crawl --help
cargo run -- import --help
cargo run -- gossip --help

cargo run --bin feed_audit -- --help
cargo run --bin audit_analyzer -- --help
cargo run --bin audit_import -- --help
cargo run --bin audit_expand_publishers -- --help
cargo run --bin musicl_backfill -- --help
```

## Usage

The examples below assume `stophammer-crawler` is on your `PATH`. After a local
source build, use `./target/release/stophammer-crawler` instead.

### crawl

Fetch and ingest a list of feed URLs:

```bash
# From arguments
CRAWL_TOKEN=secret \
INGEST_URL=http://127.0.0.1:8008/ingest/feed \
stophammer-crawler crawl https://example.com/feed.xml

# From a file
CRAWL_TOKEN=secret \
INGEST_URL=http://127.0.0.1:8008/ingest/feed \
stophammer-crawler crawl feeds.txt

# From env
CRAWL_TOKEN=secret \
INGEST_URL=http://127.0.0.1:8008/ingest/feed \
FEED_URLS="https://a.com/feed,https://b.com/feed" \
stophammer-crawler crawl

# From stdin
cat urls.txt | CRAWL_TOKEN=secret \
  INGEST_URL=http://127.0.0.1:8008/ingest/feed \
  stophammer-crawler crawl
```

### import

Batch-scan a PodcastIndex snapshot for music feeds:

```bash
CRAWL_TOKEN=secret \
INGEST_URL=http://127.0.0.1:8008/ingest/feed \
stophammer-crawler import \
  --batch 100 --concurrency 5
```

Normal snapshot import excludes Wavlake-hosted rows. Use `--wavlake-only` for
the Wavlake-specific pass. The default `all_feeds` scope now jumps to the
music-first PodcastIndex lower bound instead of replaying the full pre-music
corpus from `0`.

Restart or resume from an explicit `PodcastIndex` id:

```bash
CRAWL_TOKEN=secret \
INGEST_URL=http://127.0.0.1:8008/ingest/feed \
stophammer-crawler import \
  --cursor 5000000 \
  --batch 100 --concurrency 5
```

Wavlake-only import from the same snapshot:

```bash
CRAWL_TOKEN=secret \
INGEST_URL=http://127.0.0.1:8008/ingest/feed \
stophammer-crawler import \
  --wavlake-only \
  --cursor 0
```

If `--db` does not exist yet, the importer downloads the latest PodcastIndex
snapshot archive from `https://public.podcastindex.org/podcastindex_feeds.db.tgz`,
extracts the `.db` directly into place, and does not keep the `.tgz` on disk.
Use `--refresh-db` to check the remote snapshot with `If-Modified-Since` and
download only when it changed.
Downloads are streamed through gzip/tar extraction, so RAM usage stays low even
for multi-gigabyte snapshots.

#### Import options

| Flag | Default | Description |
| ---- | ------- | ----------- |
| `--db <path>` | `./podcastindex_feeds.db` | PodcastIndex snapshot path |
| `--db-url <url>` | (public PI URL) | Snapshot archive URL |
| `--refresh-db` | off | Conditionally refresh the snapshot if the remote archive changed |
| `--state <path>` | `./import_state.db` | Progress cursor database |
| `--skip-db <path>` | `./feed_skip.db` | Shared cross-mode skip database |
| `--batch <n>` | `100` | Feeds per DB query batch |
| `--concurrency <n>` | `5` | Parallel fetch+ingest workers |
| `--audit-output <path>` | off | Optional NDJSON dump of successfully ingested `200 OK` feeds in `feed_audit` format |
| `--audit-replace` | off | Replace `--audit-output` instead of appending to it |
| `--dry-run` | off | Log without fetching/ingesting |
| `--skip-known-non-music` | off | Skip rows already known to fail the music/publisher medium gate, including non-`music` mediums and absent `podcast:medium` |
| `--skip-known-success` | off | Skip rows already known to have reached `accepted`, `no_change`, or `skipped_known_success` in importer memory |
| `--cursor <id>` | stored cursor | Override the stored or music-first start cursor |
| `--wavlake-only` | off | Restrict snapshot import to `wavlake.com` / `www.wavlake.com` feeds; without this flag, normal import excludes Wavlake rows |
| `--cursor <id>` | stored cursor | Start from an explicit PodcastIndex id instead of the stored cursor |

Progress is stored in `--state`. If the process is interrupted,
the next run resumes from the last completed batch. A crash
mid-batch re-processes that batch -- safe because stophammer
deduplicates on content hash. `--cursor <id>` overrides the stored
starting point for that run; in non-dry runs the override is also
persisted to the state DB before the batch loop starts. Cursor state is
scoped by import mode inside the state DB, so normal import and
`--wavlake-only` do not overwrite each other's resume positions.

`--state` now stores both the batch cursor and durable per-row importer memory
in `import_feed_memory`, including the latest fetch status, outcome,
`raw_medium`, `attempt_duration_ms`, and attempt counter for each attempted
PodcastIndex row.

Import mode now also guards each feed crawl with a hard deadline. If a feed
gets stuck below the normal HTTP timeout layer, the importer logs an explicit
timeout error for that row, records a retryable failure in `import_feed_memory`,
and continues. Import mode does not retry failed rows within the same run; the
state DB is the retry/memory mechanism. Slow batches emit heartbeat lines with
the currently pending row IDs and URLs instead of going silent.

`--wavlake-only` is intentionally slower than normal import mode. Normal import
skips Wavlake-hosted snapshot rows entirely. The Wavlake-specific mode queries
only those rows, forces single-flight fetches, applies a small delay between
requests, and if Wavlake returns `429 Too Many Requests` it backs off using the
server's `Retry-After` value before attempting the next feed. Each Wavlake
`429` also increases the ongoing inter-fetch delay by 1 second for the rest of
that run. Wavlake scope does not imply any skip policy; use
`--skip-known-success` and/or `--skip-known-non-music` if you want to trade
freshness for speed on reruns.

If `--audit-output` is set, import mode writes only feeds that were actually
ingested by stophammer to an NDJSON file compatible with the `feed_audit` /
`audit_import` tooling. That includes newly accepted feeds and `no_change`
re-submissions of already-ingested feeds. Rejected feeds, parse errors, and
non-`200` fetches are not written. By default, `--audit-output` appends and the
writer de-dupes by `source_db.feed_guid` plus `fetch.content_sha256` so reruns
do not append the same fetched body again. Use `--audit-replace` to truncate
and rewrite the file instead. Import mode also creates an adjacent lock file
while writing the audit NDJSON, so only one importer can target a given audit
path at a time.

Operational note:

- initial auto-download only needs space for the extracted `.db`
- `--refresh-db` temporarily needs room for both the existing `.db` and the new
  replacement `.db` while the importer swaps them safely

### gossip

Listen to a local gossip-listener SSE stream for live podping notifications.

#### Prerequisites

- A running `gossip-listener` instance from `podping.alpha` (provides the SSE
  stream and archive)
- The crawler process must have read access to the archive database
- `CRAWL_TOKEN` and `INGEST_URL` environment variables set

Typical host setup:

1. Install and start `podping.alpha` / `gossip-listener`
2. Enable archive writing in that service
3. Confirm the resulting `archive.db` path on disk
4. Point `--archive-db` at that path

If the archive does not live at the default path, pass the actual path:

```bash
stophammer-crawler gossip --archive-db /some/other/path/archive.db
```

For packaged systemd installs, the usual pattern is to add the
`stophammer-crawler` service user to the `podping` group so it can read the
archive written by `gossip-listener`.

For Docker installs, the root repo's reference `docker-compose.yml` now runs
`podping.alpha`'s `gossip-listener` as a sibling `podping` service and shares
its `archive.db` with the crawler over a named Docker volume. That default path
inside the crawler container is:

- `GOSSIP_ARCHIVE_DB=/podping/archive.db`
- `GOSSIP_SSE_URL=http://podping:8089/events`

#### Archive-backed mode (recommended)

Uses gossip-listener's `archive.db` as a durable podping backlog. Survives
restarts, SSE disconnects, and process crashes without losing notifications.

First-time bootstrap (catches up on the last 24 hours of podpings):

```bash
CRAWL_TOKEN=secret \
INGEST_URL=http://127.0.0.1:8008/ingest/feed \
stophammer-crawler gossip \
  --archive-db /path/to/archive.db \
  --since-hours 24 \
  --skip-known-non-music \
  --skip-ttl-days 30
```

Subsequent runs resume from the stored cursor automatically:

```bash
CRAWL_TOKEN=secret \
INGEST_URL=http://127.0.0.1:8008/ingest/feed \
stophammer-crawler gossip \
  --archive-db /path/to/archive.db \
  --skip-known-non-music \
  --skip-ttl-days 30
```

On startup, the crawler validates the archive schema, checks cursor continuity
(exits fatally if notifications were lost), replays missed notifications in
batches of ~500 with backpressure, then connects to SSE for live events.
A background reconciliation task queries the archive periodically (10 s, then
60 s steady-state, backing off to 5 min when idle) to catch anything SSE missed.

If the stored cursor is older than the oldest retained archive row, the crawler
exits with a fatal error. Delete `gossip_state.db` to re-bootstrap.

#### Live-only mode (best-effort)

Without `--archive-db`, gossip mode streams SSE events only. Not restart-safe
— any events that arrive while the process is down are lost.

```bash
CRAWL_TOKEN=secret \
INGEST_URL=http://127.0.0.1:8008/ingest/feed \
stophammer-crawler gossip
```

#### Feed memory and skip logic

`gossip_state.db` records the latest crawl result for each feed URL (HTTP
status, outcome, medium, attempt count). With `--skip-known-non-music`, feeds
proven irrelevant by a prior crawl are skipped on future notifications:

- HTTP 200 with a non-music, non-publisher `raw_medium`, or
- a `[medium_music]` rejection (including absent `podcast:medium`)

Fetch errors (404, 429, timeouts), parse errors, and prior successful feeds are
never skipped. `--skip-ttl-days <n>` expires skip decisions after N days so
feeds are periodically re-evaluated.

#### Gossip options

| Flag | Default | Description |
| ---- | ------- | ----------- |
| `--state <path>` | `./gossip_state.db` | Cursor and feed memory database |
| `--skip-db <path>` | `./feed_skip.db` | Shared cross-mode skip database |
| `--sse-url <url>` | `http://localhost:8089/events` | SSE endpoint URL |
| `--archive-db <path>` | off | gossip-listener archive database path |
| `--since-hours <n>` | off | Bootstrap from N hours ago (requires `--archive-db`) |
| `--concurrency <n>` | `3` | Parallel fetch+ingest workers |
| `--skip-known-non-music` | off | Skip feeds proven non-music by prior crawl |
| `--skip-ttl-days <n>` | off | Re-evaluate skip decisions after N days |
| `-q, --quiet` | off | Hide non-music medium rejections |

## Environment variables

- **`CRAWL_TOKEN`** (required) --
  Shared secret for stophammer ingest auth.
- **`INGEST_URL`** --
  Stophammer ingest endpoint.
  Default: `http://localhost:8008/ingest/feed`
- **`CONCURRENCY`** --
  Worker pool size.
  Default: `5` (crawl/import) / `3` (gossip)
- **`FEED_URLS`** --
  Comma- or newline-separated URLs (crawl mode only).
- **`PODCASTINDEX_DB_URL`** --
  Override the PodcastIndex snapshot archive URL for
  import mode.
- **`RESOLVER_DB_PATH`** --
  If set, import mode runs
  `resolverctl import-active` at start, refreshes that
  heartbeat while the import is active, and runs
  `import-idle` on exit.
- **`RESOLVERCTL_BIN`** --
  Override the resolver control binary used with
  `RESOLVER_DB_PATH`. Default: `stophammer-resolverctl`.

## Architecture

```text
stophammer-crawler
  src/
    main.rs           CLI dispatcher (clap subcommands)
    crawl.rs          Shared pipeline: fetch → SHA-256 → parse → POST
    pool.rs           Bounded concurrency pool (tokio semaphore)
    dedup.rs          In-memory cooldown map (gossip mode)
    feed_skip.rs      Shared skip-memory database for proven irrelevant feeds
    modes/
      batch.rs        Load URLs from file/env/stdin, run pool
      import.rs       PodcastIndex DB batches, resume cursor, fallback GUID
      gossip.rs       SSE listener and optional archive replay for gossip-listener
```

The core pipeline in `crawl.rs` calls `stophammer-parser` as a Rust library — no
subprocess spawning. Every mode feeds URLs into the same `crawl_feed()` function:

1. **Fetch** the RSS feed via `reqwest`
2. **Hash** the raw response body with SHA-256
3. **Parse** the XML with `stophammer-parser::profile::stophammer()` (or
   `stophammer_with_fallback()` for import mode with PodcastIndex GUIDs)
4. **POST** the result as JSON to the stophammer `/ingest/feed` endpoint

Concurrency is bounded by a tokio semaphore — crawl and import modes drain a
fixed task list; gossip mode runs an unbounded stream with a permit-based cap.

When `RESOLVER_DB_PATH` is set, import mode also brackets the run with
`resolverctl import-active` / `import-idle` and refreshes that import heartbeat
while the bulk import is in progress. This is intended for single-host
deployments where the importer and `stophammer` share access to the same
primary database path. If the importer crashes, `resolverd` will eventually
ignore the stale heartbeat and resume draining the queue.

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
- `cargo run --bin audit_expand_publishers -- ...`
- `cargo run --bin musicl_backfill -- ...`

By default:

- `feed_audit` reads `./analysis/data/stophammer-feeds.db` and writes
  successful `200 OK` XML captures to `./analysis/data/feed_audit.ndjson`
- `feed_audit` writes retryable / failed feed URLs to
  `./analysis/data/failed_feeds.txt`
- `feed_audit` can also fetch a plain-text URL list via `--urls-file` and
  append successful captures back into an existing NDJSON corpus with `--append`
- `audit_analyzer` reads `./analysis/data/feed_audit.ndjson` and writes reports
  under `./analysis/reports/`
- `audit_import` reads `./analysis/data/feed_audit.ndjson` and replays cached
  feeds into `/ingest/feed` using `./analysis/data/audit_import_state.db` as its
  resume cursor
- `audit_import` retries transient ingest throttles / failures such as `429`
  before counting the row as an ingest error
- `musicl_backfill` scans crawler import state DBs for successful `musicL`
  discoveries, compares them against `stophammer.db`, and re-fetches only the
  missing feeds through the normal ingest path
- `crawl` writes retryable feed URLs to `./failed_feeds.txt` unless you
  override `--failed-feeds-output`
- `crawl` also spaces requests per host (`--host-delay-ms`, default `1500`) and
  retries transient fetch / ingest throttles before writing a URL to the failed
  feed dump

Examples:

```bash
# Create / refresh the cached NDJSON corpus
cargo run --bin feed_audit -- \
  --db ./analysis/data/stophammer-feeds.db \
  --output ./analysis/data/feed_audit.ndjson \
  --failed-feeds-output ./analysis/data/failed_feeds.txt

# Refill only missing feeds from failed_feeds.txt back into the same corpus
cargo run --bin feed_audit -- \
  --urls-file ./analysis/data/failed_feeds.txt \
  --output ./analysis/data/feed_audit.ndjson \
  --append \
  --failed-feeds-output ./analysis/data/failed_feeds.txt \
  --success-delay-ms 2000 \
  --failure-backoff-secs 30 \
  --max-backoff-secs 600

# Re-analyze the cached corpus
cargo run --bin audit_analyzer -- \
  --input ./analysis/data/feed_audit.ndjson

# Replay cached feeds into a running primary
CRAWL_TOKEN=secret \
INGEST_URL=http://127.0.0.1:8008/ingest/feed \
cargo run --bin audit_import -- \
  --input ./analysis/data/feed_audit.ndjson \
  --reset

# Discover and optionally fetch publisher feeds referenced by the audit corpus
cargo run --bin audit_expand_publishers -- \
  --input ./analysis/data/feed_audit.ndjson \
  --dry-run

# Dry-run musicL backfill from crawler state DBs
CRAWL_TOKEN=secret \
INGEST_URL=http://127.0.0.1:8008/ingest/feed \
cargo run --bin musicl_backfill -- \
  --stophammer-db ./stophammer.db \
  --dry-run

# Real musicL backfill
CRAWL_TOKEN=secret \
INGEST_URL=http://127.0.0.1:8008/ingest/feed \
cargo run --bin musicl_backfill -- \
  --stophammer-db ./stophammer.db \
  --concurrency 5

# Live crawl with per-host spacing and retry dumps
CRAWL_TOKEN=secret \
INGEST_URL=http://127.0.0.1:8008/ingest/feed \
cargo run -- crawl \
  --concurrency 5 \
  --host-delay-ms 1500 \
  --failed-feeds-output ./failed_feeds.txt \
  ./feeds.txt
```

If `--state-db` is omitted, `musicl_backfill` scans:

- `./import_state.db`
- `./import_state_wavlake.db`

Candidate selection is intentionally narrow:

- `fetch_http_status = 200`
- `lower(raw_medium) = 'musicl'`
- `parsed_feed_guid IS NOT NULL`
- feed GUID not already present in `stophammer.db`

`musicl_backfill` reads crawler state DBs only; it does not mutate importer
progress. It dedupes candidates by parsed feed GUID across state DBs, re-fetches
only missing candidate URLs through the normal crawl+ingest path, and uses a
short fetch timeout plus a hard per-feed deadline so bad hosts do not stall the
run indefinitely.

Useful verification queries:

```bash
sqlite3 ./import_state.db "
SELECT COUNT(*)
FROM import_feed_memory
WHERE fetch_http_status = 200
  AND lower(raw_medium) = 'musicl'
  AND parsed_feed_guid IS NOT NULL;"
```

```bash
sqlite3 ./stophammer.db "SELECT COUNT(*) FROM feeds WHERE lower(raw_medium) = 'musicl';"
```

```bash
sqlite3 ./stophammer.db "
SELECT COUNT(*)
FROM resolver_queue rq
JOIN feeds f ON f.feed_guid = rq.feed_guid
WHERE lower(f.raw_medium) = 'musicl';"
```

## Docker

Published releases also push a crawler runtime image:

- `ghcr.io/<owner>/stophammer-crawler`

This repo does not ship a general-purpose crawler compose stack. For day-to-day
operation, run the binary directly or provide your own scheduler / compose
deployment around `stophammer-crawler gossip`, `import`, or `crawl`.

## License

AGPL-3.0-only — see [LICENSE](LICENSE).
