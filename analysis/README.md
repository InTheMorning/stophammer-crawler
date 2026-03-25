# Analysis Workspace

This folder holds crawler-adjacent audit tools and local corpus data.

- `bin/`: Rust binaries for feed audits, corpus analysis, and cached-feed replay
- `data/`: local SQLite snapshots and NDJSON captures
- `reports/`: generated summaries, cluster artifacts, and notes

These files are intentionally separate from the crawler's operational code in
`src/`.

Typical usage from the repo root:

```bash
# 1. Build or run the analysis binaries through the crawler manifest
cargo run --manifest-path stophammer-crawler/Cargo.toml \
  --bin feed_audit -- --help
cargo run --manifest-path stophammer-crawler/Cargo.toml \
  --bin audit_analyzer -- --help
cargo run --manifest-path stophammer-crawler/Cargo.toml \
  --bin audit_import -- --help
cargo run --manifest-path stophammer-crawler/Cargo.toml \
  --bin audit_expand_publishers -- --help

# 2. Capture a feed corpus into NDJSON
cargo run --manifest-path stophammer-crawler/Cargo.toml --bin feed_audit -- \
  --db ./stophammer-crawler/analysis/data/stophammer-feeds.db \
  --output ./stophammer-crawler/analysis/data/feed_audit.ndjson \
  --failed-feeds-output ./stophammer-crawler/analysis/data/failed_feeds.txt

# 2b. Append only the missing feeds back into the same NDJSON corpus
cargo run --manifest-path stophammer-crawler/Cargo.toml --bin feed_audit -- \
  --urls-file ./stophammer-crawler/analysis/data/failed_feeds.txt \
  --output ./stophammer-crawler/analysis/data/feed_audit.ndjson \
  --append \
  --failed-feeds-output ./stophammer-crawler/analysis/data/failed_feeds.txt \
  --success-delay-ms 2000 \
  --failure-backoff-secs 30 \
  --max-backoff-secs 600

# 3. Re-analyze the captured corpus
cargo run --manifest-path stophammer-crawler/Cargo.toml \
  --bin audit_analyzer -- \
  --input ./stophammer-crawler/analysis/data/feed_audit.ndjson

# 4. Append missing publisher feeds discovered from cached raw_xml
cargo run --manifest-path stophammer-crawler/Cargo.toml \
  --bin audit_expand_publishers -- \
  --input ./stophammer-crawler/analysis/data/feed_audit.ndjson \
  --output ./stophammer-crawler/analysis/data/feed_audit.ndjson

# 5. Replay cached feeds into a running primary without refetching them
CRAWL_TOKEN=secret \
INGEST_URL=http://127.0.0.1:8008/ingest/feed \
cargo run --manifest-path stophammer-crawler/Cargo.toml --bin audit_import -- \
  --input ./stophammer-crawler/analysis/data/feed_audit.ndjson \
  --reset
```

`feed_audit` now keeps only successful `200 OK` captures in the NDJSON corpus.
Retryable and failed feed URLs are written separately to
`analysis/data/failed_feeds.txt` so they can be requeued later.
`audit_expand_publishers` reparses cached `raw_xml`, extracts feed-level
`podcast:remoteItem medium="publisher"` targets, skips feeds already present in
the corpus by GUID or URL, writes the missing target URLs to
`analysis/data/missing_publisher_feeds.txt`, and appends successful publisher
fetches back into the NDJSON corpus.
`audit_import` retries transient ingest throttles such as `429 Too Many Requests`
before treating a row as an ingest error.

For live URL lists, `stophammer-crawler crawl` now spaces requests per host and
retries transient throttles before writing retryable URLs to `failed_feeds.txt`.
