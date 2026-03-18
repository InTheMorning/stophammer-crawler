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
cargo run --manifest-path stophammer-crawler/Cargo.toml --bin feed_audit -- --help
cargo run --manifest-path stophammer-crawler/Cargo.toml --bin audit_analyzer -- --help
cargo run --manifest-path stophammer-crawler/Cargo.toml --bin audit_import -- --help

# 2. Capture a feed corpus into NDJSON
cargo run --manifest-path stophammer-crawler/Cargo.toml --bin feed_audit -- \
  --db ./stophammer-crawler/analysis/data/stophammer-feeds.db \
  --output ./stophammer-crawler/analysis/data/feed_audit.ndjson

# 3. Re-analyze the captured corpus
cargo run --manifest-path stophammer-crawler/Cargo.toml --bin audit_analyzer -- \
  --input ./stophammer-crawler/analysis/data/feed_audit.ndjson

# 4. Replay cached feeds into a running primary without refetching them
CRAWL_TOKEN=secret \
INGEST_URL=http://127.0.0.1:8008/ingest/feed \
cargo run --manifest-path stophammer-crawler/Cargo.toml --bin audit_import -- \
  --input ./stophammer-crawler/analysis/data/feed_audit.ndjson \
  --reset
```
