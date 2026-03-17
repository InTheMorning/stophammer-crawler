# Analysis Workspace

This folder holds crawler-adjacent audit tools and local corpus data.

- `bin/`: Rust binaries for feed audits, corpus analysis, and cached-feed replay
- `data/`: local SQLite snapshots and NDJSON captures
- `reports/`: generated summaries, cluster artifacts, and notes

These files are intentionally separate from the crawler's operational code in
`src/`.
