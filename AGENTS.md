# stophammer-crawler Agent Guidelines

Fetches RSS feeds, hashes the content, parses it with `stophammer-parser`, and
submits the result to a stophammer node at `/ingest/feed`. It is a client of
that node. It holds no index and no authority.

Follow the `project-baseline` skill. It holds the shared working rules.

`stophammer/docs/adr/` owns every decision that shapes this crate.
`stophammer/docs/adr/README.md` is the index. `README.md` here holds the
operator command lines and the environment variables.

## Where The Work Stands

2026-09-23: `stophammer` ADR 0043 is Accepted and deployed. A feed corrects
itself on the next read. A feed with unchanged content needs `--force`, because
the node stops an unchanged feed.

[stophammer ADR 0047](../docs/adr/0047-a-corrective-pass-reads-the-index.md) is
Accepted and implemented. The `refresh` mode in `src/modes/refresh.rs` takes
its corpus from the feed list of the node. The index is what defines the feeds
the node holds. The pass has run.

`FORCE_REINGEST` and `--force` stay global to a run. `src/crawl.rs:350` adds
`force_reingest` to each `/ingest/feed` payload, whatever the mode. Thus a
forced pass must use `refresh`, which reads only feeds the index holds. A
forced `import` run ingests feeds the index never held, and it grows the corpus
during a corrective pass.

## What Is True Here

- **This crawler is untrusted.** ADR 0006. The node verifies what this crate
  sends. Never move a check out of the node to save a request.
- **Five modes**, in `src/modes/`: `feed`, `import`, `ndjson`, `gossip` and
  `refresh`. `crawl` is an alias of `feed`. There is no `podping` mode. The
  `gossip` mode consumes the stream that carries podping notifications.
- **The node owns the index.** Local SQLite holds progress and memory of
  attempts only.
- Lints are `[lints.clippy] pedantic = "deny"` and nothing more.
- Tests are inline `#[cfg(test)]` modules. There is no `tests/` directory.

## Commits

`git@github.com:InTheMorning/stophammer-crawler.git`. This crate depends on
`stophammer-parser` by the path `../stophammer-parser`.
