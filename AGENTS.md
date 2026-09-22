# stophammer-crawler Agent Guidelines

Fetches RSS feeds, hashes the content, parses it with `stophammer-parser`, and
submits the result to a stophammer node at `/ingest/feed`. It is a client of
that node. It holds no index and no authority.

Follow the `project-baseline` skill. It holds the shared working rules.

`stophammer/docs/adr/` owns every decision that shapes this crate.
`stophammer/docs/adr/README.md` is the index. `README.md` here holds the
operator command lines and the environment variables.

## Where The Work Stands

2026-09-22: `stophammer` ADR 0043 is Accepted. A feed corrects itself on the
next read. A feed with unchanged content needs `FORCE_REINGEST`, because the
node stops an unchanged feed.

Pending work here: limit a forced pass to the feeds the node already holds.
`FORCE_REINGEST` is one global flag today. `src/crawl.rs:350` adds
`force_reingest` to every `/ingest/feed` payload of the run, whatever the mode.
A forced `import` run would therefore also ingest feeds the index never held,
and would grow the corpus during a corrective pass. Until that limit exists,
run a trickle from a feed list exported from the index.

## What Is True Here

- **This crawler is untrusted.** ADR 0006. The node verifies what this crate
  sends. Never move a check out of the node to save a request.
- **Four modes**, in `src/modes/`: `feed`, `import`, `ndjson` and `gossip`.
  `crawl` is an alias of `feed`. There is no `podping` mode. The `gossip` mode
  consumes the stream that carries podping notifications.
- **The node owns the index.** Local SQLite holds progress and memory of
  attempts only.
- Lints are `[lints.clippy] pedantic = "deny"` and nothing more.
- Tests are inline `#[cfg(test)]` modules. There is no `tests/` directory.

## Commits

`git@github.com:InTheMorning/stophammer-crawler.git`. This crate depends on
`stophammer-parser` by the path `../stophammer-parser`.
