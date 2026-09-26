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

[stophammer ADR 0049](../docs/adr/0049-publisher-relationships-are-rss-facts.md)
is Accepted and implemented here. `feed` and `refresh` follow a publisher
link, through `src/follow.rs` and `src/modes/batch.rs`. The walk has three
waves: the input feeds, the feeds they name, and the album list of a
publisher found through an album. `gossip` follows in two levels, in
`src/modes/gossip.rs`, with its own follow semaphore and host throttle.
Deployed on 2026-09-24.

[stophammer ADR 0052](../docs/adr/0052-a-source-moves-its-own-feed.md) task
005 is complete on 2026-09-25 and not deployed. Each feed fetch client uses
`redirect::Policy::none()`, and `fetch_following_redirects` in `src/crawl.rs`
follows at most 10 hops. The ingest request carries `redirects`, one entry for
each hop with its status. `follow_urls` also returns a declared
`itunes:new-feed-url`. The gossip SSE stream keeps its own client with the
default redirect policy. The node deploys first, because it accepts the new
field as optional.

[stophammer ADR 0054](../docs/adr/0054-a-fetch-reaches-only-public-feed-hosts.md)
task 001 is complete on 2026-09-25 and not deployed. `src/fetch_guard.rs` holds
`is_public_ip`, `check_target` and `PublicOnlyResolver`. Each feed fetch client
resolves through `PublicOnlyResolver`, and `fetch_following_redirects` calls
`check_target` before each hop. A rejected target is a final fetch error with
the reason `fetch_target_not_public`. `CrawlConfig::allow_private_targets` is
for tests with a local stub server only, and no environment value sets it.

The fetch cache keeps no row for a medium rejection or for an ingest answer
of `413` (`CrawlOutcome::keeps_no_cache_row`). The skip list stops the next
fetch of a non-music feed, and the node refuses a request body over 2 MiB each
time. On 2026-09-25, 221 rows with the answer `ingest_error` held 216 MiB of
the 252 MiB cache.

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
- **`feed`, `refresh` and `gossip` follow a publisher link.** ADR 0049 section
  2 owns the rule.
- **The node owns the index.** Local SQLite holds progress and memory of
  attempts only.
- Lints are `[lints.clippy] pedantic = "deny"` and nothing more.
- Tests are inline `#[cfg(test)]` modules. There is no `tests/` directory.

## Commits

`git@github.com:InTheMorning/stophammer-crawler.git`. This crate depends on
`stophammer-parser` by the path `../stophammer-parser`.
