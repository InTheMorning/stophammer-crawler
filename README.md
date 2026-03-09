# stophammer-crawler

RSS feed crawler for the [stophammer](https://github.com/dardevelin/stophammer) V4V music index.

Fetches a list of RSS feed URLs, parses them, and POSTs each one to a stophammer node's
`/ingest/feed` endpoint. Runs as a one-shot process — schedule it externally with cron,
a systemd timer, or a container orchestrator.

## Requirements

- [Bun](https://bun.sh) runtime

## Usage

Feed URLs can be supplied three ways (first match wins):

```bash
# 1. File argument
CRAWL_TOKEN=secret bun run src/index.ts feeds.txt

# 2. Environment variable (newline-separated)
CRAWL_TOKEN=secret \
FEED_URLS="https://feeds.rssblue.com/stereon-music
https://feeds.rssblue.com/ainsley-costello-love-letter" \
bun run src/index.ts

# 3. stdin
cat feeds.txt | CRAWL_TOKEN=secret bun run src/index.ts
```

Lines starting with `#` and blank lines are ignored.

## Environment variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `CRAWL_TOKEN` | yes | — | Shared secret checked by the stophammer node |
| `INGEST_URL` | no | `http://localhost:8008/ingest/feed` | Stophammer ingest endpoint |
| `CONCURRENCY` | no | `5` | Number of feeds fetched in parallel |

## Scheduling with cron

```
0 */6 * * *  CRAWL_TOKEN=secret INGEST_URL=http://primary:8008/ingest/feed \
             FEED_URLS="$(cat /etc/stophammer/feeds.txt)" \
             bun run /opt/stophammer-crawler/src/index.ts
```

## Docker (with stophammer compose stack)

```bash
cd hey-v4v
CRAWL_TOKEN=secret FEED_URLS="https://feeds.rssblue.com/stereon-music" \
  docker compose run --rm crawler
```

## Exit codes

- `0` — all feeds processed (individual rejections are logged, not fatal)
- `1` — missing `CRAWL_TOKEN`, no URLs provided, or unrecoverable startup error

## License

AGPL-3.0-only — see [LICENSE](LICENSE).
