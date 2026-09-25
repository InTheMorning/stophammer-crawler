#!/usr/bin/env python3
"""Export the fetch cache as NDJSON rows for the `ndjson` replay mode.

stophammer ADR 0051 section 6: after the node applies content only from a
feed's source URL, the repair applies each source URL body again with
`force_reingest`. The fetch cache of a completed `refresh` pass (ADR 0050)
holds those bodies. This script writes them in the input format of
`stophammer-crawler ndjson`, so the repair sends no request to a feed host.

The script exports a cache row only when its URL is the stored `feed_url` of a
feed in the node database. That URL is the source URL, and it is the URL that
the `refresh` mode requested. A row for any other URL, for example a URL from
a publisher-link wave, is skipped.

Both databases are opened read-only. The script writes only the output file.

Usage:
  ./export-feed-cache-ndjson.py \\
      --cache /data/feed_cache.db \\
      --node-db /data/stophammer.db \\
      --output /data/repair.ndjson \\
      [--since UNIX_SECONDS]

Then:
  stophammer-crawler --force ndjson --input /data/repair.ndjson \\
      --state /data/repair_state.db --reset

Standard library only. Python 3.9 or later.
"""

import argparse
import gzip
import hashlib
import json
import sqlite3
import sys
from pathlib import Path


def open_read_only(path):
    """Open an SQLite file read-only. Fail when the file does not exist."""
    if not Path(path).is_file():
        sys.exit(f"export: no such database file: {path}")
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)


def load_source_feeds(node_db):
    """Map each stored feed_url to (feed_guid, title)."""
    rows = node_db.execute("SELECT feed_url, feed_guid, title FROM feeds")
    return {url: (guid, title) for url, guid, title in rows}


def cache_rows(cache_db, since):
    """Yield (url, final_url, content_sha256, body_gzip, fetched_at), by URL."""
    query = (
        "SELECT url, final_url, content_sha256, body_gzip, fetched_at "
        "FROM feed_cache WHERE fetched_at >= ? ORDER BY url"
    )
    yield from cache_db.execute(query, (since,))


def export(cache_path, node_db_path, output_path, since):
    """Write one NDJSON row for each cache row at a source URL. Return counts."""
    counts = {
        "cache_rows": 0,
        "exported": 0,
        "skipped_not_a_source_url": 0,
        "skipped_undecodable": 0,
        "hash_mismatch_exported": 0,
    }
    node_db = open_read_only(node_db_path)
    cache_db = open_read_only(cache_path)
    sources = load_source_feeds(node_db)
    node_db.close()

    with open(output_path, "w", encoding="utf-8") as out:
        for url, final_url, content_sha256, body_gzip, _fetched_at in cache_rows(
            cache_db, since
        ):
            counts["cache_rows"] += 1
            source = sources.get(url)
            if source is None:
                counts["skipped_not_a_source_url"] += 1
                continue
            try:
                body_bytes = gzip.decompress(body_gzip)
                raw_xml = body_bytes.decode("utf-8")
            except (OSError, EOFError, UnicodeDecodeError) as err:
                counts["skipped_undecodable"] += 1
                print(f"export: skipped {url}: {err}", file=sys.stderr)
                continue
            # The crawler hashes the fetched bytes. The cache keeps the body
            # as text, so a body that was not valid UTF-8 can differ. The
            # repair uses force_reingest, so the hash does not gate it.
            if hashlib.sha256(body_bytes).hexdigest() != content_sha256:
                counts["hash_mismatch_exported"] += 1
            feed_guid, title = source
            row = {
                "source_db": {"feed_guid": feed_guid, "feed_url": url, "title": title},
                "fetch": {
                    "final_url": final_url,
                    "http_status": 200,
                    "content_sha256": content_sha256,
                    "error": None,
                },
                "raw_xml": raw_xml,
            }
            out.write(json.dumps(row, ensure_ascii=False))
            out.write("\n")
            counts["exported"] += 1

    cache_db.close()
    return counts


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--cache", required=True, help="path to feed_cache.db")
    parser.add_argument("--node-db", required=True, help="path to the primary node database")
    parser.add_argument("--output", required=True, help="NDJSON file to write")
    parser.add_argument(
        "--since",
        type=int,
        default=0,
        help="export only rows fetched at or after this Unix time (default: all)",
    )
    args = parser.parse_args()

    counts = export(args.cache, args.node_db, args.output, args.since)
    for key, value in counts.items():
        print(f"{key}: {value}", file=sys.stderr)
    if counts["exported"] == 0:
        sys.exit("export: no row exported")


if __name__ == "__main__":
    main()
