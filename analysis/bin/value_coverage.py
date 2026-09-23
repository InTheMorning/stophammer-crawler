#!/usr/bin/env python3
"""Measure podcast:value coverage for ADR 0048.

Classifies each feed by where its payment routes live, applying the same
validity test as `validate_routes` in src/verifiers/v4v_payment.rs: a route is
valid when the address is not empty and the split is above 0.

Two inputs:
  --urls FILE      one feed URL per line (from refresh_log_summary.sh)
  --ndjson FILE    a feed_audit.ndjson snapshot (reads raw_xml, no network)

Usage:
  ./value_coverage.py --urls v4v-rejected.txt --delay 1.5
  ./value_coverage.py --ndjson ../data/feed_audit.ndjson
"""

import argparse
import json
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from collections import Counter

EXEMPT_MEDIUMS = {"publisher", "musicl"}
UA = "stophammer-analysis/1.0"


JUDGED = {"accepted_today", "ADR0048_would_accept", "refused_partial_coverage",
          "refused_no_routes_anywhere", "refused_no_tracks"}


def local(tag):
    return tag.rsplit("}", 1)[-1]


def has_valid_route(value_node):
    """Mirror validate_routes: one recipient with an address and a split above 0."""
    for child in value_node:
        if local(child.tag) != "valueRecipient":
            continue
        address = child.attrib.get("address", "")
        try:
            split = float(child.attrib.get("split", "0"))
        except ValueError:
            split = 0.0
        if address and split > 0:
            return True
    return False


def classify(xml_text):
    """Return (medium, feed_has_route, items, items_covered, items_invalid)."""
    root = ET.fromstring(xml_text)
    channel = next(c for c in root if local(c.tag) == "channel")
    medium = None
    feed_has_route = False
    items = items_covered = items_invalid = 0
    for child in channel:
        tag = local(child.tag)
        if tag == "medium" and child.text:
            medium = child.text.strip().lower()
        elif tag == "value":
            if has_valid_route(child):
                feed_has_route = True
        elif tag == "item":
            items += 1
            declared = covered = False
            for grandchild in child:
                if local(grandchild.tag) == "value":
                    declared = True
                    if has_valid_route(grandchild):
                        covered = True
            if covered:
                items_covered += 1
            elif declared:
                items_invalid += 1
    return medium, feed_has_route, items, items_covered, items_invalid


def verdict(medium, feed_has_route, items, items_covered):
    if medium in EXEMPT_MEDIUMS:
        return "exempt_medium"
    if medium != "music":
        return "not_music"
    if feed_has_route:
        return "accepted_today"
    if items == 0:
        return "refused_no_tracks"
    if items_covered == items:
        return "ADR0048_would_accept"
    if items_covered > 0:
        return "refused_partial_coverage"
    return "refused_no_routes_anywhere"


def fetch(url, timeout):
    """Fetch through curl, which carries the system CA bundle that Python lacks."""
    result = subprocess.run(
        ["curl", "-sSL", "--fail", "-m", str(int(timeout)), "-A", UA, url],
        capture_output=True, check=True)
    return result.stdout.decode("utf-8", "replace")


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--urls", help="file with one feed URL per line")
    source.add_argument("--ndjson", help="feed_audit.ndjson snapshot")
    parser.add_argument("--delay", type=float, default=1.5,
                        help="seconds between fetches (default 1.5; raise for Wavlake)")
    parser.add_argument("--timeout", type=float, default=25.0)
    parser.add_argument("--detail", help="write a per-feed TSV report here")
    args = parser.parse_args()

    counts = Counter()
    rows = []
    fallback_feeds = 0
    tracks_total = tracks_no_block = 0

    if args.urls:
        with open(args.urls, encoding="utf-8") as handle:
            urls = [line.strip() for line in handle if line.strip()]
        for index, url in enumerate(urls, 1):
            if index > 1 and args.delay:
                time.sleep(args.delay)
            try:
                medium, feed_ok, items, covered, invalid = classify(
                    fetch(url, args.timeout))
            except Exception as error:                      # noqa: BLE001
                counts["error"] += 1
                rows.append((url, "error", "", "", "", type(error).__name__))
                print(f"[{index}/{len(urls)}] error {url}: {error}", file=sys.stderr)
                continue
            key = verdict(medium, feed_ok, items, covered)
            counts[key] += 1
            if key in JUDGED:
                tracks_total += items
                tracks_no_block += items - covered - invalid
                if key == "accepted_today" and items - covered - invalid > 0:
                    fallback_feeds += 1
            rows.append((url, key, medium or "", items, covered, invalid))
            print(f"[{index}/{len(urls)}] {key} {url}", file=sys.stderr)
    else:
        with open(args.ndjson, encoding="utf-8") as handle:
            for line in handle:
                try:
                    record = json.loads(line)
                except ValueError:
                    counts["error"] += 1
                    continue
                xml_text = record.get("raw_xml")
                if not xml_text:
                    counts["error"] += 1
                    continue
                try:
                    medium, feed_ok, items, covered, invalid = classify(xml_text)
                except Exception:                           # noqa: BLE001
                    counts["error"] += 1
                    continue
                url = record.get("source_db", {}).get("feed_url", "")
                key = verdict(medium, feed_ok, items, covered)
                counts[key] += 1
                if key in JUDGED:
                    tracks_total += items
                    tracks_no_block += items - covered - invalid
                    if key == "accepted_today" and items - covered - invalid > 0:
                        fallback_feeds += 1
                rows.append((url, key, medium or "", items, covered, invalid))

    order = ["accepted_today", "ADR0048_would_accept", "refused_partial_coverage",
             "refused_no_routes_anywhere", "refused_no_tracks",
             "exempt_medium", "not_music", "error"]
    print()
    for key in order:
        if counts[key]:
            print(f"  {key:28} {counts[key]:6}")

    judged = sum(counts[k] for k in order[:5])
    gain = counts["ADR0048_would_accept"]
    print(f"\nmusic feeds the verifier judges: {judged}")
    if judged:
        print(f"  accepted under the present rule: {counts['accepted_today']}")
        print(f"  ADR 0048 would also accept:      {gain}")
        print(f"  still refused:                   "
              f"{judged - counts['accepted_today'] - gain}")
    if tracks_total:
        print(f"\ntracks: {tracks_total}; relying on the feed-level fallback: "
              f"{tracks_no_block} ({100 * tracks_no_block / tracks_total:.1f}%)")
        print(f"accepted feeds with at least one such track: {fallback_feeds}")

    if args.detail:
        with open(args.detail, "w", encoding="utf-8") as handle:
            handle.write("url\tverdict\tmedium\titems\tcovered\tinvalid\n")
            for row in rows:
                handle.write("\t".join(str(field) for field in row) + "\n")
        print(f"\nwrote {len(rows)} rows to {args.detail}")


if __name__ == "__main__":
    main()
