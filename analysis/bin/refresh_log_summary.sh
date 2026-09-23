#!/usr/bin/env bash
# Summarize a crawler run log (refresh, feed, import, gossip).
#
# The crawler writes each feed outcome to stderr as:
#   "  <outcome>: <url>"
# where a rejection carries the owning verifier:
#   "  rejected: [v4v_payment] <message>: <url>"
#
# Capture a run with:  cargo run --release -- refresh 2>&1 | tee run.log
# Usage:               ./refresh_log_summary.sh run.log [out-dir]
set -euo pipefail

LOG=${1:?usage: refresh_log_summary.sh <run.log> [out-dir]}
OUT=${2:-.}
mkdir -p "$OUT"

echo "== corpus line (what the pass read) =="
grep -E "corpus|feeds to (crawl|refresh)" "$LOG" | head -5 || echo "  (none found)"

echo
echo "== outcomes =="
grep -oE '^[[:space:]]+(accepted|no_change|rejected|fetch_error|parse_error|ingest_error)' "$LOG" \
  | tr -d ' ' | sort | uniq -c | sort -rn

echo
echo "== rejections by verifier =="
grep -oE '^[[:space:]]+rejected: \[[a-z0-9_]+\]' "$LOG" \
  | grep -oE '\[[a-z0-9_]+\]' | sort | uniq -c | sort -rn

echo
echo "== v4v_payment rejection messages =="
grep -E '^[[:space:]]+rejected: \[v4v_payment\]' "$LOG" \
  | sed -E 's|: https?://.*$||' \
  | sed -E 's|^[[:space:]]+rejected: \[v4v_payment\] ||' \
  | sed -E "s|track '[^']*'|track '<guid>'|" \
  | sort | uniq -c | sort -rn

# One URL per line, for value_coverage.py.
grep -E '^[[:space:]]+rejected: \[v4v_payment\]' "$LOG" \
  | grep -oE 'https?://[^[:space:]]+$' | sort -u > "$OUT/v4v-rejected.txt" || true

echo
echo "== hosts among v4v_payment rejections =="
cut -d/ -f3 "$OUT/v4v-rejected.txt" | sort | uniq -c | sort -rn | head -20

echo
echo "wrote $(wc -l < "$OUT/v4v-rejected.txt") URLs to $OUT/v4v-rejected.txt"
