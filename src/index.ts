#!/usr/bin/env bun
// CLI entrypoint: reads feed URLs, runs a concurrency-limited crawl pool.
//
// Feed URL sources (first match wins):
//   1. File path argument:  bun run src/index.ts feeds.txt
//   2. Env var FEED_URLS:   newline-separated list
//   3. stdin (piped):       one URL per line
//
// Required env vars:
//   CRAWL_TOKEN   — shared secret checked by stophammer core
//
// Optional env vars:
//   INGEST_URL    — default http://localhost:8008/ingest/feed
//   CONCURRENCY   — default 5

import { crawlFeed } from "./crawl.ts";
import { postToCore, logOutcome } from "./ingest.ts";

// ── Config ──────────────────────────────────────────────────────────────────

const INGEST_URL =
  process.env["INGEST_URL"] ?? "http://localhost:8008/ingest/feed";
const CRAWL_TOKEN = process.env["CRAWL_TOKEN"] ?? "";
const CONCURRENCY = parseInt(process.env["CONCURRENCY"] ?? "5", 10);

if (!CRAWL_TOKEN) {
  console.error("ERROR: CRAWL_TOKEN env var is required");
  process.exit(1);
}

// ── URL loading ─────────────────────────────────────────────────────────────

async function loadUrls(): Promise<string[]> {
  let raw: string;

  // 1. File argument
  const fileArg = process.argv[2];
  if (fileArg) {
    raw = await Bun.file(fileArg).text();
  } else if (process.env["FEED_URLS"]) {
    // 2. Env var
    raw = process.env["FEED_URLS"];
  } else {
    // 3. stdin
    const chunks: Buffer[] = [];
    for await (const chunk of process.stdin) {
      chunks.push(chunk as Buffer);
    }
    raw = Buffer.concat(chunks).toString("utf-8");
  }

  return raw
    .split("\n")
    .map((l) => l.trim())
    .filter((l) => l.length > 0 && !l.startsWith("#"));
}

// ── Concurrency pool ────────────────────────────────────────────────────────

/**
 * Run `tasks` with at most `limit` in-flight at a time.
 * Each task is a thunk () => Promise<T>.
 */
async function pool<T>(
  tasks: Array<() => Promise<T>>,
  limit: number
): Promise<T[]> {
  const results: T[] = new Array(tasks.length);
  let next = 0;

  async function worker(): Promise<void> {
    while (next < tasks.length) {
      const i = next++;
      const task = tasks[i];
      if (task) results[i] = await task();
    }
  }

  const concurrency = Math.min(limit, tasks.length);
  const workers: Promise<void>[] = [];
  for (let w = 0; w < concurrency; w++) workers.push(worker());
  await Promise.all(workers);
  return results;
}

// ── Main ────────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  const urls = await loadUrls();

  if (urls.length === 0) {
    console.error("No feed URLs provided. Pass a file, set FEED_URLS, or pipe via stdin.");
    process.exit(1);
  }

  console.log(
    `Crawling ${urls.length} feed(s) with concurrency=${CONCURRENCY} → ${INGEST_URL}`
  );

  const tasks = urls.map((url) => async () => {
    let request;
    try {
      request = await crawlFeed(url, CRAWL_TOKEN);
    } catch (err) {
      console.error(`[${url}] FETCH ERROR: ${err}`);
      return;
    }
    const outcome = await postToCore(INGEST_URL, request);
    logOutcome(outcome);
  });

  await pool(tasks, CONCURRENCY);
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
