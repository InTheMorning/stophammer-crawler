// Fetch a feed URL, compute its content hash, parse it, and return an IngestFeedRequest.

import { parseFeed } from "./parse.ts";
import type { IngestFeedRequest } from "./types.ts";

/**
 * Fetch `url`, hash the raw bytes, parse the XML, and return a fully populated
 * IngestFeedRequest ready to POST to /ingest/feed.
 *
 * Network errors are re-thrown so the caller can log and skip.
 */
export async function crawlFeed(
  url: string,
  crawlToken: string
): Promise<IngestFeedRequest> {
  const response = await fetch(url, {
    headers: {
      "User-Agent": "stophammer-crawler/1.0 (+https://github.com/hey-v4v/stophammer)",
      Accept: "application/rss+xml, application/xml, text/xml, */*",
    },
  });

  const httpStatus = response.status;
  const bodyBytes = new Uint8Array(await response.arrayBuffer());

  // SHA-256 of raw bytes, hex-encoded
  const hashBuffer = await crypto.subtle.digest("SHA-256", bodyBytes);
  const contentHash = Array.from(new Uint8Array(hashBuffer))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");

  let feedData = undefined;
  if (response.ok) {
    const xmlText = new TextDecoder("utf-8").decode(bodyBytes);
    const parsed = parseFeed(xmlText);
    // parsed may be null if XML is malformed — feed_data stays undefined
    feedData = parsed ?? undefined;
  }

  return {
    canonical_url: url,
    source_url: url,
    crawl_token: crawlToken,
    http_status: httpStatus,
    content_hash: contentHash,
    feed_data: feedData,
  };
}
