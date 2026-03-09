// POST an IngestFeedRequest to the stophammer core and log the outcome.

import type { IngestFeedRequest, IngestResponse } from "./types.ts";

export interface IngestOutcome {
  url: string;
  accepted: boolean;
  no_change: boolean;
  reason?: string;
  warnings: string[];
  events_emitted: string[];
  error?: string;
}

/**
 * POST `payload` to `ingestUrl` and return the parsed IngestOutcome.
 * HTTP transport errors are caught and returned as `error` rather than thrown.
 */
export async function postToCore(
  ingestUrl: string,
  payload: IngestFeedRequest
): Promise<IngestOutcome> {
  let response: Response;
  try {
    response = await fetch(ingestUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
  } catch (err) {
    return {
      url: payload.canonical_url,
      accepted: false,
      no_change: false,
      warnings: [],
      events_emitted: [],
      error: String(err),
    };
  }

  let body: IngestResponse;
  try {
    body = (await response.json()) as IngestResponse;
  } catch {
    return {
      url: payload.canonical_url,
      accepted: false,
      no_change: false,
      warnings: [],
      events_emitted: [],
      error: `core returned non-JSON (HTTP ${response.status})`,
    };
  }

  return {
    url: payload.canonical_url,
    accepted: body.accepted,
    no_change: body.no_change,
    reason: body.reason,
    warnings: body.warnings ?? [],
    events_emitted: body.events_emitted ?? [],
  };
}

/** Print a human-readable summary line for one crawl outcome. */
export function logOutcome(outcome: IngestOutcome): void {
  const tag = `[${outcome.url}]`;

  if (outcome.error) {
    console.error(`${tag} ERROR: ${outcome.error}`);
    return;
  }

  if (outcome.no_change) {
    console.log(`${tag} no_change`);
    return;
  }

  if (outcome.accepted) {
    const evts =
      outcome.events_emitted.length > 0
        ? ` events=[${outcome.events_emitted.join(", ")}]`
        : "";
    console.log(`${tag} accepted${evts}`);
  } else {
    console.warn(`${tag} rejected: ${outcome.reason ?? "(no reason)"}`);
  }

  for (const w of outcome.warnings) {
    console.warn(`${tag} WARNING: ${w}`);
  }
}
