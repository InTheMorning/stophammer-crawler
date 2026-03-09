// XML → structured IngestFeedData
// Uses fast-xml-parser with namespace support for podcast: and itunes: prefixes.

import { XMLParser } from "fast-xml-parser";
import type {
  IngestFeedData,
  IngestTrackData,
  IngestPaymentRoute,
  IngestValueTimeSplit,
  RouteType,
} from "./types.ts";

// ── XML parser config ───────────────────────────────────────────────────────

const parser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: "@_",
  // Preserve namespace prefixes so podcast: and itunes: tags are accessible
  removeNSPrefix: false,
  // Coerce numbers/booleans would mangle GUIDs and split values — leave off
  parseAttributeValue: false,
  parseTagValue: false,
  // Always return arrays for item/valueRecipient/valueTimeSplit to simplify access
  isArray: (tagName) =>
    ["item", "podcast:valueRecipient", "podcast:valueTimeSplit"].includes(tagName),
});

// ── Duration parsing ────────────────────────────────────────────────────────

/**
 * Parse iTunes duration strings to integer seconds.
 * Handles "HH:MM:SS", "MM:SS", and plain second strings.
 */
export function parseDuration(raw: string | undefined): number | undefined {
  if (!raw) return undefined;
  const s = raw.trim();
  if (!s) return undefined;

  const parts = s.split(":").map((p) => parseInt(p, 10));
  if (parts.some(isNaN)) return undefined;

  if (parts.length === 3) {
    // HH:MM:SS
    const [h, m, sec] = parts as [number, number, number];
    return h * 3600 + m * 60 + sec;
  }
  if (parts.length === 2) {
    // MM:SS
    const [m, sec] = parts as [number, number];
    return m * 60 + sec;
  }
  if (parts.length === 1) {
    return parts[0] as number;
  }
  return undefined;
}

// ── pubDate parsing ─────────────────────────────────────────────────────────

export function parsePubDate(raw: string | undefined): number | undefined {
  if (!raw) return undefined;
  const ms = new Date(raw.trim()).getTime();
  if (isNaN(ms)) return undefined;
  return Math.floor(ms / 1000);
}

// ── HTML strip ──────────────────────────────────────────────────────────────

function stripHtml(s: string | undefined): string | undefined {
  if (!s) return undefined;
  const stripped = String(s).replace(/<[^>]+>/g, "").trim();
  return stripped || undefined;
}

// ── Attribute helpers ───────────────────────────────────────────────────────

function attr(node: Record<string, string> | undefined, name: string): string | undefined {
  if (!node) return undefined;
  return node[`@_${name}`] || undefined;
}

function text(node: unknown): string | undefined {
  if (node === undefined || node === null) return undefined;
  if (typeof node === "string") return node || undefined;
  if (typeof node === "number") return String(node);
  // fast-xml-parser may wrap text+attrs in {#text, @_...}
  if (typeof node === "object" && "#text" in (node as Record<string, unknown>)) {
    const t = (node as Record<string, unknown>)["#text"];
    return t !== undefined && t !== null ? String(t) : undefined;
  }
  return undefined;
}

function optInt(raw: string | undefined): number | undefined {
  if (!raw) return undefined;
  const n = parseInt(raw.trim(), 10);
  return isNaN(n) ? undefined : n;
}

// ── Payment route parsing ───────────────────────────────────────────────────

function parsePaymentRoutes(valueNode: unknown): IngestPaymentRoute[] {
  if (!valueNode || typeof valueNode !== "object") return [];
  const vn = valueNode as Record<string, unknown>;

  const recipients = vn["podcast:valueRecipient"];
  if (!Array.isArray(recipients)) return [];

  const routes: IngestPaymentRoute[] = [];
  for (const r of recipients) {
    if (typeof r !== "object" || r === null) continue;
    const rr = r as Record<string, string>;
    const address = attr(rr, "address");
    if (!address) continue;

    const rawType = attr(rr, "type") ?? "";
    const routeType: RouteType =
      rawType === "lnaddress" ? "lnaddress" : "node";

    const splitRaw = attr(rr, "split");
    const split = optInt(splitRaw) ?? 0;

    routes.push({
      recipient_name: attr(rr, "name") ?? undefined,
      route_type: routeType,
      address,
      custom_key: attr(rr, "customKey") ?? undefined,
      custom_value: attr(rr, "customValue") ?? undefined,
      split,
      fee: attr(rr, "fee") === "true",
    });
  }
  return routes;
}

// ── Value time split parsing ────────────────────────────────────────────────

function parseValueTimeSplits(valueNode: unknown): IngestValueTimeSplit[] {
  if (!valueNode || typeof valueNode !== "object") return [];
  const vn = valueNode as Record<string, unknown>;

  const splits = vn["podcast:valueTimeSplit"];
  if (!Array.isArray(splits)) return [];

  const result: IngestValueTimeSplit[] = [];
  for (const s of splits) {
    if (typeof s !== "object" || s === null) continue;
    const sv = s as Record<string, unknown>;

    // Skip valueTimeSplits that use remotePercentage (not split-based)
    const svAttrs = sv as Record<string, string>;
    if (attr(svAttrs, "remotePercentage") !== undefined) continue;

    const startTimeRaw = attr(svAttrs, "startTime");
    const startTime = optInt(startTimeRaw);
    if (startTime === undefined) continue;

    // split comes from the valueTimeSplit element itself
    const splitRaw = attr(svAttrs, "split");
    const split = optInt(splitRaw) ?? 0;

    const durationRaw = attr(svAttrs, "duration");
    const durationSecs = optInt(durationRaw);

    // remoteItem for feed/item guid
    const remoteItem = sv["podcast:remoteItem"];
    let remoteFeedGuid = "";
    let remoteItemGuid = "";
    if (remoteItem && typeof remoteItem === "object") {
      const ri = remoteItem as Record<string, string>;
      remoteFeedGuid = attr(ri, "feedGuid") ?? "";
      remoteItemGuid = attr(ri, "itemGuid") ?? "";
    }

    result.push({
      start_time_secs: startTime,
      duration_secs: durationSecs,
      remote_feed_guid: remoteFeedGuid,
      remote_item_guid: remoteItemGuid,
      split,
    });
  }
  return result;
}

// ── Item (track) parsing ────────────────────────────────────────────────────

function parseItem(item: unknown): IngestTrackData | null {
  if (!item || typeof item !== "object") return null;
  const it = item as Record<string, unknown>;

  const trackGuid = text(it["guid"]);
  if (!trackGuid) return null;

  const title = text(it["title"]) ?? "";

  const explicit =
    text(it["itunes:explicit"])?.toLowerCase() === "yes" ||
    text(it["itunes:explicit"])?.toLowerCase() === "true";

  const enclosureNode = it["enclosure"];
  let enclosureUrl: string | undefined;
  let enclosureType: string | undefined;
  let enclosureBytes: number | undefined;
  if (enclosureNode && typeof enclosureNode === "object") {
    const en = enclosureNode as Record<string, string>;
    enclosureUrl = attr(en, "url");
    enclosureType = attr(en, "type");
    const lenRaw = attr(en, "length");
    enclosureBytes = optInt(lenRaw);
  }

  const paymentRoutes = parsePaymentRoutes(it["podcast:value"]);
  const valueTimeSplits = parseValueTimeSplits(it["podcast:value"]);

  return {
    track_guid: trackGuid,
    title,
    pub_date: parsePubDate(text(it["pubDate"])),
    duration_secs: parseDuration(text(it["itunes:duration"])),
    enclosure_url: enclosureUrl,
    enclosure_type: enclosureType,
    enclosure_bytes: enclosureBytes,
    track_number: optInt(text(it["itunes:episode"])),
    season: optInt(text(it["itunes:season"])),
    explicit,
    description: stripHtml(text(it["description"])),
    author_name: text(it["itunes:author"]) ?? undefined,
    payment_routes: paymentRoutes,
    value_time_splits: valueTimeSplits,
  };
}

// ── Feed-level parsing ──────────────────────────────────────────────────────

/**
 * Parse raw RSS/Podcast XML bytes into an IngestFeedData object.
 * Returns null if the XML is unparseable.
 */
export function parseFeed(xmlText: string): IngestFeedData | null {
  let root: Record<string, unknown>;
  try {
    root = parser.parse(xmlText) as Record<string, unknown>;
  } catch {
    return null;
  }

  const rss = root["rss"] as Record<string, unknown> | undefined;
  if (!rss) return null;
  const channel = rss["channel"] as Record<string, unknown> | undefined;
  if (!channel) return null;

  // feed_guid: empty string if absent (server-side FeedGuidVerifier will reject)
  const feedGuid = text(channel["podcast:guid"]) ?? "";

  const title = text(channel["title"]) ?? "";

  // image_url: prefer itunes:image@href, fall back to image/url
  let imageUrl: string | undefined;
  const itunesImageNode = channel["itunes:image"];
  if (itunesImageNode && typeof itunesImageNode === "object") {
    imageUrl = attr(itunesImageNode as Record<string, string>, "href");
  }
  if (!imageUrl) {
    const imageNode = channel["image"] as Record<string, unknown> | undefined;
    imageUrl = text(imageNode?.["url"]);
  }

  const explicit =
    text(channel["itunes:explicit"])?.toLowerCase() === "yes" ||
    text(channel["itunes:explicit"])?.toLowerCase() === "true";

  // author_name / owner_name
  let authorName = text(channel["itunes:author"]) ?? undefined;
  let ownerName: string | undefined;
  const ownerNode = channel["itunes:owner"] as Record<string, unknown> | undefined;
  if (ownerNode) {
    ownerName = text(ownerNode["itunes:name"]) ?? undefined;
    if (!authorName) {
      authorName = ownerName;
    }
  }

  // tracks
  const items = channel["item"];
  const itemArray = Array.isArray(items) ? items : items !== undefined ? [items] : [];
  const tracks: IngestTrackData[] = [];
  for (const item of itemArray) {
    const track = parseItem(item);
    if (track) tracks.push(track);
  }

  return {
    feed_guid: feedGuid,
    title,
    description: stripHtml(text(channel["description"])),
    image_url: imageUrl,
    language: text(channel["language"]) ?? undefined,
    explicit,
    itunes_type: text(channel["itunes:type"]) ?? undefined,
    raw_medium: text(channel["podcast:medium"]) ?? undefined,
    author_name: authorName,
    owner_name: ownerName,
    pub_date: parsePubDate(text(channel["pubDate"])),
    tracks,
  };
}
