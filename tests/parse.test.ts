import { test, expect, describe } from "bun:test";
import { parseFeed, parseDuration, parsePubDate } from "../src/parse.ts";

// ── parseFeed ───────────────────────────────────────────────────────────────

describe("parseFeed", () => {
  test("parses a basic music feed", () => {
    const xml = `<?xml version="1.0"?>
<rss version="2.0" xmlns:podcast="https://podcastindex.org/namespace/1.0" xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd">
  <channel>
    <title>Test Music Show</title>
    <podcast:guid>aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee</podcast:guid>
    <description>A test music podcast</description>
    <itunes:image href="https://example.com/cover.jpg"/>
    <language>en</language>
    <itunes:explicit>yes</itunes:explicit>
    <itunes:type>serial</itunes:type>
    <podcast:medium>music</podcast:medium>
    <itunes:author>Test Author</itunes:author>
    <itunes:owner><itunes:name>Owner Name</itunes:name></itunes:owner>
    <pubDate>Thu, 01 Jan 2026 00:00:00 GMT</pubDate>
    <item>
      <guid>track-guid-1</guid>
      <title>Track One</title>
      <enclosure url="https://example.com/track1.mp3" type="audio/mpeg" length="1234567"/>
      <itunes:duration>3:45</itunes:duration>
      <pubDate>Wed, 31 Dec 2025 00:00:00 GMT</pubDate>
    </item>
  </channel>
</rss>`;
    const result = parseFeed(xml);
    expect(result).not.toBeNull();
    expect(result!.feed_guid).toBe("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
    expect(result!.title).toBe("Test Music Show");
    expect(result!.description).toBe("A test music podcast");
    expect(result!.image_url).toBe("https://example.com/cover.jpg");
    expect(result!.language).toBe("en");
    expect(result!.explicit).toBe(true);
    expect(result!.itunes_type).toBe("serial");
    expect(result!.raw_medium).toBe("music");
    expect(result!.author_name).toBe("Test Author");
    expect(result!.owner_name).toBe("Owner Name");
    expect(result!.tracks).toHaveLength(1);
    expect(result!.tracks[0].track_guid).toBe("track-guid-1");
    expect(result!.tracks[0].title).toBe("Track One");
    expect(result!.tracks[0].enclosure_url).toBe("https://example.com/track1.mp3");
    expect(result!.tracks[0].enclosure_type).toBe("audio/mpeg");
    expect(result!.tracks[0].duration_secs).toBe(225);
  });

  test("extracts payment routes from track-level podcast:value", () => {
    const xml = `<?xml version="1.0"?>
<rss version="2.0" xmlns:podcast="https://podcastindex.org/namespace/1.0" xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd">
  <channel>
    <title>Pay Test</title>
    <podcast:guid>pay-feed-guid</podcast:guid>
    <item>
      <guid>pay-track-1</guid>
      <title>Track with payments</title>
      <podcast:value type="lightning" method="keysend">
        <podcast:valueRecipient name="Alice" type="node" address="abc123" split="90" customKey="7629169" customValue="podcast123" fee="false"/>
        <podcast:valueRecipient name="Bob" type="lnaddress" address="bob@getalby.com" split="10" fee="true"/>
        <podcast:valueRecipient name="Default Type" type="lightning" address="def456" split="5"/>
      </podcast:value>
    </item>
  </channel>
</rss>`;
    const result = parseFeed(xml);
    expect(result).not.toBeNull();
    const routes = result!.tracks[0].payment_routes;
    expect(routes).toHaveLength(3);

    // First recipient: explicit node type
    expect(routes[0].recipient_name).toBe("Alice");
    expect(routes[0].route_type).toBe("node");
    expect(routes[0].address).toBe("abc123");
    expect(routes[0].split).toBe(90);
    expect(routes[0].custom_key).toBe("7629169");
    expect(routes[0].custom_value).toBe("podcast123");
    expect(routes[0].fee).toBe(false);

    // Second recipient: lnaddress type preserved
    expect(routes[1].recipient_name).toBe("Bob");
    expect(routes[1].route_type).toBe("lnaddress");
    expect(routes[1].address).toBe("bob@getalby.com");
    expect(routes[1].split).toBe(10);
    expect(routes[1].fee).toBe(true);

    // Third recipient: "lightning" type should become "node"
    expect(routes[2].recipient_name).toBe("Default Type");
    expect(routes[2].route_type).toBe("node");
    expect(routes[2].address).toBe("def456");
    expect(routes[2].split).toBe(5);
    expect(routes[2].fee).toBe(false);
  });

  test("extracts feed-level payment routes from podcast:value", () => {
    const xml = `<?xml version="1.0"?>
<rss version="2.0" xmlns:podcast="https://podcastindex.org/namespace/1.0" xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd">
  <channel>
    <title>Feed-Level Value</title>
    <podcast:guid>feed-val-guid</podcast:guid>
    <podcast:value type="lightning" method="keysend">
      <podcast:valueRecipient name="Producer" type="node" address="prod-addr" split="80" fee="false"/>
      <podcast:valueRecipient name="App" type="node" address="app-addr" split="20" fee="true"/>
    </podcast:value>
    <item>
      <guid>track-no-value</guid>
      <title>No track-level value</title>
    </item>
  </channel>
</rss>`;
    const result = parseFeed(xml);
    expect(result).not.toBeNull();

    // Feed-level routes extracted
    expect(result!.feed_payment_routes).toHaveLength(2);
    expect(result!.feed_payment_routes[0].recipient_name).toBe("Producer");
    expect(result!.feed_payment_routes[0].address).toBe("prod-addr");
    expect(result!.feed_payment_routes[0].split).toBe(80);
    expect(result!.feed_payment_routes[0].fee).toBe(false);
    expect(result!.feed_payment_routes[1].recipient_name).toBe("App");
    expect(result!.feed_payment_routes[1].address).toBe("app-addr");
    expect(result!.feed_payment_routes[1].split).toBe(20);
    expect(result!.feed_payment_routes[1].fee).toBe(true);

    // Track has no payment routes of its own
    expect(result!.tracks[0].payment_routes).toHaveLength(0);
  });

  test("parses value time splits with remoteItem child element", () => {
    const xml = `<?xml version="1.0"?>
<rss version="2.0" xmlns:podcast="https://podcastindex.org/namespace/1.0" xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd">
  <channel>
    <title>VTS Test</title>
    <podcast:guid>vts-feed-guid</podcast:guid>
    <item>
      <guid>vts-track-1</guid>
      <title>Track with VTS</title>
      <podcast:value type="lightning" method="keysend">
        <podcast:valueRecipient name="Main" type="node" address="main-addr" split="100" fee="false"/>
        <podcast:valueTimeSplit startTime="60" duration="120" split="50">
          <podcast:remoteItem feedGuid="remote-feed" itemGuid="remote-item"/>
        </podcast:valueTimeSplit>
      </podcast:value>
    </item>
  </channel>
</rss>`;
    const result = parseFeed(xml);
    expect(result).not.toBeNull();
    const vts = result!.tracks[0].value_time_splits;
    expect(vts).toHaveLength(1);
    expect(vts[0].start_time_secs).toBe(60);
    expect(vts[0].duration_secs).toBe(120);
    expect(vts[0].split).toBe(50);
    // Remote GUIDs come from the child remoteItem element, not VTS attributes
    expect(vts[0].remote_feed_guid).toBe("remote-feed");
    expect(vts[0].remote_item_guid).toBe("remote-item");
  });

  test("skips value time splits with remotePercentage", () => {
    const xml = `<?xml version="1.0"?>
<rss version="2.0" xmlns:podcast="https://podcastindex.org/namespace/1.0" xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd">
  <channel>
    <title>VTS Percentage Test</title>
    <podcast:guid>vts-pct-guid</podcast:guid>
    <item>
      <guid>vts-pct-track-1</guid>
      <title>Track with remotePercentage VTS</title>
      <podcast:value type="lightning" method="keysend">
        <podcast:valueRecipient name="Main" type="node" address="main-addr" split="100" fee="false"/>
        <podcast:valueTimeSplit startTime="0" remotePercentage="90">
          <podcast:remoteItem feedGuid="x" itemGuid="y"/>
        </podcast:valueTimeSplit>
      </podcast:value>
    </item>
  </channel>
</rss>`;
    const result = parseFeed(xml);
    expect(result).not.toBeNull();
    // VTS with remotePercentage should be excluded
    expect(result!.tracks[0].value_time_splits).toHaveLength(0);
  });

  test("extracts owner_name from itunes:owner", () => {
    const xml = `<?xml version="1.0"?>
<rss version="2.0" xmlns:podcast="https://podcastindex.org/namespace/1.0" xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd">
  <channel>
    <title>Owner Test</title>
    <podcast:guid>owner-guid</podcast:guid>
    <itunes:author>Author Here</itunes:author>
    <itunes:owner><itunes:name>Owner Here</itunes:name></itunes:owner>
    <item>
      <guid>t1</guid>
      <title>T</title>
    </item>
  </channel>
</rss>`;
    const result = parseFeed(xml);
    expect(result).not.toBeNull();
    expect(result!.owner_name).toBe("Owner Here");
    expect(result!.author_name).toBe("Author Here");
  });

  test("author_name falls back to owner_name when itunes:author is absent", () => {
    const xml = `<?xml version="1.0"?>
<rss version="2.0" xmlns:podcast="https://podcastindex.org/namespace/1.0" xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd">
  <channel>
    <title>Fallback Test</title>
    <podcast:guid>fallback-guid</podcast:guid>
    <itunes:owner><itunes:name>Fallback Owner</itunes:name></itunes:owner>
    <item>
      <guid>t1</guid>
      <title>T</title>
    </item>
  </channel>
</rss>`;
    const result = parseFeed(xml);
    expect(result).not.toBeNull();
    // author_name should fall back to owner_name
    expect(result!.author_name).toBe("Fallback Owner");
    expect(result!.owner_name).toBe("Fallback Owner");
  });

  test("explicit accepts 'yes' and 'true' at feed level", () => {
    const makeXml = (value: string) => `<?xml version="1.0"?>
<rss version="2.0" xmlns:podcast="https://podcastindex.org/namespace/1.0" xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd">
  <channel>
    <title>Explicit Test</title>
    <podcast:guid>explicit-guid</podcast:guid>
    <itunes:explicit>${value}</itunes:explicit>
    <item><guid>t1</guid><title>T</title></item>
  </channel>
</rss>`;
    expect(parseFeed(makeXml("yes"))!.explicit).toBe(true);
    expect(parseFeed(makeXml("true"))!.explicit).toBe(true);
    expect(parseFeed(makeXml("no"))!.explicit).toBe(false);
    expect(parseFeed(makeXml("false"))!.explicit).toBe(false);
  });

  test("explicit accepts 'yes' and 'true' at track level", () => {
    const makeXml = (value: string) => `<?xml version="1.0"?>
<rss version="2.0" xmlns:podcast="https://podcastindex.org/namespace/1.0" xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd">
  <channel>
    <title>Track Explicit Test</title>
    <podcast:guid>texplicit-guid</podcast:guid>
    <item>
      <guid>t1</guid>
      <title>T</title>
      <itunes:explicit>${value}</itunes:explicit>
    </item>
  </channel>
</rss>`;
    expect(parseFeed(makeXml("yes"))!.tracks[0].explicit).toBe(true);
    expect(parseFeed(makeXml("true"))!.tracks[0].explicit).toBe(true);
    expect(parseFeed(makeXml("no"))!.tracks[0].explicit).toBe(false);
    expect(parseFeed(makeXml("false"))!.tracks[0].explicit).toBe(false);
  });

  test("missing podcast:guid returns empty string for feed_guid", () => {
    const xml = `<?xml version="1.0"?>
<rss version="2.0" xmlns:podcast="https://podcastindex.org/namespace/1.0" xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd">
  <channel>
    <title>No GUID</title>
    <item><guid>t1</guid><title>T</title></item>
  </channel>
</rss>`;
    const result = parseFeed(xml);
    // Feed without podcast:guid returns a result with empty feed_guid
    expect(result).not.toBeNull();
    expect(result!.feed_guid).toBe("");
  });

  test("no payment route ever has route_type 'lightning'", () => {
    const xml = `<?xml version="1.0"?>
<rss version="2.0" xmlns:podcast="https://podcastindex.org/namespace/1.0" xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd">
  <channel>
    <title>Lightning Regression</title>
    <podcast:guid>lightning-guid</podcast:guid>
    <podcast:value type="lightning" method="keysend">
      <podcast:valueRecipient name="Feed R" type="lightning" address="feedaddr" split="100" fee="false"/>
    </podcast:value>
    <item>
      <guid>lt1</guid>
      <title>LT</title>
      <podcast:value type="lightning" method="keysend">
        <podcast:valueRecipient name="Track R" type="lightning" address="trackaddr" split="100" fee="false"/>
      </podcast:value>
    </item>
  </channel>
</rss>`;
    const result = parseFeed(xml);
    expect(result).not.toBeNull();

    // Feed-level routes: none should be "lightning"
    for (const route of result!.feed_payment_routes) {
      expect(route.route_type).not.toBe("lightning");
    }
    // Track-level routes: none should be "lightning"
    for (const track of result!.tracks) {
      for (const route of track.payment_routes) {
        expect(route.route_type).not.toBe("lightning");
      }
    }
  });
});

// ── parseDuration ───────────────────────────────────────────────────────────

describe("parseDuration", () => {
  test("parses HH:MM:SS format", () => {
    expect(parseDuration("1:02:03")).toBe(3723);
  });

  test("parses MM:SS format", () => {
    expect(parseDuration("3:45")).toBe(225);
  });

  test("parses plain seconds string", () => {
    expect(parseDuration("180")).toBe(180);
  });

  test("returns undefined for invalid input", () => {
    expect(parseDuration("abc")).toBeUndefined();
    expect(parseDuration("")).toBeUndefined();
    expect(parseDuration(undefined)).toBeUndefined();
  });
});

// ── parsePubDate ────────────────────────────────────────────────────────────

describe("parsePubDate", () => {
  test("parses RFC 2822 date to epoch seconds", () => {
    const result = parsePubDate("Thu, 01 Jan 2026 00:00:00 GMT");
    expect(result).toBe(1767225600);
  });

  test("returns undefined for invalid date", () => {
    expect(parsePubDate("not a date")).toBeUndefined();
  });

  test("returns undefined for empty/undefined input", () => {
    expect(parsePubDate("")).toBeUndefined();
    expect(parsePubDate(undefined)).toBeUndefined();
  });
});
