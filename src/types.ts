// TypeScript types mirroring Rust IngestFeedRequest / IngestFeedData / etc.
// Kept in sync with stophammer/src/ingest.rs.

export type RouteType = "node" | "lnaddress";

export interface IngestPaymentRoute {
  recipient_name?: string;
  route_type: RouteType;
  address: string;
  custom_key?: string;
  custom_value?: string;
  split: number;
  fee: boolean;
}

export interface IngestValueTimeSplit {
  start_time_secs: number;
  duration_secs?: number;
  remote_feed_guid: string;
  remote_item_guid: string;
  split: number;
}

export interface IngestTrackData {
  track_guid: string;
  title: string;
  pub_date?: number;
  duration_secs?: number;
  enclosure_url?: string;
  enclosure_type?: string;
  enclosure_bytes?: number;
  track_number?: number;
  season?: number;
  explicit: boolean;
  description?: string;
  author_name?: string;
  payment_routes: IngestPaymentRoute[];
  value_time_splits: IngestValueTimeSplit[];
}

export interface IngestFeedData {
  feed_guid: string;
  title: string;
  description?: string;
  image_url?: string;
  language?: string;
  explicit: boolean;
  itunes_type?: string;
  raw_medium?: string;
  author_name?: string;
  owner_name?: string;
  pub_date?: number;
  tracks: IngestTrackData[];
}

export interface IngestFeedRequest {
  canonical_url: string;
  source_url: string;
  crawl_token: string;
  http_status: number;
  content_hash: string;
  feed_data?: IngestFeedData;
}

export interface IngestResponse {
  accepted: boolean;
  reason?: string;
  events_emitted: string[];
  no_change: boolean;
  warnings: string[];
}
