//! Finds the feed the crawler follows next after a publisher link.
//!
//! ADR 0049 §2 (in the `stophammer` repository) owns this decision. A music
//! feed names its publisher feed. A publisher feed lists its music feeds.
//! [`follow_urls`] gives the URLs on the other side of that link, for one
//! feed. It reads only channel-level `podcast:remoteItem` references; an
//! item-level reference, on a track, never comes back.

use std::collections::HashSet;

use stophammer_parser::types::IngestFeedData;

/// The reason a feed was fetched (ADR 0049 §2, `stophammer` repository).
///
/// The level decides which of a feed's [`follow_urls`] the crawler follows
/// next. A caller states the level as this explicit value. The level is
/// never read from a URL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FollowLevel {
    /// The input URL list of a batch run, or a gossip notification. Every
    /// link of [`follow_urls`] counts, whichever step (1 or 2) it belongs
    /// to.
    Input,
    /// Reached through step 1: a music feed named this feed as its
    /// publisher. Its own `medium="music"` links (step 2) count, but only
    /// when this feed parses as a publisher feed.
    Publisher,
    /// Reached through step 2: a publisher feed listed this feed as an
    /// album. No link of this feed counts. ADR 0049 §2 step 3 stops the
    /// walk here.
    Listed,
}

/// Gives the follow URLs of `feed`, filtered by the level it was fetched at
/// (ADR 0049 §2, `stophammer` repository).
///
/// `fetched_url` is the URL the crawler fetched to get `feed`. [`follow_urls`]
/// needs it to skip a declared `itunes:new-feed-url` that names this same
/// URL (`stophammer` ADR 0052 §2).
///
/// [`FollowLevel::Input`] gives every URL [`follow_urls`] gives.
/// [`FollowLevel::Publisher`] gives those URLs only when `feed`'s
/// `raw_medium` is `publisher`, ASCII case ignored. A feed that does not
/// parse as a publisher feed gives nothing at this level, even when it
/// carries a link of its own. [`FollowLevel::Listed`] always gives nothing.
#[must_use]
pub fn follow_urls_at_level(
    feed: &IngestFeedData,
    level: FollowLevel,
    fetched_url: &str,
) -> Vec<String> {
    match level {
        FollowLevel::Input => follow_urls(feed, fetched_url),
        FollowLevel::Publisher => {
            let is_publisher_feed = feed
                .raw_medium
                .as_deref()
                .is_some_and(|medium| medium.eq_ignore_ascii_case("publisher"));
            if is_publisher_feed {
                follow_urls(feed, fetched_url)
            } else {
                Vec::new()
            }
        }
        FollowLevel::Listed => Vec::new(),
    }
}

/// Gives the follow URLs of `feed`.
///
/// A feed whose `raw_medium` is `music`, ignoring ASCII case, gives the
/// `remote_feed_url` of each channel-level remote item whose `medium` is
/// `publisher`. A feed whose `raw_medium` is `publisher` gives the
/// `remote_feed_url` of each channel-level remote item whose `medium` is
/// `music`. Any other medium, including none, gives none of these links.
///
/// An item's own `medium` comparison also ignores ASCII case, so
/// `medium="Music"` counts. An item with no URL, or a URL whose scheme is
/// not `http` or `https`, is skipped. The URL is never changed, and none is
/// made from a GUID. Duplicates are removed; the first position is kept.
///
/// `feed`'s own `itunes:new-feed-url` (`stophammer` ADR 0052 §2) is added
/// last, when it is present, it is a followable URL, and it is not
/// `fetched_url`, the URL the crawler fetched to get `feed`. This check runs
/// whatever `feed`'s medium is.
#[must_use]
pub fn follow_urls(feed: &IngestFeedData, fetched_url: &str) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut urls = Vec::new();

    let target_medium = match feed.raw_medium.as_deref() {
        Some(medium) if medium.eq_ignore_ascii_case("music") => Some("publisher"),
        Some(medium) if medium.eq_ignore_ascii_case("publisher") => Some("music"),
        _ => None,
    };

    if let Some(target_medium) = target_medium {
        for item in &feed.remote_items {
            let Some(medium) = item.medium.as_deref() else {
                continue;
            };
            if !medium.eq_ignore_ascii_case(target_medium) {
                continue;
            }
            let Some(url) = item.remote_feed_url.as_deref() else {
                continue;
            };
            if !is_followable_url(url) {
                continue;
            }
            if seen.insert(url.to_string()) {
                urls.push(url.to_string());
            }
        }
    }

    if let Some(new_feed_url) = feed.new_feed_url.as_deref()
        && new_feed_url != fetched_url
        && is_followable_url(new_feed_url)
        && seen.insert(new_feed_url.to_string())
    {
        urls.push(new_feed_url.to_string());
    }

    urls
}

/// Returns `true` for a URL whose scheme is `http` or `https`. The URL's
/// target must also be public (`stophammer` ADR 0054 §1).
fn is_followable_url(url: &str) -> bool {
    reqwest::Url::parse(url).is_ok_and(|parsed| crate::fetch_guard::check_target(&parsed).is_ok())
}

#[cfg(test)]
mod tests {
    use stophammer_parser::types::{IngestFeedData, IngestRemoteFeedRef, IngestTrackData};

    use super::{FollowLevel, follow_urls, follow_urls_at_level};

    /// The URL these tests treat as the one the crawler fetched. None of
    /// the feeds these tests build declares this as its `new_feed_url`,
    /// except where a test says so.
    const FETCHED_URL: &str = "https://fetched.example/feed.xml";

    fn remote_item(position: i64, medium: Option<&str>, url: Option<&str>) -> IngestRemoteFeedRef {
        IngestRemoteFeedRef {
            position,
            medium: medium.map(ToString::to_string),
            remote_feed_guid: format!("guid-{position}"),
            remote_feed_url: url.map(ToString::to_string),
            rel: None,
        }
    }

    fn feed(raw_medium: Option<&str>, remote_items: Vec<IngestRemoteFeedRef>) -> IngestFeedData {
        IngestFeedData {
            feed_guid: "feed-guid".to_string(),
            title: "Feed".to_string(),
            description: None,
            image_url: None,
            language: None,
            explicit: false,
            itunes_type: None,
            raw_medium: raw_medium.map(ToString::to_string),
            author_name: None,
            owner_name: None,
            pub_date: None,
            last_build_date: None,
            new_feed_url: None,
            locked: None,
            locked_owner: None,
            remote_items,
            persons: Vec::new(),
            entity_ids: Vec::new(),
            links: Vec::new(),
            podcast_namespace: None,
            feed_payment_routes: Vec::new(),
            live_items: Vec::new(),
            tracks: Vec::new(),
        }
    }

    fn empty_track_with_remote_items(remote_items: Vec<IngestRemoteFeedRef>) -> IngestTrackData {
        IngestTrackData {
            track_guid: "track-guid".to_string(),
            title: "Track".to_string(),
            pub_date: None,
            duration_secs: None,
            image_url: None,
            language: None,
            enclosure_url: None,
            enclosure_type: None,
            enclosure_bytes: None,
            alternate_enclosures: Vec::new(),
            track_number: None,
            season: None,
            explicit: false,
            description: None,
            author_name: None,
            persons: Vec::new(),
            entity_ids: Vec::new(),
            links: Vec::new(),
            payment_routes: Vec::new(),
            value_time_splits: Vec::new(),
            transcripts: Vec::new(),
            remote_items,
        }
    }

    #[test]
    fn a_music_feed_gives_its_publisher_url() {
        let data = feed(
            Some("music"),
            vec![remote_item(
                0,
                Some("publisher"),
                Some("https://publisher.example/feed.xml"),
            )],
        );

        assert_eq!(
            follow_urls(&data, FETCHED_URL),
            vec!["https://publisher.example/feed.xml".to_string()],
            "a music feed must give the URL of its publisher item"
        );
    }

    #[test]
    fn a_publisher_feed_gives_its_music_urls() {
        let data = feed(
            Some("publisher"),
            vec![
                remote_item(0, Some("music"), Some("https://a.example/feed.xml")),
                remote_item(1, Some("music"), Some("https://b.example/feed.xml")),
            ],
        );

        assert_eq!(
            follow_urls(&data, FETCHED_URL),
            vec![
                "https://a.example/feed.xml".to_string(),
                "https://b.example/feed.xml".to_string(),
            ],
            "a publisher feed must give the URL of each music item"
        );
    }

    #[test]
    fn the_feed_medium_comparison_ignores_ascii_case() {
        let data = feed(
            Some("MUSIC"),
            vec![remote_item(
                0,
                Some("publisher"),
                Some("https://publisher.example/feed.xml"),
            )],
        );

        assert_eq!(
            follow_urls(&data, FETCHED_URL),
            vec!["https://publisher.example/feed.xml".to_string()],
            "raw_medium comparison must ignore ASCII case"
        );
    }

    #[test]
    fn an_item_medium_of_music_counts() {
        let data = feed(
            Some("publisher"),
            vec![remote_item(
                0,
                Some("Music"),
                Some("https://a.example/feed.xml"),
            )],
        );

        assert_eq!(
            follow_urls(&data, FETCHED_URL),
            vec!["https://a.example/feed.xml".to_string()],
            "medium=\"Music\" must count, since the comparison ignores ASCII case"
        );
    }

    #[test]
    fn a_medium_l_feed_gives_nothing() {
        let data = feed(
            Some("musicL"),
            vec![remote_item(
                0,
                Some("publisher"),
                Some("https://publisher.example/feed.xml"),
            )],
        );

        assert_eq!(
            follow_urls(&data, FETCHED_URL),
            Vec::<String>::new(),
            "a musicL feed is a different medium and must give nothing"
        );
    }

    #[test]
    fn a_feed_with_no_medium_gives_nothing() {
        let data = feed(
            None,
            vec![remote_item(
                0,
                Some("publisher"),
                Some("https://publisher.example/feed.xml"),
            )],
        );

        assert_eq!(
            follow_urls(&data, FETCHED_URL),
            Vec::<String>::new(),
            "a feed with no medium must give nothing"
        );
    }

    #[test]
    fn an_item_with_no_url_is_skipped() {
        let data = feed(Some("music"), vec![remote_item(0, Some("publisher"), None)]);

        assert_eq!(
            follow_urls(&data, FETCHED_URL),
            Vec::<String>::new(),
            "an item with no URL must be skipped, not turned into a URL from its GUID"
        );
    }

    #[test]
    fn an_ftp_url_is_skipped() {
        let data = feed(
            Some("music"),
            vec![remote_item(
                0,
                Some("publisher"),
                Some("ftp://publisher.example/feed.xml"),
            )],
        );

        assert_eq!(
            follow_urls(&data, FETCHED_URL),
            Vec::<String>::new(),
            "a non-http(s) URL must be skipped"
        );
    }

    #[test]
    fn duplicates_give_one_url() {
        let data = feed(
            Some("publisher"),
            vec![
                remote_item(0, Some("music"), Some("https://a.example/feed.xml")),
                remote_item(1, Some("music"), Some("https://a.example/feed.xml")),
            ],
        );

        assert_eq!(
            follow_urls(&data, FETCHED_URL),
            vec!["https://a.example/feed.xml".to_string()],
            "a duplicate URL must give one entry, keeping the first position"
        );
    }

    #[test]
    fn a_publisher_item_with_no_medium_is_not_followed() {
        let data = feed(
            Some("publisher"),
            vec![remote_item(0, None, Some("https://a.example/feed.xml"))],
        );

        assert_eq!(
            follow_urls(&data, FETCHED_URL),
            Vec::<String>::new(),
            "a remote item with no medium is not a listed album and must not be followed"
        );
    }

    #[test]
    fn an_item_level_remote_item_on_a_track_is_not_followed() {
        let mut data = feed(Some("music"), Vec::new());
        data.tracks
            .push(empty_track_with_remote_items(vec![remote_item(
                0,
                Some("publisher"),
                Some("https://publisher.example/feed.xml"),
            )]));

        assert_eq!(
            follow_urls(&data, FETCHED_URL),
            Vec::<String>::new(),
            "an item-level remote item, on a track, must not be followed"
        );
    }

    #[test]
    fn a_declared_new_feed_url_is_followed() {
        let mut data = feed(None, Vec::new());
        data.new_feed_url = Some("https://moved.example/feed.xml".to_string());

        assert_eq!(
            follow_urls(&data, FETCHED_URL),
            vec!["https://moved.example/feed.xml".to_string()],
            "a declared new_feed_url must be followed (stophammer ADR 0052 §2)"
        );
    }

    #[test]
    fn a_new_feed_url_equal_to_the_fetched_url_is_not_followed() {
        let mut data = feed(None, Vec::new());
        data.new_feed_url = Some(FETCHED_URL.to_string());

        assert_eq!(
            follow_urls(&data, FETCHED_URL),
            Vec::<String>::new(),
            "a new_feed_url that names the fetched URL must not be followed (stophammer ADR 0052 §2)"
        );
    }

    #[test]
    fn an_input_level_feed_gives_every_follow_url() {
        let data = feed(
            Some("music"),
            vec![remote_item(
                0,
                Some("publisher"),
                Some("https://publisher.example/feed.xml"),
            )],
        );

        assert_eq!(
            follow_urls_at_level(&data, FollowLevel::Input, FETCHED_URL),
            vec!["https://publisher.example/feed.xml".to_string()],
            "FollowLevel::Input must give every follow_urls link"
        );
    }

    #[test]
    fn a_publisher_level_publisher_feed_gives_its_music_urls() {
        let data = feed(
            Some("publisher"),
            vec![remote_item(
                0,
                Some("music"),
                Some("https://a.example/feed.xml"),
            )],
        );

        assert_eq!(
            follow_urls_at_level(&data, FollowLevel::Publisher, FETCHED_URL),
            vec!["https://a.example/feed.xml".to_string()],
            "FollowLevel::Publisher must give the music links of a feed that parses as publisher"
        );
    }

    #[test]
    fn a_publisher_level_music_feed_gives_nothing() {
        let data = feed(
            Some("music"),
            vec![remote_item(
                0,
                Some("publisher"),
                Some("https://publisher.example/feed.xml"),
            )],
        );

        assert_eq!(
            follow_urls_at_level(&data, FollowLevel::Publisher, FETCHED_URL),
            Vec::<String>::new(),
            "FollowLevel::Publisher must give nothing when the fetched feed is not a publisher feed"
        );
    }

    #[test]
    fn a_listed_level_feed_gives_nothing() {
        let data = feed(
            Some("publisher"),
            vec![remote_item(
                0,
                Some("music"),
                Some("https://a.example/feed.xml"),
            )],
        );

        assert_eq!(
            follow_urls_at_level(&data, FollowLevel::Listed, FETCHED_URL),
            Vec::<String>::new(),
            "FollowLevel::Listed must always give nothing, so the walk stops at one level (ADR 0049 §2 step 3)"
        );
    }
}
