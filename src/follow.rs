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

/// The most follow URLs [`follow_urls`] gives back for one source feed
/// (`stophammer` ADR 0054 §3).
pub const MAX_FOLLOW_URLS_PER_FEED: usize = 200;

/// The most follow URLs [`follow_urls`] gives back for a `musicL` feed
/// (`stophammer` ADR 0060 §6).
pub const MAX_FOLLOW_URLS_PER_LIST_FEED: usize = 1_000;

/// Gives the follow URLs of `feed`.
///
/// A feed whose `raw_medium` is `music`, ignoring ASCII case, gives the
/// `remote_feed_url` of each channel-level remote item whose `medium` is
/// `publisher`. A feed whose `raw_medium` is `publisher` gives the
/// `remote_feed_url` of each channel-level remote item whose `medium` is
/// `music`. A feed whose `raw_medium` is `musicL` gives the `remote_feed_url`
/// of each channel-level remote item whose `medium` is `music` or has no
/// `medium`. Any other medium, including none, gives none of these links.
/// (`stophammer` ADR 0060 §5 for list feeds.)
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
///
/// The result holds at most [`MAX_FOLLOW_URLS_PER_FEED`] URLs for a non-list
/// feed, or at most [`MAX_FOLLOW_URLS_PER_LIST_FEED`] URLs for a `musicL`
/// feed, in the order this function finds them. When it drops URLs past that
/// count, it logs `fetched_url` and the number dropped, once.
/// (`stophammer` ADR 0054 §3, ADR 0060 §6.)
#[must_use]
pub fn follow_urls(feed: &IngestFeedData, fetched_url: &str) -> Vec<String> {
    let mut urls = all_follow_urls(feed, fetched_url);

    let is_list_feed = feed
        .raw_medium
        .as_deref()
        .is_some_and(|medium| medium.eq_ignore_ascii_case("musicL"));

    let max_urls = if is_list_feed {
        MAX_FOLLOW_URLS_PER_LIST_FEED
    } else {
        MAX_FOLLOW_URLS_PER_FEED
    };
    let owner = if is_list_feed {
        "ADR 0060 §6"
    } else {
        "ADR 0054 §3"
    };

    if urls.len() > max_urls {
        let dropped = urls.len() - max_urls;
        urls.truncate(max_urls);
        eprintln!(
            "follow: dropped {dropped} follow URL(s) of {fetched_url}, {owner} caps this \
             feed at {max_urls}"
        );
    }

    urls
}

/// Gives every follow URL of `feed`, with no cap. [`follow_urls`] is the
/// public entry point; it caps and logs what this function finds.
fn all_follow_urls(feed: &IngestFeedData, fetched_url: &str) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut urls = Vec::new();

    // Which item mediums a feed of this medium follows. A `musicL` feed
    // also follows an item with no `medium` (ADR 0060 §5).
    let (target_medium, list_feed) = match feed.raw_medium.as_deref() {
        Some(medium) if medium.eq_ignore_ascii_case("music") => (Some("publisher"), false),
        Some(medium) if medium.eq_ignore_ascii_case("publisher") => (Some("music"), false),
        Some(medium) if medium.eq_ignore_ascii_case("musicL") => (Some("music"), true),
        _ => (None, false),
    };

    if let Some(target_medium) = target_medium {
        let mut items_with_no_url = 0_usize;
        for item in &feed.remote_items {
            let accepted = match item.medium.as_deref() {
                Some(medium) => medium.eq_ignore_ascii_case(target_medium),
                None => list_feed,
            };
            if !accepted {
                continue;
            }
            let Some(url) = item.remote_feed_url.as_deref() else {
                items_with_no_url += 1;
                continue;
            };
            if !is_followable_url(url) {
                continue;
            }
            if seen.insert(url.to_string()) {
                urls.push(url.to_string());
            }
        }

        if list_feed && items_with_no_url > 0 {
            eprintln!(
                "follow: {items_with_no_url} music item(s) of {fetched_url} give no \
                 feedUrl, so the crawler cannot fetch them (ADR 0060 §5)"
            );
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
            item_guid: None,
            item_title: None,
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
    fn a_musicl_feed_with_one_music_item_with_feedurl_gives_that_url() {
        let data = feed(
            Some("musicL"),
            vec![remote_item(
                0,
                Some("music"),
                Some("https://music.example/feed.xml"),
            )],
        );

        assert_eq!(
            follow_urls(&data, FETCHED_URL),
            vec!["https://music.example/feed.xml".to_string()],
            "a musicL feed must give the URL of each music item (ADR 0060 §5)"
        );
    }

    #[test]
    fn a_musicl_feed_with_an_item_with_no_medium_gives_its_url() {
        let data = feed(
            Some("musicL"),
            vec![remote_item(0, None, Some("https://music.example/feed.xml"))],
        );

        assert_eq!(
            follow_urls(&data, FETCHED_URL),
            vec!["https://music.example/feed.xml".to_string()],
            "a musicL feed must give the URL of each item with no medium (ADR 0060 §5)"
        );
    }

    #[test]
    fn a_musicl_feed_with_a_publisher_item_gives_nothing_for_it() {
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
            "a musicL feed must not give URLs from publisher items (ADR 0060 §5)"
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
    fn a_publisher_level_music_feed_that_names_a_publisher_gives_nothing() {
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
            "FollowLevel::Publisher must give nothing when the fetched feed is not a publisher feed (ADR 0060 §5)"
        );
    }

    #[test]
    fn a_music_feed_with_300_publisher_links_gives_200() {
        let items = (0..300)
            .map(|i| {
                remote_item(
                    i,
                    Some("publisher"),
                    Some(&format!("https://pub{i}.example/feed.xml")),
                )
            })
            .collect();
        let data = feed(Some("music"), items);

        assert_eq!(
            follow_urls(&data, FETCHED_URL).len(),
            200,
            "ADR 0060 §6: a feed that is not a list keeps the limit of 200 (ADR 0054 §3)"
        );
    }

    #[test]
    fn a_feed_with_201_music_links_gives_200() {
        let remote_items: Vec<IngestRemoteFeedRef> = (0..201i64)
            .map(|position| {
                let url = format!("https://album-{position}.example/feed.xml");
                remote_item(position, Some("music"), Some(url.as_str()))
            })
            .collect();
        let data = feed(Some("publisher"), remote_items);

        let urls = follow_urls(&data, FETCHED_URL);

        assert_eq!(
            urls.len(),
            200,
            "a publisher feed that lists 201 music feeds must give at most 200 (ADR 0054 §3)"
        );
        assert_eq!(
            urls.first().map(String::as_str),
            Some("https://album-0.example/feed.xml"),
            "the cap must keep the URLs in the order follow_urls finds them, not drop from the front"
        );
        assert_eq!(
            urls.last().map(String::as_str),
            Some("https://album-199.example/feed.xml"),
            "the cap must drop only past the 200th URL"
        );
    }

    #[test]
    fn a_musicl_feed_with_1200_different_urls_gives_1000() {
        let remote_items: Vec<IngestRemoteFeedRef> = (0..1200i64)
            .map(|position| {
                let url = format!("https://track-{position}.example/feed.xml");
                remote_item(position, Some("music"), Some(url.as_str()))
            })
            .collect();
        let data = feed(Some("musicL"), remote_items);

        let urls = follow_urls(&data, FETCHED_URL);

        assert_eq!(
            urls.len(),
            1000,
            "a musicL feed that lists 1200 tracks must give at most 1000 (ADR 0060 §6)"
        );
        assert_eq!(
            urls.first().map(String::as_str),
            Some("https://track-0.example/feed.xml"),
            "the cap must keep the URLs in the order follow_urls finds them"
        );
        assert_eq!(
            urls.last().map(String::as_str),
            Some("https://track-999.example/feed.xml"),
            "the cap must drop only past the 1000th URL"
        );
    }

    #[test]
    fn a_musicl_feed_with_30_items_for_one_url_gives_one_url() {
        let remote_items: Vec<IngestRemoteFeedRef> = (0..30i64)
            .map(|position| {
                remote_item(
                    position,
                    Some("music"),
                    Some("https://album.example/feed.xml"),
                )
            })
            .collect();
        let data = feed(Some("musicL"), remote_items);

        assert_eq!(
            follow_urls(&data, FETCHED_URL),
            vec!["https://album.example/feed.xml".to_string()],
            "a musicL feed with 30 items for the same URL must give that URL only once (ADR 0060 §5)"
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
