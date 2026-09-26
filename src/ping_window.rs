//! The podping window of each URL (`stophammer` ADR 0062).
//!
//! A podping is never dropped. The first podping of a URL with no open window
//! is crawled at once, and the crawl task owns the URL until its window
//! closes. A podping inside the window sets a pending mark. When the window
//! closes, the owner crawls once more if the mark is set, or releases the
//! URL. The window is 30 seconds, and doubles to at most 1 hour after each
//! crawl that changes nothing. A `live` or `liveEnd` podping wakes the owner
//! at once.
//!
//! The state is in memory (ADR 0062 §6).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Notify;

/// The window of a new URL, and of a URL whose last crawl changed the index.
pub const BASE_WINDOW: Duration = Duration::from_secs(30);

/// The longest window of a URL whose crawls change nothing.
pub const MAX_WINDOW: Duration = Duration::from_secs(60 * 60);

/// What a podping gives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PingDecision {
    /// Crawl now. The caller owns the URL until [`PingWindows::end_window`]
    /// releases it.
    CrawlNow,
    /// The URL has an owner. The podping set its pending mark.
    Merged,
}

struct Entry {
    window: Duration,
    owned: bool,
    pending: bool,
    until: Instant,
    live: Arc<Notify>,
}

/// The windows of all URLs with a podping (ADR 0062).
#[derive(Default)]
pub struct PingWindows {
    entries: HashMap<String, Entry>,
}

impl PingWindows {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a podping for `url` at `now`. `live` is true for the reasons
    /// `live` and `liveEnd` (ADR 0062 §4).
    pub fn on_ping(&mut self, url: &str, live: bool, now: Instant) -> PingDecision {
        match self.entries.get_mut(url) {
            None => {
                self.entries.insert(
                    url.to_string(),
                    Entry {
                        window: BASE_WINDOW,
                        owned: true,
                        pending: false,
                        until: now + BASE_WINDOW,
                        live: Arc::new(Notify::new()),
                    },
                );
                PingDecision::CrawlNow
            }
            Some(entry) if !entry.owned => {
                entry.owned = true;
                entry.pending = false;
                PingDecision::CrawlNow
            }
            Some(entry) => {
                entry.pending = true;
                if live {
                    entry.live.notify_one();
                }
                PingDecision::Merged
            }
        }
    }

    /// Records the end of a crawl of `url` that started at `started`, and
    /// gives the time that the window closes and the signal of a live
    /// podping.
    ///
    /// `changed` is true when the node accepted a change. Then the window is
    /// [`BASE_WINDOW`]. Else it doubles, to at most [`MAX_WINDOW`]
    /// (ADR 0062 §3). A crawl that a live podping started keeps the window
    /// that was open (`keep_window`, ADR 0062 §4).
    pub fn finish_crawl(
        &mut self,
        url: &str,
        changed: bool,
        started: Instant,
        keep_window: bool,
    ) -> (Instant, Arc<Notify>) {
        let entry = self
            .entries
            .entry(url.to_string())
            .or_insert_with(|| Entry {
                window: BASE_WINDOW,
                owned: true,
                pending: false,
                until: started + BASE_WINDOW,
                live: Arc::new(Notify::new()),
            });
        if !keep_window {
            entry.window = if changed {
                BASE_WINDOW
            } else {
                (entry.window * 2).min(MAX_WINDOW)
            };
            entry.until = started + entry.window;
        }
        (entry.until, Arc::clone(&entry.live))
    }

    /// Called by the owner when the window of `url` closes. Gives `true`
    /// when a podping set the pending mark: the owner keeps the URL and
    /// crawls again. Gives `false` and releases the URL otherwise.
    pub fn end_window(&mut self, url: &str) -> bool {
        let Some(entry) = self.entries.get_mut(url) else {
            return false;
        };
        if entry.pending {
            entry.pending = false;
            true
        } else {
            entry.owned = false;
            false
        }
    }

    /// Clears the pending mark before a crawl that a live podping started.
    pub fn clear_pending(&mut self, url: &str) {
        if let Some(entry) = self.entries.get_mut(url) {
            entry.pending = false;
        }
    }

    /// The window of `url`, for tests.
    #[cfg(test)]
    #[must_use]
    pub fn window(&self, url: &str) -> Option<Duration> {
        self.entries.get(url).map(|entry| entry.window)
    }

    /// Removes each released URL whose window closed more than
    /// [`MAX_WINDOW`] before `now`. A URL that returns after that starts
    /// again at [`BASE_WINDOW`].
    pub fn cleanup(&mut self, now: Instant) {
        self.entries
            .retain(|_, entry| entry.owned || now < entry.until + MAX_WINDOW);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const URL: &str = "https://feeds.example/album.xml";

    #[test]
    fn a_burst_gives_one_crawl_now_and_one_when_the_window_closes() {
        let mut windows = PingWindows::new();
        let t0 = Instant::now();
        assert_eq!(
            windows.on_ping(URL, false, t0),
            PingDecision::CrawlNow,
            "ADR 0062 §1: the first podping is crawled at once"
        );
        for s in [3, 6, 10] {
            assert_eq!(
                windows.on_ping(URL, false, t0 + Duration::from_secs(s)),
                PingDecision::Merged,
                "ADR 0062 §1: a podping inside the window is merged, not dropped"
            );
        }
        windows.finish_crawl(URL, true, t0, false);
        assert!(
            windows.end_window(URL),
            "ADR 0062 §1: the merged podpings give one more crawl when the window closes"
        );
        windows.finish_crawl(URL, true, t0 + BASE_WINDOW, false);
        assert!(
            !windows.end_window(URL),
            "with no podping in the second window, the URL is released"
        );
        assert_eq!(
            windows.on_ping(URL, false, t0 + Duration::from_secs(120)),
            PingDecision::CrawlNow,
            "after the release, the next podping is crawled at once"
        );
    }

    #[test]
    fn one_podping_inside_the_window_is_crawled_when_it_closes() {
        let mut windows = PingWindows::new();
        let t0 = Instant::now();
        windows.on_ping(URL, false, t0);
        windows.finish_crawl(URL, true, t0, false);
        assert_eq!(
            windows.on_ping(URL, false, t0 + Duration::from_secs(20)),
            PingDecision::Merged,
            "the second podping is inside the window"
        );
        assert!(
            windows.end_window(URL),
            "ADR 0062 guard: the merged podping gives a crawl when the window closes"
        );
    }

    #[test]
    fn no_change_doubles_the_window_and_a_change_resets_it() {
        let mut windows = PingWindows::new();
        let t0 = Instant::now();
        windows.on_ping(URL, false, t0);
        let mut expected = BASE_WINDOW;
        for _ in 0..10 {
            windows.finish_crawl(URL, false, t0, false);
            expected = (expected * 2).min(MAX_WINDOW);
            assert_eq!(
                windows.window(URL),
                Some(expected),
                "ADR 0062 §3: a crawl with no change doubles the window, to at most 1 hour"
            );
        }
        assert_eq!(
            windows.window(URL),
            Some(MAX_WINDOW),
            "the window stops at 1 hour"
        );
        windows.finish_crawl(URL, true, t0, false);
        assert_eq!(
            windows.window(URL),
            Some(BASE_WINDOW),
            "ADR 0062 §3: an accepted change resets the window to 30 seconds"
        );
    }

    #[test]
    fn the_window_closes_at_the_start_of_the_crawl_plus_the_window() {
        let mut windows = PingWindows::new();
        let t0 = Instant::now();
        windows.on_ping(URL, false, t0);
        let (until, _) = windows.finish_crawl(URL, false, t0, false);
        assert_eq!(
            until,
            t0 + 2 * BASE_WINDOW,
            "a no-change crawl gives a 60 s window"
        );
    }

    #[tokio::test]
    async fn a_live_podping_wakes_the_owner_at_once() {
        let mut windows = PingWindows::new();
        let t0 = Instant::now();
        windows.on_ping(URL, false, t0);
        let (until, live) = windows.finish_crawl(URL, true, t0, false);
        assert_eq!(
            windows.on_ping(URL, true, t0 + Duration::from_secs(5)),
            PingDecision::Merged,
            "the owner still holds the URL"
        );
        let woken = tokio::time::timeout(Duration::from_millis(50), live.notified()).await;
        assert!(
            woken.is_ok(),
            "ADR 0062 §4: a live podping inside a window wakes the owner for a crawl at once"
        );
        windows.clear_pending(URL);
        let (until_after, _) = windows.finish_crawl(URL, true, t0 + Duration::from_secs(6), true);
        assert_eq!(
            until_after, until,
            "ADR 0062 §4: a live crawl keeps the open window"
        );
    }

    #[tokio::test]
    async fn an_update_podping_does_not_wake_the_owner() {
        let mut windows = PingWindows::new();
        let t0 = Instant::now();
        windows.on_ping(URL, false, t0);
        let (_, live) = windows.finish_crawl(URL, true, t0, false);
        windows.on_ping(URL, false, t0 + Duration::from_secs(5));
        let woken = tokio::time::timeout(Duration::from_millis(50), live.notified()).await;
        assert!(
            woken.is_err(),
            "an update podping waits for the window to close"
        );
    }

    #[test]
    fn a_podping_after_a_follow_fetch_is_crawled_at_once() {
        let mut state = crate::dedup::Dedup::new();
        assert!(
            state.should_process(URL),
            "the follow fetch of the URL runs"
        );
        assert_eq!(
            state.pings.on_ping(URL, false, Instant::now()),
            PingDecision::CrawlNow,
            "ADR 0062 §5: a follow fetch opens no window, so a podping just after it is crawled at once"
        );
    }

    #[test]
    fn cleanup_keeps_the_owner_and_a_recent_window() {
        let mut windows = PingWindows::new();
        let t0 = Instant::now();
        windows.on_ping("https://a.example/owned.xml", false, t0);
        windows.on_ping(URL, false, t0);
        windows.finish_crawl(URL, false, t0, false);
        assert!(!windows.end_window(URL));
        windows.cleanup(t0 + Duration::from_secs(60));
        assert!(
            windows.window(URL).is_some(),
            "a recent window keeps its length"
        );
        windows.cleanup(t0 + MAX_WINDOW + Duration::from_secs(120));
        assert!(
            windows.window(URL).is_none(),
            "an old released window is removed"
        );
        assert!(
            windows.window("https://a.example/owned.xml").is_some(),
            "an owned URL is never removed"
        );
    }
}
