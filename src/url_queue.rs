use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Returns the URL host used for coarse per-host scheduling.
#[must_use]
pub fn host_key(url: &str) -> Option<String> {
    reqwest::Url::parse(url)
        .ok()
        .and_then(|parsed| parsed.host_str().map(ToString::to_string))
}

/// Interleaves items by host so adjacent requests are less likely to hit the same host.
#[must_use]
pub fn interleave_by_host<T, F>(items: Vec<T>, mut host_of: F) -> Vec<T>
where
    F: FnMut(&T) -> Option<String>,
{
    let total = items.len();
    let mut buckets: HashMap<String, VecDeque<T>> = HashMap::new();
    let mut host_order = Vec::new();
    let mut seen_hosts = HashSet::new();

    for item in items {
        let host = host_of(&item).unwrap_or_else(|| "(unknown-host)".to_string());
        if seen_hosts.insert(host.clone()) {
            host_order.push(host.clone());
        }
        buckets.entry(host).or_default().push_back(item);
    }

    let mut interleaved = Vec::with_capacity(total);
    while interleaved.len() < total {
        let mut progressed = false;
        for host in &host_order {
            if let Some(bucket) = buckets.get_mut(host)
                && let Some(item) = bucket.pop_front()
            {
                interleaved.push(item);
                progressed = true;
            }
        }

        if !progressed {
            break;
        }
    }

    interleaved
}

/// The most follow URLs one pass may queue in total (`stophammer` ADR 0054
/// §3). A batch pass and a gossip pass each keep one [`FollowQueueLimit`]
/// for their own run.
pub const MAX_FOLLOW_QUEUE_URLS: usize = 50_000;

/// Caps the total follow URLs one pass admits to its queue (`stophammer`
/// ADR 0054 §3).
///
/// A batch pass and a gossip pass each build one of these, at the start of
/// the pass, and share it with every task that would add a follow URL. Past
/// the limit, [`try_admit`](Self::try_admit) gives `false`, and the caller
/// drops that URL instead of fetching it. [`dropped`](Self::dropped) gives
/// the running total, so the pass can log it once, at the end.
#[derive(Debug)]
pub struct FollowQueueLimit {
    limit: usize,
    admitted: AtomicUsize,
    dropped: AtomicUsize,
}

impl FollowQueueLimit {
    /// Builds a queue that admits at most `limit` follow URLs.
    #[must_use]
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            admitted: AtomicUsize::new(0),
            dropped: AtomicUsize::new(0),
        }
    }

    /// Tries to admit one more follow URL. Gives `true` while the queue has
    /// room. Past the limit, gives `false`, and counts the drop.
    ///
    /// This is one atomic compare-and-swap, so two tasks that call it at
    /// the same time can never both admit the one URL that fills the last
    /// slot.
    #[must_use]
    pub fn try_admit(&self) -> bool {
        let admitted = self
            .admitted
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                (count < self.limit).then_some(count + 1)
            });
        if admitted.is_err() {
            self.dropped.fetch_add(1, Ordering::AcqRel);
        }
        admitted.is_ok()
    }

    /// The number of follow URLs admitted so far.
    #[must_use]
    pub fn admitted(&self) -> usize {
        self.admitted.load(Ordering::Acquire)
    }

    /// The number of follow URLs dropped so far, past the limit.
    #[must_use]
    pub fn dropped(&self) -> usize {
        self.dropped.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::{FollowQueueLimit, host_key, interleave_by_host};

    #[test]
    fn host_key_extracts_hostname() {
        assert_eq!(
            host_key("https://example.com/feed.xml").as_deref(),
            Some("example.com")
        );
        assert_eq!(host_key("not-a-url"), None);
    }

    #[test]
    fn interleave_by_host_round_robins_hosts() {
        let urls = vec![
            "https://a.example/1".to_string(),
            "https://a.example/2".to_string(),
            "https://b.example/1".to_string(),
            "https://b.example/2".to_string(),
            "https://c.example/1".to_string(),
        ];

        let interleaved = interleave_by_host(urls, |url| host_key(url));

        assert_eq!(
            interleaved,
            vec![
                "https://a.example/1",
                "https://b.example/1",
                "https://c.example/1",
                "https://a.example/2",
                "https://b.example/2",
            ]
        );
    }

    #[test]
    fn a_follow_queue_admits_up_to_its_limit_then_drops() {
        let queue = FollowQueueLimit::new(2);

        assert!(
            queue.try_admit(),
            "the first URL must fit under a limit of 2"
        );
        assert!(
            queue.try_admit(),
            "the second URL must fit under a limit of 2"
        );
        assert!(
            !queue.try_admit(),
            "a pass whose follow queue reaches its limit must drop the next URL (ADR 0054 §3)"
        );
        assert!(
            !queue.try_admit(),
            "the queue must stay closed once it is full"
        );

        assert_eq!(
            queue.admitted(),
            2,
            "the queue must admit exactly the limit"
        );
        assert_eq!(
            queue.dropped(),
            2,
            "the queue must count every URL dropped past the limit"
        );
    }

    #[test]
    fn a_follow_queue_with_no_drops_reports_zero() {
        let queue = FollowQueueLimit::new(50_000);

        assert!(queue.try_admit());
        assert!(queue.try_admit());

        assert_eq!(queue.admitted(), 2);
        assert_eq!(
            queue.dropped(),
            0,
            "a pass that never reaches its limit must report zero dropped"
        );
    }
}
