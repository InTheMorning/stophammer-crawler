use std::collections::{HashMap, HashSet, VecDeque};

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

#[cfg(test)]
mod tests {
    use super::{host_key, interleave_by_host};

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
}
