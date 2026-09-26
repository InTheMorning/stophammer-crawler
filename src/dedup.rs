use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::ping_window::PingWindows;

struct PingEntry {
    processed_at: Instant,
    ping_count: u32,
}

/// In-memory state of the gossip mode.
///
/// `seen` is the cooldown of a follow URL (`stophammer` ADR 0062 §5): the
/// crawler makes a follow URL itself, so a repeat inside the cooldown is not
/// fetched. `pings` holds the podping windows of ADR 0062, and a podping is
/// never dropped.
pub struct Dedup {
    pub pings: PingWindows,
    seen: HashMap<String, PingEntry>,
    cooldown: Duration,
    spam_cooldown: Duration,
    spam_threshold: u32,
}

impl Default for Dedup {
    fn default() -> Self {
        Self::new()
    }
}

impl Dedup {
    pub fn new() -> Self {
        Self {
            pings: PingWindows::new(),
            seen: HashMap::new(),
            cooldown: Duration::from_secs(5 * 60),
            spam_cooldown: Duration::from_secs(30 * 60),
            spam_threshold: 5,
        }
    }

    /// Returns `true` if the follow URL should be fetched (not in cooldown).
    pub fn should_process(&mut self, url: &str) -> bool {
        let now = Instant::now();

        if let Some(entry) = self.seen.get_mut(url) {
            let effective_cooldown = if entry.ping_count >= self.spam_threshold {
                self.spam_cooldown
            } else {
                self.cooldown
            };

            if now.duration_since(entry.processed_at) < effective_cooldown {
                entry.ping_count += 1;
                return false;
            }

            // Cooldown expired — reset and reprocess
            entry.processed_at = now;
            entry.ping_count = 1;
            true
        } else {
            self.seen.insert(
                url.to_string(),
                PingEntry {
                    processed_at: now,
                    ping_count: 1,
                },
            );
            true
        }
    }

    /// Remove entries older than the spam cooldown, and old podping windows
    /// (called periodically).
    pub fn cleanup(&mut self) {
        let cutoff = self.spam_cooldown;
        let now = Instant::now();
        self.pings.cleanup(now);
        self.seen
            .retain(|_, entry| now.duration_since(entry.processed_at) < cutoff);
    }
}
