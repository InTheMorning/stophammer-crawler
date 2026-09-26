//! SSRF guard for the crawler's own feed fetches.
//!
//! `stophammer` ADR 0054 owns the rule this module applies. A fetch of a
//! URL from RSS, or from a podping, connects only to a public address.
//! This rule applies to the first URL and to each redirect hop. This crate
//! does not depend on the `stophammer` node crate (ADR 0054 §5). It keeps
//! its own copy of the rule.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use reqwest::Url;
use reqwest::dns::{Addrs, Name, Resolve, Resolving};

/// `true` only for a public unicast address (`stophammer` ADR 0054 §1).
///
/// An IPv4-mapped (`::ffff:0:0/96`), IPv4-compatible (`::/96`) or NAT64
/// (`64:ff9b::/96`) IPv6 address takes the result of the IPv4 address
/// inside it.
#[must_use]
pub fn is_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_public_ipv4(v4),
        IpAddr::V6(v6) => is_public_ipv6(v6),
    }
}

/// `true` only for a public IPv4 unicast address (`stophammer` ADR 0054
/// §1): not loopback, private, link-local, CGNAT, multicast, broadcast,
/// unspecified, documentation or benchmark.
fn is_public_ipv4(v4: Ipv4Addr) -> bool {
    if v4.is_loopback()
        || v4.is_private()
        || v4.is_link_local()
        || v4.is_broadcast()
        || v4.is_unspecified()
        || v4.is_multicast()
    {
        return false;
    }

    let octets = v4.octets();
    let is_cgnat = octets[0] == 100 && (octets[1] & 0xC0) == 64; // 100.64.0.0/10
    let is_test_net_1 = octets[0] == 192 && octets[1] == 0 && octets[2] == 2; // 192.0.2.0/24
    let is_test_net_2 = octets[0] == 198 && octets[1] == 51 && octets[2] == 100; // 198.51.100.0/24
    let is_test_net_3 = octets[0] == 203 && octets[1] == 0 && octets[2] == 113; // 203.0.113.0/24
    let is_benchmark = octets[0] == 198 && (octets[1] & 0xFE) == 18; // 198.18.0.0/15

    !(is_cgnat || is_test_net_1 || is_test_net_2 || is_test_net_3 || is_benchmark)
}

/// `true` only for a public IPv6 address (`stophammer` ADR 0054 §1): not
/// loopback, unspecified, multicast, unique local, link-local or
/// documentation. An embedded IPv4 address (mapped, compatible or NAT64)
/// takes the result of [`is_public_ipv4`] on the address inside it.
fn is_public_ipv6(v6: Ipv6Addr) -> bool {
    if v6.is_loopback() || v6.is_unspecified() || v6.is_multicast() {
        return false;
    }
    if is_unique_local(v6) || is_unicast_link_local(v6) || is_documentation_v6(v6) {
        return false;
    }
    if let Some(v4) = embedded_ipv4(v6) {
        return is_public_ipv4(v4);
    }
    true
}

/// `fc00::/7`, the unique local range.
fn is_unique_local(v6: Ipv6Addr) -> bool {
    (v6.segments()[0] & 0xFE00) == 0xFC00
}

/// `fe80::/10`, the link-local range.
fn is_unicast_link_local(v6: Ipv6Addr) -> bool {
    (v6.segments()[0] & 0xFFC0) == 0xFE80
}

/// `2001:db8::/32`, reserved for documentation.
fn is_documentation_v6(v6: Ipv6Addr) -> bool {
    let segments = v6.segments();
    segments[0] == 0x2001 && segments[1] == 0x0db8
}

/// Extracts the IPv4 address inside an IPv4-mapped (`::ffff:0:0/96`),
/// IPv4-compatible (`::/96`) or NAT64 (`64:ff9b::/96`) IPv6 address
/// (`stophammer` ADR 0054 §1). [`is_public_ipv6`] checks loopback and
/// unspecified first. `::1` and `::` never reach this function through
/// that path.
fn embedded_ipv4(v6: Ipv6Addr) -> Option<Ipv4Addr> {
    match v6.segments() {
        // ::ffff:a.b.c.d (IPv4-mapped), ::a.b.c.d (IPv4-compatible), or
        // 64:ff9b::a.b.c.d (NAT64)
        [0, 0, 0, 0, 0, 0 | 0xFFFF, hi, lo] | [0x0064, 0xFF9B, 0, 0, 0, 0, hi, lo] => {
            let [a, b] = hi.to_be_bytes();
            let [c, d] = lo.to_be_bytes();
            Some(Ipv4Addr::new(a, b, c, d))
        }
        _ => None,
    }
}

/// Rejects a scheme other than `http` or `https`, a URL with a user name or
/// a password, and a host that is an IP literal and not public
/// (`stophammer` ADR 0054 §1).
///
/// [`PublicOnlyResolver`] checks a host name. It never sees an IP literal.
/// This function checks the IP literal case.
///
/// # Errors
///
/// Returns a reason that starts with `fetch_target_not_public` when the
/// target is rejected.
pub fn check_target(url: &Url) -> Result<(), String> {
    match url.scheme() {
        "http" | "https" => {}
        scheme => {
            return Err(format!(
                "fetch_target_not_public: scheme {scheme} is not allowed"
            ));
        }
    }

    if !url.username().is_empty() || url.password().is_some() {
        return Err(
            "fetch_target_not_public: the URL carries a user name or a password".to_string(),
        );
    }

    if let Some(host) = url.host_str() {
        let literal = host.trim_start_matches('[').trim_end_matches(']');
        if let Ok(ip) = literal.parse::<IpAddr>()
            && !is_public_ip(ip)
        {
            return Err(format!("fetch_target_not_public: {ip}"));
        }
    }

    Ok(())
}

/// A `reqwest` DNS resolver that accepts only a public answer
/// (`stophammer` ADR 0054 §1).
///
/// It resolves the name with [`tokio::net::lookup_host`]. Then it rejects
/// an empty answer. It also rejects an answer that holds an address that
/// is not public. A client sets this resolver through
/// [`ClientBuilder::dns_resolver`](reqwest::ClientBuilder::dns_resolver).
/// Such a client connects only to an address this resolver accepted. A
/// second DNS answer cannot change the target.
#[derive(Debug)]
pub struct PublicOnlyResolver;

impl Resolve for PublicOnlyResolver {
    fn resolve(&self, name: Name) -> Resolving {
        Box::pin(async move {
            let host = name.as_str().to_string();

            let addrs: Vec<SocketAddr> = match tokio::net::lookup_host((host.as_str(), 0)).await {
                Ok(iter) => iter.collect(),
                Err(e) => {
                    let boxed: Box<dyn std::error::Error + Send + Sync> = Box::new(e);
                    return Err(boxed);
                }
            };

            if !resolved_addrs_are_public(&addrs) {
                let boxed: Box<dyn std::error::Error + Send + Sync> =
                    Box::new(NonPublicTarget::new(host));
                return Err(boxed);
            }

            let iter: Addrs = Box::new(addrs.into_iter());
            Ok(iter)
        })
    }
}

/// `true` only when `addrs` is non-empty and every address in it is public
/// (`stophammer` ADR 0054 §1). [`PublicOnlyResolver`] uses this on its own
/// DNS answer.
fn resolved_addrs_are_public(addrs: &[SocketAddr]) -> bool {
    !addrs.is_empty() && addrs.iter().all(|addr| is_public_ip(addr.ip()))
}

/// The mark [`PublicOnlyResolver`] leaves on a rejected answer
/// (`stophammer` ADR 0054 §1). [`non_public_rejection`] finds it in the
/// error chain that `reqwest` builds around it.
#[derive(Debug)]
struct NonPublicTarget {
    host: String,
}

impl NonPublicTarget {
    fn new(host: String) -> Self {
        Self { host }
    }
}

impl std::fmt::Display for NonPublicTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "fetch_target_not_public: {}", self.host)
    }
}

impl std::error::Error for NonPublicTarget {}

/// Finds [`PublicOnlyResolver`]'s rejection in the error chain of a failed
/// request (`stophammer` ADR 0054 §1). `reqwest` wraps the resolver's error
/// inside its own chain of causes. This function walks each `source()`. It
/// stops when it finds the mark, or when the chain ends.
#[must_use]
pub fn non_public_rejection(err: &(dyn std::error::Error + 'static)) -> Option<String> {
    let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(current) = cause {
        if let Some(target) = current.downcast_ref::<NonPublicTarget>() {
            return Some(target.to_string());
        }
        cause = current.source();
    }
    None
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use reqwest::Url;

    use super::{check_target, is_public_ip, resolved_addrs_are_public};

    fn v4(a: u8, b: u8, c: u8, d: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(a, b, c, d))
    }

    fn v6(s: &str) -> IpAddr {
        s.parse().expect("valid IPv6 literal in a test case")
    }

    /// `stophammer` ADR 0054 §5, each address case, for [`is_public_ip`].
    #[test]
    fn is_public_ip_rejects_each_case_of_adr_0054_section_5() {
        let rejected = [
            ("IPv4 loopback", v4(127, 0, 0, 1)),
            ("IPv4 private 10/8", v4(10, 0, 0, 1)),
            ("IPv4 private 172.16/12", v4(172, 16, 0, 1)),
            ("IPv4 private 192.168/16", v4(192, 168, 0, 1)),
            ("IPv4 link-local", v4(169, 254, 169, 254)),
            ("IPv4 CGNAT", v4(100, 64, 0, 1)),
            ("IPv4 broadcast", v4(255, 255, 255, 255)),
            ("IPv4 unspecified", v4(0, 0, 0, 0)),
            ("IPv4 multicast", v4(224, 0, 0, 1)),
            ("IPv4 documentation TEST-NET-1", v4(192, 0, 2, 1)),
            ("IPv4 documentation TEST-NET-2", v4(198, 51, 100, 1)),
            ("IPv4 documentation TEST-NET-3", v4(203, 0, 113, 1)),
            ("IPv4 benchmark", v4(198, 18, 0, 1)),
            ("IPv6 loopback", v6("::1")),
            ("IPv6 unspecified", v6("::")),
            ("IPv6 unique local", v6("fc00::1")),
            ("IPv6 link-local", v6("fe80::1")),
            ("IPv6 multicast", v6("ff02::1")),
            ("IPv6 documentation", v6("2001:db8::1")),
            ("IPv4-mapped of a private address", v6("::ffff:10.0.0.1")),
            ("IPv4-compatible of a loopback address", v6("::127.0.0.1")),
            ("NAT64 of a loopback address", v6("64:ff9b::7f00:1")),
        ];

        for (label, ip) in rejected {
            assert!(!is_public_ip(ip), "{label} ({ip}) must not be public");
        }
    }

    #[test]
    fn is_public_ip_accepts_a_public_address_of_each_family() {
        assert!(
            is_public_ip(v4(1, 1, 1, 1)),
            "a public IPv4 address must be public"
        );
        assert!(
            is_public_ip(v6("2606:4700:4700::1111")),
            "a public IPv6 address must be public"
        );
        assert!(
            is_public_ip(v6("::ffff:1.1.1.1")),
            "an IPv4-mapped public address must be public"
        );
    }

    #[test]
    fn check_target_accepts_a_public_https_url() {
        let url = Url::parse("https://example.com/feed.xml").expect("valid URL");
        assert!(
            check_target(&url).is_ok(),
            "a public https URL must be accepted"
        );
    }

    /// `stophammer` ADR 0054 §5, each URL case a literal host or a scheme
    /// can decide, for [`check_target`]. `check_target` never sees a host
    /// name's resolved address. A case that needs DNS resolution, a name
    /// that resolves to a private address, is covered by
    /// [`resolved_addrs_are_public`]'s own tests below.
    #[test]
    fn check_target_rejects_each_literal_case_of_adr_0054_section_5() {
        let cases = [
            "http://127.0.0.1/",
            "http://[::1]/",
            "http://10.0.0.1/",
            "http://172.16.0.1/",
            "http://192.168.0.1/",
            "http://169.254.169.254/",
            "http://100.64.0.1/",
            "http://[fc00::1]/",
            "http://[fe80::1]/",
            "http://[::ffff:127.0.0.1]/",
            "http://[64:ff9b::a00:1]/",
            "ftp://example.com/feed.xml",
            "file:///etc/passwd",
            "http://user:pass@example.com/",
        ];

        for case in cases {
            let url = Url::parse(case).unwrap_or_else(|e| panic!("{case} must parse: {e}"));
            assert!(
                check_target(&url).is_err(),
                "{case} must be rejected by ADR 0054 §5"
            );
        }
    }

    #[test]
    fn a_rejection_reason_starts_with_the_adr_0054_marker() {
        let url = Url::parse("http://127.0.0.1/").expect("valid URL");
        let reason = check_target(&url).expect_err("a loopback target must be rejected");
        assert!(
            reason.starts_with("fetch_target_not_public"),
            "the reason must start with fetch_target_not_public, got {reason}"
        );
    }

    /// Stands in for the resolver's own check on a name that resolves to
    /// `127.0.0.1` (`stophammer` ADR 0054 §5). [`PublicOnlyResolver`] runs
    /// exactly this check on the addresses that `tokio::net::lookup_host`
    /// gives it.
    #[test]
    fn a_resolver_answer_with_one_non_public_address_is_rejected() {
        let addrs = [SocketAddr::new(v4(127, 0, 0, 1), 443)];
        assert!(
            !resolved_addrs_are_public(&addrs),
            "an answer holding 127.0.0.1 must fail the resolver's check"
        );
    }

    #[test]
    fn an_empty_resolver_answer_is_rejected() {
        assert!(
            !resolved_addrs_are_public(&[]),
            "an empty DNS answer must fail the resolver's check"
        );
    }

    #[test]
    fn a_resolver_answer_of_only_public_addresses_is_accepted() {
        let addrs = [SocketAddr::new(v4(1, 1, 1, 1), 443)];
        assert!(
            resolved_addrs_are_public(&addrs),
            "an answer holding only public addresses must pass the resolver's check"
        );
    }
}
