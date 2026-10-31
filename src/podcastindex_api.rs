//! The `PodcastIndex` API client (`stophammer` ADR 0081 §1 to §3).
//!
//! The mode `podcastindex` sends one request to the music list. This module
//! reads the credentials, signs that request and gives the rows of the list.
//! No credential value reaches a log, an error text or a `Debug` output.

use std::fmt;

use serde_json::Value;
use sha1::{Digest, Sha1};

/// The base URL of the `PodcastIndex` API.
pub const DEFAULT_API_BASE: &str = "https://api.podcastindex.org/api/1.0";

/// The largest response body that the client accepts, in bytes.
const MAX_BODY_BYTES: usize = 64 * 1024 * 1024;

/// The number of body bytes that an HTTP status error shows.
const ERROR_BODY_BYTES: usize = 200;

/// The API key and the API secret of `PodcastIndex`.
///
/// The type has no `Clone`, `Serialize` or `Display`. Its `Debug` output hides
/// both values.
pub struct Credentials {
    key: String,
    secret: String,
}

impl Credentials {
    /// Read the credentials from `PODCASTINDEX_API_KEY` and
    /// `PODCASTINDEX_API_SECRET`.
    ///
    /// # Errors
    ///
    /// Returns an error text when a variable is missing or empty. The text
    /// holds no value.
    pub fn from_env() -> Result<Self, String> {
        let read = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
        match (
            read("PODCASTINDEX_API_KEY"),
            read("PODCASTINDEX_API_SECRET"),
        ) {
            (Some(key), Some(secret)) => Ok(Self { key, secret }),
            _ => Err("PODCASTINDEX_API_KEY and PODCASTINDEX_API_SECRET must be set".to_string()),
        }
    }
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(r#"Credentials { key: "<redacted>", secret: "<redacted>" }"#)
    }
}

/// One row of the music list. The mode uses only these three fields
/// (`stophammer` ADR 0081 §3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MusicListRow {
    /// The `PodcastIndex` feed ID.
    pub id: i64,
    /// The feed URL that `PodcastIndex` holds.
    pub url: String,
    /// The GUID that `PodcastIndex` holds. `None` when the list gives none.
    pub podcast_guid: Option<String>,
}

/// Give the `Authorization` value: the SHA-1 of `key`, `secret` and `date`,
/// in that sequence, as 40 lowercase hex digits.
#[must_use]
pub fn authorization_hex(key: &str, secret: &str, date: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(key.as_bytes());
    hasher.update(secret.as_bytes());
    hasher.update(date.as_bytes());
    hex::encode(hasher.finalize())
}

/// Give the three signed headers for a request at `unix_time`: `X-Auth-Key`,
/// `X-Auth-Date` and `Authorization`, in that sequence.
#[must_use]
pub fn auth_headers(credentials: &Credentials, unix_time: i64) -> [(&'static str, String); 3] {
    let date = unix_time.to_string();
    let authorization = authorization_hex(&credentials.key, &credentials.secret, &date);
    [
        ("X-Auth-Key", credentials.key.clone()),
        ("X-Auth-Date", date),
        ("Authorization", authorization),
    ]
}

/// Parse the body of the music list.
///
/// # Errors
///
/// Returns an error text when the body is not JSON, when `status` is not
/// true, or when a row has no `id` or no `url`.
pub fn parse_music_list(body: &[u8]) -> Result<Vec<MusicListRow>, String> {
    let json: Value =
        serde_json::from_slice(body).map_err(|e| format!("music list is not JSON: {e}"))?;
    let status_ok = match json.get("status") {
        Some(Value::Bool(b)) => *b,
        Some(Value::String(s)) => s == "true",
        _ => false,
    };
    if !status_ok {
        let description = json
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or("no description");
        return Err(format!("music list status is not true: {description}"));
    }
    let feeds = json
        .get("feeds")
        .and_then(Value::as_array)
        .ok_or_else(|| "music list has no feeds array".to_string())?;
    feeds
        .iter()
        .enumerate()
        .map(|(index, feed)| {
            let id = feed
                .get("id")
                .and_then(Value::as_i64)
                .ok_or_else(|| format!("music list row {index} has no id"))?;
            let url = feed
                .get("url")
                .and_then(Value::as_str)
                .ok_or_else(|| format!("music list row {index} has no url"))?
                .to_string();
            let podcast_guid = feed
                .get("podcastGuid")
                .and_then(Value::as_str)
                .map(str::to_string);
            Ok(MusicListRow {
                id,
                url,
                podcast_guid,
            })
        })
        .collect()
}

/// Send one signed request to the music list and give its rows.
///
/// # Errors
///
/// Returns an error text when the request fails, when the status is not 200,
/// when the body is over 64 MiB, or when the body does not parse. The text
/// holds no header value.
pub async fn fetch_music_list(
    client: &reqwest::Client,
    api_base: &str,
    credentials: &Credentials,
    user_agent: &str,
) -> Result<Vec<MusicListRow>, String> {
    let unix_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| "system clock is before 1970".to_string())?
        .as_secs();
    let unix_time =
        i64::try_from(unix_time).map_err(|_| "system clock is too large".to_string())?;
    let url = format!("{api_base}/podcasts/bymedium?medium=music");
    let mut request = client.get(&url).header("User-Agent", user_agent);
    for (name, value) in auth_headers(credentials, unix_time) {
        request = request.header(name, value);
    }
    let response = request
        .send()
        .await
        .map_err(|e| format!("music list request failed: {}", e.without_url()))?;
    let status = response.status();
    let mut response = response;
    let mut body: Vec<u8> = Vec::new();
    loop {
        let chunk = response
            .chunk()
            .await
            .map_err(|e| format!("music list body read failed: {}", e.without_url()))?;
        let Some(chunk) = chunk else { break };
        if body.len() + chunk.len() > MAX_BODY_BYTES {
            return Err("music list body is over 64 MiB".to_string());
        }
        body.extend_from_slice(&chunk);
    }
    if status.as_u16() != 200 {
        let head = &body[..body.len().min(ERROR_BODY_BYTES)];
        return Err(format!(
            "music list status {}: {}",
            status.as_u16(),
            String::from_utf8_lossy(head)
        ));
    }
    parse_music_list(&body)
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    const TWO_ROWS: &str = r#"{"status":"true","count":2,"feeds":[
        {"id":1,"url":"https://a.example/feed","podcastGuid":"guid-a","title":"x"},
        {"id":2,"url":"https://b.example/feed","podcastGuid":null}]}"#;

    fn test_credentials() -> Credentials {
        Credentials {
            key: "KEY-SECRET-TEST-1".to_string(),
            secret: "SECRET-VALUE-TEST-1".to_string(),
        }
    }

    #[test]
    fn authorization_hex_matches_the_published_example() {
        assert_eq!(
            authorization_hex(
                "UXKCGDSYGUUEVQJSYDZH",
                "yzJe2eE7XV-3eY576dyRZ6wXyAbndh6LUrCZ8KN|",
                "1613713388"
            ),
            "73a1fffed61c1d30d858beb1fc48f355386449d2",
            "SHA-1 of key, secret and date"
        );
    }

    #[test]
    fn auth_headers_have_three_names_in_sequence() {
        let headers = auth_headers(&test_credentials(), 1_613_713_388);
        assert_eq!(headers[0].0, "X-Auth-Key");
        assert_eq!(headers[0].1, "KEY-SECRET-TEST-1");
        assert_eq!(headers[1].0, "X-Auth-Date");
        assert_eq!(headers[1].1, "1613713388");
        assert_eq!(headers[2].0, "Authorization");
        assert_eq!(headers[2].1.len(), 40, "Authorization is 40 hex digits");
    }

    #[test]
    fn credentials_hide_their_values() {
        let debug = format!("{:?}", test_credentials());
        assert!(!debug.contains("KEY-SECRET-TEST-1"), "Debug shows the key");
        assert!(
            !debug.contains("SECRET-VALUE-TEST-1"),
            "Debug shows the secret"
        );
        // The test run sets no variable of these names.
        let error = Credentials::from_env().expect_err("no variables are set");
        assert!(!error.contains("KEY-SECRET-TEST-1"));
        assert!(!error.contains("SECRET-VALUE-TEST-1"));
        assert!(error.contains("must be set"));
    }

    #[test]
    fn parse_gives_some_and_none_for_the_guid() {
        let rows = parse_music_list(TWO_ROWS.as_bytes()).expect("two rows parse");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].podcast_guid.as_deref(), Some("guid-a"));
        assert_eq!(rows[0].id, 1);
        assert_eq!(rows[1].podcast_guid, None);
        assert_eq!(rows[1].url, "https://b.example/feed");
    }

    #[test]
    fn parse_refuses_a_false_status_with_its_description() {
        let body = r#"{"status":"false","description":"Invalid credentials"}"#;
        let error = parse_music_list(body.as_bytes()).expect_err("status is false");
        assert!(error.contains("Invalid credentials"), "got: {error}");
    }

    #[test]
    fn parse_refuses_a_row_with_no_url() {
        let body = r#"{"status":true,"count":1,"feeds":[{"id":1}]}"#;
        assert!(parse_music_list(body.as_bytes()).is_err());
    }

    #[test]
    fn parse_refuses_a_body_that_is_not_json() {
        assert!(parse_music_list(b"<html>").is_err());
    }

    /// Serve one answer on `127.0.0.1` and give back the request head.
    async fn stub(
        status_line: &'static str,
        body: &'static str,
    ) -> (String, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let base = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut head = Vec::new();
            let mut buf = [0_u8; 1024];
            while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = stream.read(&mut buf).await.expect("read request");
                if n == 0 {
                    break;
                }
                head.extend_from_slice(&buf[..n]);
            }
            let response = format!(
                "{status_line}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("write response");
            let _ = stream.shutdown().await;
            String::from_utf8_lossy(&head).to_ascii_lowercase()
        });
        (base, handle)
    }

    #[tokio::test]
    async fn fetch_gives_rows_and_sends_the_headers() {
        let (base, handle) = stub("HTTP/1.1 200 OK", TWO_ROWS).await;
        let rows = fetch_music_list(
            &reqwest::Client::new(),
            &base,
            &test_credentials(),
            "test-agent/1",
        )
        .await
        .expect("fetch succeeds");
        assert_eq!(rows.len(), 2);
        let request = handle.await.expect("stub task");
        assert!(request.contains("get /podcasts/bymedium?medium=music"));
        assert!(request.contains("x-auth-key: key-secret-test-1"));
        assert!(request.contains("x-auth-date: "));
        assert!(request.contains("authorization: "));
        assert!(request.contains("user-agent: test-agent/1"));
    }

    #[tokio::test]
    async fn fetch_error_for_401_holds_the_status_and_no_key() {
        let (base, _handle) = stub("HTTP/1.1 401 Unauthorized", "denied").await;
        let error = fetch_music_list(&reqwest::Client::new(), &base, &test_credentials(), "ua")
            .await
            .expect_err("401 is an error");
        assert!(error.contains("401"), "got: {error}");
        assert!(!error.contains("KEY-SECRET-TEST-1"));
        assert!(!error.contains("SECRET-VALUE-TEST-1"));
    }
}
