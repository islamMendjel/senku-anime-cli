use crate::error::{Result, SenkuError};
use crate::models::{Resolution, StreamSource};
use crate::providers::common::expand_hls_streams;
use futures::StreamExt;
use reqwest::header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, RANGE, REFERER};
use reqwest::{Client, StatusCode};
use std::cmp::Reverse;
use std::collections::HashSet;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
use url::{Host, Url};

const PROBE_SAMPLE_BYTES: usize = 262_144; // 256 KiB
const THROUGHPUT_PROBE_TIMEOUT: Duration = Duration::from_millis(2200);
const OVERALL_VERIFY_DEADLINE: Duration = Duration::from_millis(4000);

/// Resolves the best playable stream URL using concurrent HEAD + 256 KiB
/// throughput probing and strict highest-resolution + fastest-mirror ranking.
#[derive(Debug, Clone)]
pub struct StreamResolver {
    client: Client,
    probe_timeout: Duration,
}

impl StreamResolver {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            probe_timeout: THROUGHPUT_PROBE_TIMEOUT,
        }
    }

    pub fn with_timeout(client: Client, probe_timeout: Duration) -> Self {
        Self {
            client,
            probe_timeout,
        }
    }

    /// Ranks streams and concurrently verifies their availability and throughput.
    pub async fn resolve_best(
        &self,
        episode_num: u32,
        candidates: Vec<StreamSource>,
        preferred: Option<Resolution>,
    ) -> Result<StreamSource> {
        let mut ranked = self
            .resolve_ranked_verified(episode_num, candidates, preferred)
            .await?;
        Ok(ranked.remove(0))
    }

    /// Returns all verified streams ordered highest priority & fastest throughput
    /// first so callers can fail over cleanly or display mirror speeds.
    pub async fn resolve_ranked_verified(
        &self,
        episode_num: u32,
        candidates: Vec<StreamSource>,
        preferred: Option<Resolution>,
    ) -> Result<Vec<StreamSource>> {
        self.resolve_ranked_verified_with_penalties(
            episode_num,
            candidates,
            preferred,
            &HashSet::new(),
        )
        .await
    }

    /// Returns strictly verified streams (`verified = true`), failing with `NoValidStreams`
    /// if no candidate passes HTTP reachability probing.
    pub async fn resolve_strict_verified(
        &self,
        episode_num: u32,
        candidates: Vec<StreamSource>,
        preferred: Option<Resolution>,
    ) -> Result<Vec<StreamSource>> {
        self.resolve_strict_verified_with_penalties(
            episode_num,
            candidates,
            preferred,
            &HashSet::new(),
        )
        .await
    }

    /// Strict verification with session stalled-host penalties (never falls back to
    /// unverified streams so callers can try alternate providers first).
    pub async fn resolve_strict_verified_with_penalties(
        &self,
        episode_num: u32,
        candidates: Vec<StreamSource>,
        preferred: Option<Resolution>,
        stalled_hosts: &HashSet<String>,
    ) -> Result<Vec<StreamSource>> {
        if candidates.is_empty() {
            return Err(SenkuError::NoValidStreams {
                episode: episode_num,
            });
        }

        let expanded = self
            .expand_and_rank_candidates(candidates, preferred, stalled_hosts)
            .await;
        let verified = self
            .probe_candidates(expanded, preferred, stalled_hosts)
            .await;

        if verified.is_empty() {
            return Err(SenkuError::NoValidStreams {
                episode: episode_num,
            });
        }

        Ok(verified)
    }

    /// Returns all verified streams ordered with session stalled-host penalties
    /// and smart CDN throughput tie-breaking applied. If strict probing drops all
    /// candidates, retains up to 4 unique valid `http(s)` candidates marked `verified = false`
    /// (`[Unverified]`) so `mpv` can still attempt direct playback.
    pub async fn resolve_ranked_verified_with_penalties(
        &self,
        episode_num: u32,
        candidates: Vec<StreamSource>,
        preferred: Option<Resolution>,
        stalled_hosts: &HashSet<String>,
    ) -> Result<Vec<StreamSource>> {
        if candidates.is_empty() {
            return Err(SenkuError::NoValidStreams {
                episode: episode_num,
            });
        }

        let expanded = self
            .expand_and_rank_candidates(candidates, preferred, stalled_hosts)
            .await;
        let verified = self
            .probe_candidates(expanded.clone(), preferred, stalled_hosts)
            .await;

        if !verified.is_empty() {
            return Ok(verified);
        }

        let fallback = fallback_unverified_streams(expanded, preferred, stalled_hosts);
        if fallback.is_empty() {
            return Err(SenkuError::NoValidStreams {
                episode: episode_num,
            });
        }
        Ok(fallback)
    }

    async fn expand_and_rank_candidates(
        &self,
        candidates: Vec<StreamSource>,
        preferred: Option<Resolution>,
        stalled_hosts: &HashSet<String>,
    ) -> Vec<StreamSource> {
        let mut expanded = tokio::time::timeout(
            Duration::from_millis(2500),
            expand_hls_streams(&self.client, candidates.clone()),
        )
        .await
        .unwrap_or(candidates);
        for candidate in &mut expanded {
            normalize_stream_resolution(candidate);
        }
        rank_candidates_with_penalties(&mut expanded, preferred, stalled_hosts);
        expanded
    }

    async fn probe_candidates(
        &self,
        expanded: Vec<StreamSource>,
        preferred: Option<Resolution>,
        stalled_hosts: &HashSet<String>,
    ) -> Vec<StreamSource> {
        let mut set = JoinSet::new();
        for (idx, candidate) in expanded.into_iter().enumerate() {
            let client = self.client.clone();
            let timeout = self.probe_timeout;
            let outer_cap = timeout + Duration::from_millis(500);
            set.spawn(async move {
                let checked = tokio::time::timeout(
                    outer_cap,
                    verify_stream_head(&client, candidate, timeout),
                )
                .await
                .ok()
                .flatten();
                (idx, checked)
            });
        }

        let mut verified: Vec<StreamSource> = Vec::new();
        let verify_deadline = tokio::time::sleep(OVERALL_VERIFY_DEADLINE);
        tokio::pin!(verify_deadline);
        loop {
            tokio::select! {
                _ = &mut verify_deadline => {
                    set.abort_all();
                    break;
                }
                joined = set.join_next() => {
                    match joined {
                        Some(Ok((_idx, Some(stream)))) => verified.push(stream),
                        Some(_) => {}
                        None => break,
                    }
                }
            }
        }

        // Re-rank with measured throughput_kbps + stalled host penalties + smart CDN tie-breaking.
        rank_candidates_with_penalties(&mut verified, preferred, stalled_hosts);
        verified
    }
}

/// Filters valid `http(s)` candidates, deduplicates URLs, ranks by quality/host, and
/// returns up to 4 candidates marked `verified = false` for direct `[Unverified]` fallback.
pub fn fallback_unverified_streams(
    candidates: Vec<StreamSource>,
    preferred: Option<Resolution>,
    stalled_hosts: &HashSet<String>,
) -> Vec<StreamSource> {
    let mut seen = HashSet::new();
    let mut out: Vec<StreamSource> = candidates
        .into_iter()
        .filter_map(|mut s| {
            let trimmed = s.url.trim().to_string();
            let valid = (trimmed.starts_with("http://") || trimmed.starts_with("https://"))
                && !trimmed.chars().any(char::is_whitespace);
            if !valid || !seen.insert(trimmed.clone()) {
                return None;
            }
            s.url = trimmed;
            normalize_stream_resolution(&mut s);
            s.verified = false;
            Some(s)
        })
        .collect();
    rank_candidates_with_penalties(&mut out, preferred, stalled_hosts);
    out.truncate(4);
    out
}

/// Extracts a normalized upstream host identifier (e.g. `"mp4upload.com"` across
/// subdomains like `s1.mp4upload.com`, or `"127.0.0.1:PORT"` for local mocks).
pub fn stream_host_key(stream: &StreamSource) -> String {
    if let Ok(parsed) = Url::parse(&stream.url) {
        match parsed.host() {
            Some(Host::Domain(domain)) => {
                let lower = domain.to_ascii_lowercase();
                let parts: Vec<&str> = lower.split('.').collect();
                if parts.len() >= 2 {
                    return format!("{}.{}", parts[parts.len() - 2], parts[parts.len() - 1]);
                }
                return lower;
            }
            Some(Host::Ipv4(ip)) => {
                return match parsed.port() {
                    Some(port) => format!("{ip}:{port}"),
                    None => ip.to_string(),
                };
            }
            Some(Host::Ipv6(ip)) => {
                return match parsed.port() {
                    Some(port) => format!("[{ip}]:{port}"),
                    None => ip.to_string(),
                };
            }
            None => {}
        }
    }
    stream
        .server_name
        .split_whitespace()
        .next()
        .unwrap_or(&stream.server_name)
        .to_ascii_lowercase()
}

/// Identifies fast streaming CDNs (`.m3u8` HLS, `vidcache`, `yourupload`, `ok.ru`,
/// `vkvideo`) that typically sustain smooth playback without connection limits.
pub fn is_fast_streaming_cdn(stream: &StreamSource) -> bool {
    let url_l = stream.url.to_ascii_lowercase();
    let srv_l = stream.server_name.to_ascii_lowercase();
    url_l.contains(".m3u8")
        || url_l.contains("vidcache")
        || srv_l.contains("vidcache")
        || url_l.contains("yourupload")
        || srv_l.contains("yourupload")
        || url_l.contains("ok.ru")
        || url_l.contains("okcdn")
        || srv_l.contains("ok.ru")
        || srv_l.contains("okru")
        || srv_l.contains("okanime-embed")
        || url_l.contains("vkvideo")
        || url_l.contains("vk.com")
        || url_l.contains("vk.ru")
        || srv_l.contains("vk")
}

/// Identifies strict rate-limiting filehosts (`mp4upload.com`, etc.) prone to
/// multi-connection tarpitting.
pub fn is_throttled_filehost(stream: &StreamSource) -> bool {
    let url_l = stream.url.to_ascii_lowercase();
    let srv_l = stream.server_name.to_ascii_lowercase();
    url_l.contains("mp4upload") || srv_l.contains("mp4upload")
}

/// Normalizes resolution tags (`FHD`/`Full HD` -> `1080p`, `HD` -> `720p`,
/// `SD` -> `480p`) when explicit numeric resolutions are absent.
pub fn normalize_stream_resolution(stream: &mut StreamSource) {
    if matches!(stream.resolution, Resolution::Unknown | Resolution::Auto) {
        let from_server = Resolution::parse_from_label(&stream.server_name);
        if !matches!(from_server, Resolution::Unknown | Resolution::Auto) {
            stream.resolution = from_server;
            return;
        }
        let from_url = Resolution::parse_from_label(&stream.url);
        if !matches!(from_url, Resolution::Unknown | Resolution::Auto) {
            stream.resolution = from_url;
        }
    }
}

/// Orders candidates highest-resolution first (unless `-q <res>` is set),
/// breaking ties within the same resolution by highest measured throughput.
pub fn rank_candidates(candidates: &mut [StreamSource], preferred: Option<Resolution>) {
    rank_candidates_with_penalties(candidates, preferred, &HashSet::new());
}

/// Ranks verified candidates while deprioritizing previously stalled hosts and
/// preferring healthy fast CDNs that significantly (`>= 2x`) outpace throttled
/// filehosts like `mp4upload.com`.
pub fn rank_candidates_with_penalties(
    candidates: &mut [StreamSource],
    preferred: Option<Resolution>,
    stalled_hosts: &HashSet<String>,
) {
    let fast_cdn_max_kbps = candidates
        .iter()
        .filter(|s| is_fast_streaming_cdn(s) && !stalled_hosts.contains(&stream_host_key(s)))
        .filter_map(|s| s.throughput_kbps)
        .max()
        .unwrap_or(0);

    candidates.sort_by_key(|s| {
        let host = stream_host_key(s);
        let stalled_penalty: u8 = if stalled_hosts.contains(&host) { 1 } else { 0 };
        let throttled_penalty: u8 = if preferred.is_none() && is_throttled_filehost(s) {
            match s.throughput_kbps {
                Some(kbps) if kbps > 0 && fast_cdn_max_kbps >= kbps.saturating_mul(2) => 1,
                _ => 0,
            }
        } else {
            0
        };

        let h = s.resolution.height();
        let pref_score = match preferred {
            Some(target) => {
                if s.resolution == target {
                    0u8
                } else if h > 0 && h < target.height() {
                    1u8
                } else {
                    2u8
                }
            }
            None => 0u8,
        };
        let pref_dist = preferred.map(|t| h.abs_diff(t.height())).unwrap_or(0);
        let cdn_tie_break: u8 = if is_fast_streaming_cdn(s) {
            0
        } else if is_throttled_filehost(s) {
            2
        } else {
            1
        };

        (
            stalled_penalty,
            throttled_penalty,
            pref_score,
            pref_dist,
            Reverse(h),
            Reverse(s.throughput_kbps.unwrap_or(0)),
            cdn_tie_break,
        )
    });
}

fn has_video_or_hls_extension(url: &str) -> bool {
    let lower = url.to_ascii_lowercase();
    let path_only = lower.split(['?', '#']).next().unwrap_or(&lower);
    path_only.ends_with(".mp4")
        || path_only.ends_with(".m3u8")
        || path_only.ends_with(".mkv")
        || path_only.ends_with(".webm")
        || lower.contains(".m3u8")
        || lower.contains(".mp4")
}

fn is_playable_content_type(content_type: Option<&str>, url: &str) -> bool {
    let Some(ct) = content_type else {
        return true;
    };
    let lower = ct.to_ascii_lowercase();
    if lower.contains("text/html") || lower.contains("application/xhtml") {
        return false;
    }
    let is_octet =
        lower.contains("application/octet-stream") || lower.contains("binary/octet-stream");
    if is_octet && has_video_or_hls_extension(url) {
        return true;
    }
    lower.contains("video/")
        || lower.contains("application/mp4")
        || lower.contains("application/octet-stream")
        || lower.contains("binary/octet-stream")
        || lower.contains("mpegurl")
        || lower.contains("binary")
        || has_video_or_hls_extension(url)
}

enum RangeProbeResult {
    Verified(StreamSource),
    Rejected,
    TryHeadFallback,
}

async fn verify_stream_head(
    client: &Client,
    stream: StreamSource,
    timeout: Duration,
) -> Option<StreamSource> {
    match probe_range_and_throughput(client, stream.clone(), timeout).await {
        RangeProbeResult::Verified(verified) => Some(verified),
        RangeProbeResult::Rejected => None,
        RangeProbeResult::TryHeadFallback => head_verify_fallback(client, stream, timeout).await,
    }
}

async fn head_verify_fallback(
    client: &Client,
    mut stream: StreamSource,
    timeout: Duration,
) -> Option<StreamSource> {
    let budget = timeout.min(THROUGHPUT_PROBE_TIMEOUT);
    let mut head_req = client.head(&stream.url);
    if let Some(ref ref_url) = stream.referer {
        head_req = head_req.header(REFERER, ref_url);
    }

    let resp = tokio::time::timeout(budget, head_req.send())
        .await
        .ok()?
        .ok()?;
    let status = resp.status();
    if !status.is_success() && status != StatusCode::PARTIAL_CONTENT {
        return None;
    }
    let ct = resp
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok());
    if !is_playable_content_type(ct, &stream.url) {
        return None;
    }
    stream.content_length = resp
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .or(stream.content_length);
    if resp
        .headers()
        .get(ACCEPT_RANGES)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("bytes"))
    {
        stream.supports_ranges = true;
    }
    stream.verified = true;
    Some(stream)
}

/// Separates stream reachability (valid HTTP `200`/`206` + playable content-type)
/// from throughput measurement: once headers succeed, incremental partial reads
/// keep the stream verified even if the full `256 KiB` sample hits the deadline.
async fn probe_range_and_throughput(
    client: &Client,
    mut stream: StreamSource,
    timeout: Duration,
) -> RangeProbeResult {
    let probe_budget = timeout.min(THROUGHPUT_PROBE_TIMEOUT);
    let started = Instant::now();

    let mut req = client
        .get(&stream.url)
        .header(RANGE, format!("bytes=0-{}", PROBE_SAMPLE_BYTES - 1));
    if let Some(ref ref_url) = stream.referer {
        req = req.header(REFERER, ref_url);
    }

    let resp = match tokio::time::timeout(probe_budget, req.send()).await {
        Ok(Ok(r)) => r,
        _ => return RangeProbeResult::TryHeadFallback,
    };

    let status = resp.status();
    if status == StatusCode::METHOD_NOT_ALLOWED
        || status == StatusCode::RANGE_NOT_SATISFIABLE
        || status == StatusCode::NOT_IMPLEMENTED
    {
        return RangeProbeResult::TryHeadFallback;
    }
    if !status.is_success() && status != StatusCode::PARTIAL_CONTENT {
        return RangeProbeResult::Rejected;
    }

    let ct = resp
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok());
    if !is_playable_content_type(ct, &stream.url) {
        return RangeProbeResult::Rejected;
    }

    let accept_ranges_bytes = resp
        .headers()
        .get(ACCEPT_RANGES)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("bytes"));
    let has_content_range = resp.headers().get(CONTENT_RANGE).is_some();

    if status == StatusCode::PARTIAL_CONTENT || accept_ranges_bytes || has_content_range {
        stream.supports_ranges = true;
    }

    stream.content_length = resp
        .headers()
        .get(CONTENT_RANGE)
        .and_then(|v| v.to_str().ok())
        .and_then(|cr| cr.rsplit('/').next())
        .and_then(|total| total.parse::<u64>().ok())
        .or_else(|| {
            resp.headers()
                .get(CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
        })
        .or(stream.content_length);

    let mut bytes_read = 0usize;
    let remaining_budget = probe_budget.saturating_sub(started.elapsed());
    if !remaining_budget.is_zero() {
        let deadline = tokio::time::sleep(remaining_budget);
        tokio::pin!(deadline);
        let mut byte_stream = resp.bytes_stream();
        loop {
            tokio::select! {
                _ = &mut deadline => break,
                chunk = byte_stream.next() => {
                    match chunk {
                        Some(Ok(bytes)) => {
                            bytes_read += bytes.len();
                            if bytes_read >= PROBE_SAMPLE_BYTES {
                                break;
                            }
                        }
                        _ => break,
                    }
                }
            }
        }
    }

    if bytes_read > 0 {
        let elapsed = started.elapsed().as_secs_f64().max(0.001);
        let kbps = ((bytes_read as f64 / 1024.0) / elapsed).round().max(1.0) as u64;
        stream.throughput_kbps = Some(kbps);
    }

    stream.verified = true;
    RangeProbeResult::Verified(stream)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::ProviderId;
    use crate::providers::build_http_client;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    async fn spawn_head_test_server() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = [0u8; 1024];
                    let n = sock.read(&mut buf).await.unwrap_or(0);
                    let req = String::from_utf8_lossy(&buf[..n]);
                    let first_line = req.lines().next().unwrap_or("");
                    let resp = if first_line.contains("/dead-1080p.mp4") {
                        "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    } else if first_line.contains("/forbidden-1080p.mp4") {
                        "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    } else if first_line.contains("/err500-1080p.mp4") {
                        "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    } else if first_line.contains("/live-1080p.mp4") {
                        "HTTP/1.1 200 OK\r\nContent-Type: video/mp4\r\nContent-Length: 1048576\r\nConnection: close\r\n\r\n"
                    } else {
                        "HTTP/1.1 200 OK\r\nContent-Type: video/mp4\r\nContent-Length: 524288\r\nConnection: close\r\n\r\n"
                    };
                    let _ = sock.write_all(resp.as_bytes()).await;
                });
            }
        });
        format!("http://{addr}")
    }

    async fn spawn_speed_test_server(delay: Duration) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let chunk = vec![0xAAu8; PROBE_SAMPLE_BYTES];
            while let Ok((mut sock, _)) = listener.accept().await {
                let data = chunk.clone();
                tokio::spawn(async move {
                    let mut buf = [0u8; 1024];
                    let n = sock.read(&mut buf).await.unwrap_or(0);
                    let req = String::from_utf8_lossy(&buf[..n]);
                    let is_head = req.starts_with("HEAD ");
                    if !is_head && !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                    if is_head {
                        let hdr = "HTTP/1.1 200 OK\r\nAccept-Ranges: bytes\r\nContent-Type: video/mp4\r\nContent-Length: 2097152\r\nConnection: close\r\n\r\n";
                        let _ = sock.write_all(hdr.as_bytes()).await;
                    } else {
                        let hdr = format!(
                            "HTTP/1.1 206 Partial Content\r\nAccept-Ranges: bytes\r\nContent-Range: bytes 0-{}/2097152\r\nContent-Type: video/mp4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            data.len() - 1,
                            data.len()
                        );
                        let _ = sock.write_all(hdr.as_bytes()).await;
                        let _ = sock.write_all(&data).await;
                    }
                });
            }
        });
        format!("http://{addr}/stream.mp4")
    }

    #[tokio::test]
    async fn defaults_to_highest_verified_resolution_and_respects_override() {
        let base = spawn_head_test_server().await;
        let client = build_http_client(Duration::from_secs(3)).unwrap();
        let resolver = StreamResolver::new(client);

        let candidates = vec![
            StreamSource {
                url: format!("{base}/live-480p.mp4"),
                resolution: Resolution::Sd480,
                provider: ProviderId::Anime3rb,
                server_name: "s1".into(),
                referer: None,
                content_length: None,
                verified: false,
                throughput_kbps: None,
                supports_ranges: false,
            },
            StreamSource {
                url: format!("{base}/dead-1080p.mp4"),
                resolution: Resolution::Fhd1080,
                provider: ProviderId::Anime3rb,
                server_name: "dead".into(),
                referer: None,
                content_length: None,
                verified: false,
                throughput_kbps: None,
                supports_ranges: false,
            },
            StreamSource {
                url: format!("{base}/live-720p.mp4"),
                resolution: Resolution::Hd720,
                provider: ProviderId::Anime3rb,
                server_name: "s2".into(),
                referer: None,
                content_length: None,
                verified: false,
                throughput_kbps: None,
                supports_ranges: false,
            },
            StreamSource {
                url: format!("{base}/live-1080p.mp4"),
                resolution: Resolution::Fhd1080,
                provider: ProviderId::Anime3rb,
                server_name: "s3".into(),
                referer: None,
                content_length: None,
                verified: false,
                throughput_kbps: None,
                supports_ranges: false,
            },
        ];

        let best = resolver
            .resolve_best(1, candidates.clone(), None)
            .await
            .unwrap();
        assert_eq!(best.resolution, Resolution::Fhd1080);
        assert!(best.url.ends_with("/live-1080p.mp4"));
        assert!(best.verified);
        assert_eq!(best.content_length, Some(1048576));

        let q720 = resolver
            .resolve_best(1, candidates, Some(Resolution::Hd720))
            .await
            .unwrap();
        assert_eq!(q720.resolution, Resolution::Hd720);
        assert!(q720.url.ends_with("/live-720p.mp4"));
    }

    #[tokio::test]
    async fn fails_over_to_second_verified_candidate_when_first_returns_403_or_500() {
        let base = spawn_head_test_server().await;
        let client = build_http_client(Duration::from_secs(3)).unwrap();
        let resolver = StreamResolver::new(client);

        let candidates = vec![
            StreamSource {
                url: format!("{base}/forbidden-1080p.mp4"),
                resolution: Resolution::Fhd1080,
                provider: ProviderId::OkAnime,
                server_name: "broken-403".into(),
                referer: None,
                content_length: None,
                verified: false,
                throughput_kbps: None,
                supports_ranges: false,
            },
            StreamSource {
                url: format!("{base}/err500-1080p.mp4"),
                resolution: Resolution::Fhd1080,
                provider: ProviderId::OkAnime,
                server_name: "broken-500".into(),
                referer: None,
                content_length: None,
                verified: false,
                throughput_kbps: None,
                supports_ranges: false,
            },
            StreamSource {
                url: format!("{base}/live-720p.mp4"),
                resolution: Resolution::Hd720,
                provider: ProviderId::OkAnime,
                server_name: "healthy-backup".into(),
                referer: None,
                content_length: None,
                verified: false,
                throughput_kbps: None,
                supports_ranges: false,
            },
        ];

        let ranked = resolver
            .resolve_ranked_verified(1, candidates, None)
            .await
            .unwrap();
        assert_eq!(ranked.len(), 1);
        assert_eq!(ranked[0].server_name, "healthy-backup");
        assert!(ranked[0].url.ends_with("/live-720p.mp4"));
    }

    #[tokio::test]
    async fn normalizes_fhd_hd_sd_tags_and_ranks_mp4upload_hd_as_720p() {
        let base = spawn_head_test_server().await;
        let client = build_http_client(Duration::from_secs(3)).unwrap();
        let resolver = StreamResolver::new(client);

        let candidates = vec![
            StreamSource {
                url: format!("{base}/stream-a.mp4"),
                resolution: Resolution::Unknown,
                provider: ProviderId::OkAnime,
                server_name: "mirror SD".into(),
                referer: None,
                content_length: None,
                verified: false,
                throughput_kbps: None,
                supports_ranges: false,
            },
            StreamSource {
                url: format!("{base}/stream-b.mp4"),
                resolution: Resolution::Unknown,
                provider: ProviderId::OkAnime,
                server_name: "mp4upload HD".into(),
                referer: None,
                content_length: None,
                verified: false,
                throughput_kbps: None,
                supports_ranges: false,
            },
        ];

        let best = resolver.resolve_best(1, candidates, None).await.unwrap();
        assert_eq!(best.resolution, Resolution::Hd720);
        assert_eq!(best.server_name, "mp4upload HD");
    }

    #[tokio::test]
    async fn probes_throughput_detects_ranges_and_ranks_fastest_same_resolution_first() {
        let slow_url = spawn_speed_test_server(Duration::from_millis(220)).await;
        let fast_url = spawn_speed_test_server(Duration::ZERO).await;

        let client = build_http_client(Duration::from_secs(5)).unwrap();
        let resolver = StreamResolver::new(client);

        let candidates = vec![
            StreamSource {
                url: slow_url,
                resolution: Resolution::Fhd1080,
                provider: ProviderId::Animerco,
                server_name: "slow-1080p".into(),
                referer: None,
                content_length: None,
                verified: false,
                throughput_kbps: None,
                supports_ranges: false,
            },
            StreamSource {
                url: fast_url,
                resolution: Resolution::Fhd1080,
                provider: ProviderId::Animerco,
                server_name: "fast-1080p".into(),
                referer: None,
                content_length: None,
                verified: false,
                throughput_kbps: None,
                supports_ranges: false,
            },
        ];

        let ranked = resolver
            .resolve_ranked_verified(1, candidates, None)
            .await
            .unwrap();
        assert_eq!(ranked.len(), 2);
        assert_eq!(ranked[0].server_name, "fast-1080p");
        assert!(ranked[0].supports_ranges);
        assert!(ranked[0].throughput_kbps.unwrap() > ranked[1].throughput_kbps.unwrap());
    }

    #[test]
    fn smart_host_tie_breaking_prefers_fast_cdn_and_penalizes_stalled_hosts() {
        let mut candidates = vec![
            StreamSource {
                url: "https://s1.mp4upload.com:282/d/ep1-1080.mp4".into(),
                resolution: Resolution::Fhd1080,
                provider: ProviderId::OkAnime,
                server_name: "mp4upload FHD".into(),
                referer: None,
                content_length: Some(200_000_000),
                verified: true,
                throughput_kbps: Some(450),
                supports_ranges: true,
            },
            StreamSource {
                url: "https://cdn.vidcache.net/hls/ep1-720p.m3u8".into(),
                resolution: Resolution::Hd720,
                provider: ProviderId::OkAnime,
                server_name: "vidcache HD".into(),
                referer: None,
                content_length: None,
                verified: true,
                throughput_kbps: Some(2400),
                supports_ranges: false,
            },
        ];

        rank_candidates(&mut candidates, None);
        assert_eq!(
            candidates[0].server_name, "vidcache HD",
            "fast CDN with >=2x throughput should beat throttled mp4upload.com in Auto-Best"
        );

        // Even if mp4upload had no measured throughput yet, a stalled-host penalty in the
        // session must put mp4upload below all healthy mirrors even with -q 1080p.
        candidates[0].throughput_kbps = None;
        candidates[1].throughput_kbps = None;
        let mut stalled = HashSet::new();
        stalled.insert(stream_host_key(&candidates[1]));
        assert_eq!(stream_host_key(&candidates[1]), "mp4upload.com");

        rank_candidates_with_penalties(&mut candidates, Some(Resolution::Fhd1080), &stalled);
        assert_eq!(candidates[0].server_name, "vidcache HD");
    }

    #[tokio::test]
    async fn partial_read_probe_retains_slow_206_1080p_instead_of_dropping_to_480p() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = [0u8; 1024];
                    let n = sock.read(&mut buf).await.unwrap_or(0);
                    let req = String::from_utf8_lossy(&buf[..n]);
                    let first_line = req.lines().next().unwrap_or("");
                    if first_line.contains("/slow-1080p.mp4") {
                        // Send 206 headers + 32 KiB immediately, then stall past the probe deadline.
                        let hdr = "HTTP/1.1 206 Partial Content\r\nAccept-Ranges: bytes\r\nContent-Range: bytes 0-262143/41943040\r\nContent-Type: video/mp4\r\nContent-Length: 262144\r\nConnection: close\r\n\r\n";
                        let _ = sock.write_all(hdr.as_bytes()).await;
                        let partial = vec![0x55u8; 32 * 1024];
                        let _ = sock.write_all(&partial).await;
                        tokio::time::sleep(Duration::from_secs(3)).await;
                    } else if first_line.contains("/headers-only-720p.mp4") {
                        // Send 200 OK headers, then trickle nothing before probe deadline.
                        let hdr = "HTTP/1.1 200 OK\r\nContent-Type: video/mp4\r\nContent-Length: 20971520\r\nConnection: close\r\n\r\n";
                        let _ = sock.write_all(hdr.as_bytes()).await;
                        tokio::time::sleep(Duration::from_secs(3)).await;
                    } else {
                        let data = vec![0x33u8; PROBE_SAMPLE_BYTES];
                        let hdr = format!(
                            "HTTP/1.1 206 Partial Content\r\nAccept-Ranges: bytes\r\nContent-Range: bytes 0-{}/10485760\r\nContent-Type: video/mp4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            data.len() - 1,
                            data.len()
                        );
                        let _ = sock.write_all(hdr.as_bytes()).await;
                        let _ = sock.write_all(&data).await;
                    }
                });
            }
        });

        let base = format!("http://{addr}");
        let client = build_http_client(Duration::from_secs(5)).unwrap();
        let resolver = StreamResolver::with_timeout(client, Duration::from_millis(350));

        let candidates = vec![
            StreamSource {
                url: format!("{base}/fast-480p.mp4"),
                resolution: Resolution::Sd480,
                provider: ProviderId::OkAnime,
                server_name: "mirror SD".into(),
                referer: None,
                content_length: None,
                verified: false,
                throughput_kbps: None,
                supports_ranges: false,
            },
            StreamSource {
                url: format!("{base}/headers-only-720p.mp4"),
                resolution: Resolution::Hd720,
                provider: ProviderId::Animerco,
                server_name: "vidcache HD".into(),
                referer: None,
                content_length: None,
                verified: false,
                throughput_kbps: None,
                supports_ranges: false,
            },
            StreamSource {
                url: format!("{base}/slow-1080p.mp4"),
                resolution: Resolution::Fhd1080,
                provider: ProviderId::OkAnime,
                server_name: "yourupload FHD".into(),
                referer: None,
                content_length: None,
                verified: false,
                throughput_kbps: None,
                supports_ranges: false,
            },
        ];

        let ranked = resolver
            .resolve_ranked_verified(6, candidates, None)
            .await
            .unwrap();
        assert_eq!(
            ranked.len(),
            3,
            "slow 1080p & 720p must stay verified, got {ranked:#?}"
        );
        assert_eq!(ranked[0].resolution, Resolution::Fhd1080);
        assert!(ranked[0].verified);
        assert!(ranked[0].supports_ranges);
        assert!(ranked[0].throughput_kbps.is_some());
        assert_eq!(ranked[1].resolution, Resolution::Hd720);
        assert!(ranked[1].verified);
        assert_eq!(ranked[1].throughput_kbps, None);
    }

    #[tokio::test]
    async fn accepts_octet_stream_for_media_ext_and_retains_unverified_fallback_when_probes_fail() {
        assert!(is_playable_content_type(
            Some("application/octet-stream"),
            "https://archive.example/download/hxh-130.mp4?token=abc"
        ));
        assert!(is_playable_content_type(
            Some("binary/octet-stream"),
            "https://cdn.example/master/ep130.m3u8"
        ));
        assert!(!is_playable_content_type(
            Some("text/html; charset=utf-8"),
            "https://cdn.example/ep130.mp4"
        ));

        // Server returns 403/500 for ALL candidates -> strict fails, regular ranked retains top 4 as Unverified!
        let base = spawn_head_test_server().await;
        let client = build_http_client(Duration::from_secs(3)).unwrap();
        let resolver = StreamResolver::new(client);

        let candidates = vec![
            StreamSource {
                url: format!("{base}/forbidden-1080p.mp4"),
                resolution: Resolution::Fhd1080,
                provider: ProviderId::OkAnime,
                server_name: "mirror-1".into(),
                referer: None,
                content_length: None,
                verified: false,
                throughput_kbps: None,
                supports_ranges: false,
            },
            StreamSource {
                url: format!("{base}/err500-1080p.mp4"),
                resolution: Resolution::Hd720,
                provider: ProviderId::Animerco,
                server_name: "mirror-2".into(),
                referer: None,
                content_length: None,
                verified: false,
                throughput_kbps: None,
                supports_ranges: false,
            },
            StreamSource {
                url: format!("{base}/dead-1080p.mp4"),
                resolution: Resolution::Sd480,
                provider: ProviderId::OkAnime,
                server_name: "mirror-3".into(),
                referer: None,
                content_length: None,
                verified: false,
                throughput_kbps: None,
                supports_ranges: false,
            },
            StreamSource {
                url: format!("{base}/dead-1080p.mp4"), // duplicate URL should be deduped
                resolution: Resolution::Sd480,
                provider: ProviderId::OkAnime,
                server_name: "mirror-dup".into(),
                referer: None,
                content_length: None,
                verified: false,
                throughput_kbps: None,
                supports_ranges: false,
            },
            StreamSource {
                url: "not-a-valid-url".into(),
                resolution: Resolution::Sd360,
                provider: ProviderId::OkAnime,
                server_name: "invalid".into(),
                referer: None,
                content_length: None,
                verified: false,
                throughput_kbps: None,
                supports_ranges: false,
            },
            StreamSource {
                url: format!("{base}/forbidden-1080p.mp4?m=4"),
                resolution: Resolution::Sd360,
                provider: ProviderId::OkAnime,
                server_name: "mirror-4".into(),
                referer: None,
                content_length: None,
                verified: false,
                throughput_kbps: None,
                supports_ranges: false,
            },
            StreamSource {
                url: format!("{base}/forbidden-1080p.mp4?m=5"),
                resolution: Resolution::Sd360,
                provider: ProviderId::OkAnime,
                server_name: "mirror-5".into(),
                referer: None,
                content_length: None,
                verified: false,
                throughput_kbps: None,
                supports_ranges: false,
            },
        ];

        assert!(resolver
            .resolve_strict_verified(130, candidates.clone(), None)
            .await
            .is_err());

        let fallback = resolver
            .resolve_ranked_verified(130, candidates, None)
            .await
            .unwrap();
        assert_eq!(
            fallback.len(),
            4,
            "must keep up to 4 unique valid http(s) URLs"
        );
        assert!(fallback.iter().all(|s| !s.verified));
        assert_eq!(fallback[0].resolution, Resolution::Fhd1080);
    }
}
