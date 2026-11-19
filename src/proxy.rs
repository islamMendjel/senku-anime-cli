use crate::error::Result;
use crate::models::StreamSource;
use crate::providers::common::CHROME_USER_AGENT;
use futures::StreamExt;
use reqwest::header::{RANGE, REFERER, USER_AGENT};
use reqwest::{Client, StatusCode};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, Mutex};
use tokio::task::{JoinHandle, JoinSet};

pub const PROXY_CHUNK_SIZE: u64 = 512 * 1024; // 512 KiB
pub const MAX_AHEAD_BYTES: usize = 8 * 1024 * 1024; // 8 MiB lean lookahead bound
pub const PROXY_MAX_WINDOW_CHUNKS: usize = MAX_AHEAD_BYTES / (PROXY_CHUNK_SIZE as usize); // 16 chunks
const CHUNK_TIMEOUT: Duration = Duration::from_secs(6);
const CHUNK_RETRIES: usize = 4;

/// Shared lock-free + adaptive concurrency control state for a running proxy.
#[derive(Debug)]
pub struct ProxyStats {
    configured_workers: usize,
    active_limit: AtomicUsize,
    in_flight_workers: AtomicUsize,
    throughput_bps: Arc<AtomicU64>,
    window_bytes: AtomicU64,
    window_started_ms: AtomicU64,
    last_progress_ms: AtomicU64,
    epoch: Instant,
    serial_gate: Mutex<()>,
}

impl ProxyStats {
    pub fn new(workers: usize) -> Self {
        let workers = workers.clamp(1, 16);
        Self {
            configured_workers: workers,
            active_limit: AtomicUsize::new(workers),
            in_flight_workers: AtomicUsize::new(0),
            throughput_bps: Arc::new(AtomicU64::new(0)),
            window_bytes: AtomicU64::new(0),
            window_started_ms: AtomicU64::new(0),
            last_progress_ms: AtomicU64::new(0),
            epoch: Instant::now(),
            serial_gate: Mutex::new(()),
        }
    }

    pub fn configured_workers(&self) -> usize {
        self.configured_workers
    }

    pub fn active_workers(&self) -> usize {
        self.active_limit.load(Ordering::Relaxed)
    }

    /// Number of upstream chunk-fetcher workers currently executing (drops to 0 on abort/disconnect).
    pub fn in_flight_workers(&self) -> usize {
        self.in_flight_workers.load(Ordering::Relaxed)
    }

    /// Steps active concurrency down from N -> 1 immediately when an upstream
    /// filehost returns HTTP 429/503/403 or times out.
    pub fn step_down_to_single(&self) {
        self.active_limit.store(1, Ordering::Relaxed);
    }

    pub fn throughput_counter(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.throughput_bps)
    }

    /// Current smoothed throughput in bytes/sec (decays to `0` if stalled > 4s).
    pub fn current_throughput_bps(&self) -> u64 {
        let last_ms = self.last_progress_ms.load(Ordering::Relaxed);
        let now_ms = self.epoch.elapsed().as_millis() as u64;
        if last_ms > 0 && now_ms.saturating_sub(last_ms) > 4000 {
            return 0;
        }
        self.throughput_bps.load(Ordering::Relaxed)
    }

    fn record_bytes(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let now_ms = self.epoch.elapsed().as_millis() as u64;
        self.last_progress_ms.store(now_ms, Ordering::Relaxed);
        let total_window = self.window_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
        let started_ms = self.window_started_ms.load(Ordering::Relaxed);
        let elapsed_ms = now_ms.saturating_sub(started_ms);
        if elapsed_ms >= 400
            && self
                .window_started_ms
                .compare_exchange(started_ms, now_ms, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            self.window_bytes.store(0, Ordering::Relaxed);
            let instant_bps = total_window.saturating_mul(1000) / elapsed_ms.max(1);
            let prev = self.throughput_bps.load(Ordering::Relaxed);
            let smoothed = if prev == 0 {
                instant_bps
            } else {
                (prev.saturating_mul(3) + instant_bps.saturating_mul(5)) / 8
            };
            self.throughput_bps.store(smoothed, Ordering::Relaxed);
        } else if self.throughput_bps.load(Ordering::Relaxed) == 0 && elapsed_ms >= 50 {
            let instant_bps = total_window.saturating_mul(1000) / elapsed_ms.max(1);
            self.throughput_bps.store(instant_bps, Ordering::Relaxed);
        }
    }
}

/// Handle to a running local `TurboStreamProxy`. Shuts down the listener and
/// any active connections cleanly on drop or explicit `shutdown()`.
#[derive(Debug)]
pub struct ProxyHandle {
    local_url: String,
    stats: Arc<ProxyStats>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl ProxyHandle {
    pub fn local_url(&self) -> &str {
        &self.local_url
    }

    pub fn workers(&self) -> usize {
        self.stats.active_workers()
    }

    pub fn throughput_bps(&self) -> u64 {
        self.stats.current_throughput_bps()
    }

    pub fn stats(&self) -> Arc<ProxyStats> {
        Arc::clone(&self.stats)
    }

    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.task.take() {
            handle.abort();
            let _ = handle.await;
        }
    }
}

impl Drop for ProxyHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.task.take() {
            handle.abort();
        }
    }
}

/// Pure-Tokio + `reqwest` multi-threaded HTTP Range sliding-window accelerator
/// bound to `127.0.0.1:0` for direct progressive streams.
pub struct TurboStreamProxy;

impl TurboStreamProxy {
    /// Returns `true` for strict single-socket filehosts (such as `mp4upload.com`
    /// and its storage subdomains) that tarpit/throttle parallel Range splitting.
    pub fn is_strict_single_socket_host(stream: &StreamSource) -> bool {
        if let Ok(parsed) = url::Url::parse(&stream.url) {
            if let Some(host) = parsed.host_str() {
                let lower = host.to_ascii_lowercase();
                if lower == "mp4upload.com" || lower.ends_with(".mp4upload.com") {
                    return true;
                }
            }
        }
        stream.url.to_ascii_lowercase().contains("mp4upload.com")
    }

    /// Returns `true` for progressive streams (non-HLS, non-strict-filehost)
    /// whose upstream supports byte ranges and reports a non-zero `Content-Length`.
    pub fn is_eligible(stream: &StreamSource) -> bool {
        !stream.url.to_ascii_lowercase().contains(".m3u8")
            && !Self::is_strict_single_socket_host(stream)
            && stream.supports_ranges
            && stream.content_length.is_some_and(|len| len > 0)
    }

    /// Starts the local proxy bound to `127.0.0.1:0` and returns a `ProxyHandle`
    /// exposing `http://127.0.0.1:<port>/stream`.
    pub async fn start(
        client: Client,
        stream: StreamSource,
        workers: usize,
    ) -> Result<ProxyHandle> {
        let total_len = stream.content_length.unwrap_or(0);
        let stats = Arc::new(ProxyStats::new(workers));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr: SocketAddr = listener.local_addr()?;
        let local_url = format!("http://{addr}/stream");

        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
        let upstream = Arc::new(stream);
        let stats_for_task = Arc::clone(&stats);

        let task = tokio::spawn(async move {
            let mut active_conns = JoinSet::new();
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => {
                        active_conns.abort_all();
                        break;
                    }
                    accepted = listener.accept() => {
                        match accepted {
                            Ok((sock, _)) => {
                                let _ = sock.set_nodelay(true);
                                let c = client.clone();
                                let up = Arc::clone(&upstream);
                                let st = Arc::clone(&stats_for_task);
                                active_conns.spawn(async move {
                                    let _ = handle_connection(sock, c, up, total_len, st).await;
                                });
                            }
                            Err(_) => break,
                        }
                    }
                    // Reap finished connection tasks so JoinSet never accumulates metadata.
                    _ = active_conns.join_next(), if !active_conns.is_empty() => {}
                }
            }
        });

        Ok(ProxyHandle {
            local_url,
            stats,
            shutdown_tx: Some(shutdown_tx),
            task: Some(task),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HttpMethod {
    Head,
    Get,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ParsedRequest {
    method: HttpMethod,
    range: Option<(u64, Option<u64>)>,
}

fn parse_http_request(raw: &str) -> Option<ParsedRequest> {
    let mut lines = raw.lines();
    let request_line = lines.next()?.trim();
    let method_str = request_line.split_whitespace().next()?;
    let method = match method_str {
        "HEAD" => HttpMethod::Head,
        "GET" => HttpMethod::Get,
        _ => return None,
    };

    let mut range = None;
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            if k.trim().eq_ignore_ascii_case("range") {
                if let Some(spec) = v.trim().strip_prefix("bytes=") {
                    let first_range = spec.split(',').next().unwrap_or("").trim();
                    if let Some((s_str, e_str)) = first_range.split_once('-') {
                        if let Ok(start) = s_str.trim().parse::<u64>() {
                            let end = if e_str.trim().is_empty() {
                                None
                            } else {
                                e_str.trim().parse::<u64>().ok()
                            };
                            range = Some((start, end));
                        }
                    }
                }
            }
        }
    }

    Some(ParsedRequest { method, range })
}

async fn handle_connection(
    mut sock: TcpStream,
    client: Client,
    upstream: Arc<StreamSource>,
    total_len: u64,
    stats: Arc<ProxyStats>,
) -> Result<()> {
    let mut req_buf = Vec::with_capacity(2048);
    let mut tmp = [0u8; 1024];
    loop {
        let n = sock.read(&mut tmp).await?;
        if n == 0 {
            return Ok(());
        }
        req_buf.extend_from_slice(&tmp[..n]);
        if req_buf.windows(4).any(|w| w == b"\r\n\r\n") || req_buf.len() >= 16_384 {
            break;
        }
    }

    let req_text = String::from_utf8_lossy(&req_buf);
    let Some(req) = parse_http_request(&req_text) else {
        let resp =
            "HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        sock.write_all(resp.as_bytes()).await?;
        return Ok(());
    };

    if total_len == 0 {
        let resp = "HTTP/1.1 200 OK\r\nAccept-Ranges: bytes\r\nContent-Type: video/mp4\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        sock.write_all(resp.as_bytes()).await?;
        return Ok(());
    }

    let (start, end, is_partial) = match req.range {
        Some((s, maybe_e)) => {
            if s >= total_len {
                let resp = format!(
                    "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Range: bytes */{total_len}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                );
                sock.write_all(resp.as_bytes()).await?;
                return Ok(());
            }
            let e = maybe_e.unwrap_or(total_len - 1).min(total_len - 1);
            if e < s {
                let resp = format!(
                    "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Range: bytes */{total_len}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                );
                sock.write_all(resp.as_bytes()).await?;
                return Ok(());
            }
            (s, e, true)
        }
        None => (0, total_len - 1, false),
    };

    let resp_len = (end - start) + 1;
    let headers = if is_partial {
        format!(
            "HTTP/1.1 206 Partial Content\r\nAccept-Ranges: bytes\r\nContent-Type: video/mp4\r\nContent-Range: bytes {start}-{end}/{total_len}\r\nContent-Length: {resp_len}\r\nConnection: close\r\n\r\n"
        )
    } else {
        format!(
            "HTTP/1.1 200 OK\r\nAccept-Ranges: bytes\r\nContent-Type: video/mp4\r\nContent-Length: {resp_len}\r\nConnection: close\r\n\r\n"
        )
    };

    sock.write_all(headers.as_bytes()).await?;
    sock.flush().await?;

    if req.method == HttpMethod::Head {
        return Ok(());
    }

    stream_sliding_window(&mut sock, &client, &upstream, start, end, stats).await
}

async fn stream_sliding_window(
    sock: &mut TcpStream,
    client: &Client,
    upstream: &StreamSource,
    range_start: u64,
    range_end: u64,
    stats: Arc<ProxyStats>,
) -> Result<()> {
    let total_bytes = (range_end - range_start) + 1;
    let total_chunks = total_bytes.div_ceil(PROXY_CHUNK_SIZE) as usize;
    let mut next_to_spawn = 0usize;
    let mut next_to_send = 0usize;
    let mut buffered: BTreeMap<usize, Vec<u8>> = BTreeMap::new();
    let mut set: JoinSet<(usize, std::result::Result<Vec<u8>, String>)> = JoinSet::new();

    loop {
        let concurrency_cap = stats.active_workers();
        while set.len() < concurrency_cap
            && next_to_spawn < total_chunks
            && (next_to_spawn - next_to_send) < PROXY_MAX_WINDOW_CHUNKS
        {
            let chunk_idx = next_to_spawn;
            next_to_spawn += 1;

            let c_start = range_start + (chunk_idx as u64) * PROXY_CHUNK_SIZE;
            let c_end = (c_start + PROXY_CHUNK_SIZE - 1).min(range_end);
            let client_ref = client.clone();
            let url = upstream.url.clone();
            let referer = upstream.referer.clone();
            let stats_ref = Arc::clone(&stats);

            set.spawn(async move {
                let res = fetch_chunk_bytes(
                    &client_ref,
                    &url,
                    referer.as_deref(),
                    c_start,
                    c_end,
                    &stats_ref,
                )
                .await;
                (chunk_idx, res)
            });
        }

        if next_to_send >= total_chunks {
            break;
        }

        let outcome = tokio::select! {
            ready_err = sock.ready(tokio::io::Interest::ERROR) => {
                if ready_err.is_ok() {
                    set.abort_all();
                    return Ok(());
                }
                continue;
            }
            joined = set.join_next() => joined,
        };
        match outcome {
            Some(Ok((idx, Ok(bytes)))) => {
                stats.record_bytes(bytes.len() as u64);
                buffered.insert(idx, bytes);
                while let Some(chunk) = buffered.remove(&next_to_send) {
                    if sock.write_all(&chunk).await.is_err() {
                        // Reader closed / sought: immediately abort all in-flight workers.
                        set.abort_all();
                        return Ok(());
                    }
                    next_to_send += 1;
                }
            }
            Some(Ok((_idx, Err(_err)))) => {
                set.abort_all();
                return Ok(());
            }
            Some(Err(_panic)) => {
                set.abort_all();
                return Ok(());
            }
            None => break,
        }
    }

    let _ = sock.flush().await;
    Ok(())
}

async fn fetch_chunk_once(
    client: &Client,
    url: &str,
    referer: Option<&str>,
    start: u64,
    end: u64,
    expected: usize,
) -> std::result::Result<Vec<u8>, Option<StatusCode>> {
    let range_hdr = format!("bytes={start}-{end}");
    let mut req = client
        .get(url)
        .header(RANGE, &range_hdr)
        .header(USER_AGENT, CHROME_USER_AGENT)
        .timeout(CHUNK_TIMEOUT);
    if let Some(r) = referer {
        req = req.header(REFERER, r);
    }

    let resp = req.send().await.map_err(|_| None)?;
    let status = resp.status();
    if status == StatusCode::TOO_MANY_REQUESTS
        || status == StatusCode::SERVICE_UNAVAILABLE
        || status == StatusCode::FORBIDDEN
    {
        return Err(Some(status));
    }

    if !(status == StatusCode::PARTIAL_CONTENT || (start == 0 && status.is_success())) {
        return Err(Some(status));
    }

    let mut out = Vec::with_capacity(expected);
    let mut byte_stream = resp.bytes_stream();
    while let Some(piece) = byte_stream.next().await {
        match piece {
            Ok(bytes) => {
                out.extend_from_slice(&bytes);
                if out.len() >= expected {
                    out.truncate(expected);
                    return Ok(out);
                }
            }
            Err(_) => return Err(None),
        }
    }

    if out.len() >= expected {
        out.truncate(expected);
        Ok(out)
    } else {
        Err(None)
    }
}

struct InFlightGuard<'a>(&'a ProxyStats);

impl<'a> InFlightGuard<'a> {
    fn acquire(stats: &'a ProxyStats) -> Self {
        stats.in_flight_workers.fetch_add(1, Ordering::Relaxed);
        Self(stats)
    }
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.0.in_flight_workers.fetch_sub(1, Ordering::Relaxed);
    }
}

async fn fetch_chunk_bytes(
    client: &Client,
    url: &str,
    referer: Option<&str>,
    start: u64,
    end: u64,
    stats: &ProxyStats,
) -> std::result::Result<Vec<u8>, String> {
    let _in_flight = InFlightGuard::acquire(stats);
    let expected = ((end - start) + 1) as usize;

    for attempt in 1..=CHUNK_RETRIES {
        // When stepped down to 1 worker, serialize upstream HTTP requests across
        // any remaining in-flight workers so strict filehosts only ever see 1 connection.
        let _serial_guard = if stats.active_workers() <= 1 {
            Some(stats.serial_gate.lock().await)
        } else {
            None
        };

        let result = tokio::time::timeout(
            CHUNK_TIMEOUT,
            fetch_chunk_once(client, url, referer, start, end, expected),
        )
        .await;

        drop(_serial_guard);

        match result {
            Ok(Ok(bytes)) => return Ok(bytes),
            Ok(Err(Some(status)))
                if status == StatusCode::TOO_MANY_REQUESTS
                    || status == StatusCode::SERVICE_UNAVAILABLE
                    || status == StatusCode::FORBIDDEN =>
            {
                stats.step_down_to_single();
            }
            Err(_) => {
                // Hard per-chunk 6s timeout -> step down N -> 1 immediately.
                stats.step_down_to_single();
            }
            Ok(Err(_)) => {}
        }

        if attempt < CHUNK_RETRIES {
            tokio::time::sleep(Duration::from_millis(120 * attempt as u64)).await;
        }
    }

    Err(format!("failed upstream chunk {start}-{end}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{ProviderId, Resolution};
    use crate::providers::build_http_client;
    use std::time::Instant;

    async fn spawn_throttled_range_server(
        payload: Arc<Vec<u8>>,
        delay_per_request: Duration,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                let data = Arc::clone(&payload);
                tokio::spawn(async move {
                    let mut buf = [0u8; 2048];
                    let n = sock.read(&mut buf).await.unwrap_or(0);
                    let req = String::from_utf8_lossy(&buf[..n]);
                    let first = req.lines().next().unwrap_or("");
                    let range_hdr = req
                        .lines()
                        .find(|l| l.to_ascii_lowercase().starts_with("range:"))
                        .and_then(|l| l.split_once(':').map(|(_, v)| v.trim().to_string()));

                    if !delay_per_request.is_zero() {
                        tokio::time::sleep(delay_per_request).await;
                    }

                    if first.starts_with("HEAD ") {
                        let resp = format!(
                            "HTTP/1.1 200 OK\r\nAccept-Ranges: bytes\r\nContent-Length: {}\r\nContent-Type: video/mp4\r\nConnection: close\r\n\r\n",
                            data.len()
                        );
                        let _ = sock.write_all(resp.as_bytes()).await;
                        return;
                    }

                    if let Some(r) =
                        range_hdr.and_then(|h| h.strip_prefix("bytes=").map(str::to_string))
                    {
                        if let Some((s, e)) = r.split_once('-') {
                            let start: usize = s.parse().unwrap_or(0);
                            let end: usize = e
                                .parse::<usize>()
                                .unwrap_or(data.len() - 1)
                                .min(data.len() - 1);
                            let slice = &data[start..=end];
                            let hdr = format!(
                                "HTTP/1.1 206 Partial Content\r\nAccept-Ranges: bytes\r\nContent-Range: bytes {start}-{end}/{}\r\nContent-Length: {}\r\nContent-Type: video/mp4\r\nConnection: close\r\n\r\n",
                                data.len(),
                                slice.len()
                            );
                            let _ = sock.write_all(hdr.as_bytes()).await;
                            let _ = sock.write_all(slice).await;
                            return;
                        }
                    }

                    let hdr = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: video/mp4\r\nConnection: close\r\n\r\n",
                        data.len()
                    );
                    let _ = sock.write_all(hdr.as_bytes()).await;
                    let _ = sock.write_all(&data).await;
                });
            }
        });
        format!("http://{addr}/video.mp4")
    }

    #[test]
    fn eligibility_bypasses_hls_and_non_range_streams() {
        let mut s = StreamSource {
            url: "https://cdn.example/ep.mp4".into(),
            resolution: Resolution::Fhd1080,
            provider: ProviderId::Animerco,
            server_name: "yourupload".into(),
            referer: None,
            content_length: Some(10_000_000),
            verified: true,
            throughput_kbps: Some(2500),
            supports_ranges: true,
        };
        assert!(TurboStreamProxy::is_eligible(&s));

        s.supports_ranges = false;
        assert!(!TurboStreamProxy::is_eligible(&s));

        s.supports_ranges = true;
        s.url = "https://cdn.example/master.m3u8".into();
        assert!(!TurboStreamProxy::is_eligible(&s));

        // Strict single-socket filehosts (mp4upload.com and subdomains) must bypass proxy.
        s.url = "https://a4.mp4upload.com:282/d/abc123/video.mp4".into();
        assert!(!TurboStreamProxy::is_eligible(&s));
        s.url = "https://mp4upload.com/d/abc123/video.mp4".into();
        assert!(!TurboStreamProxy::is_eligible(&s));

        // Standard range-capable CDN remains eligible.
        s.url = "https://cdn.yourupload.com/stream/video.mp4".into();
        assert!(TurboStreamProxy::is_eligible(&s));
    }

    #[tokio::test]
    async fn turbo_proxy_accelerates_throttled_chunks_and_supports_mid_file_seeks() {
        // 2.5 MiB = 5 chunks of 512 KiB; 60ms delay per request -> sequential 5 chunks = 300ms,
        // parallel 6 workers finishes in ~60-90ms.
        let payload: Vec<u8> = (0..(2560 * 1024))
            .map(|i| ((i * 7 + 3) % 251) as u8)
            .collect();
        let expected = Arc::new(payload);
        let upstream_url =
            spawn_throttled_range_server(Arc::clone(&expected), Duration::from_millis(60)).await;

        let client = build_http_client(Duration::from_secs(5)).unwrap();
        let stream = StreamSource {
            url: upstream_url,
            resolution: Resolution::Fhd1080,
            provider: ProviderId::Animerco,
            server_name: "throttled".into(),
            referer: Some("https://eva.animerco.org/".into()),
            content_length: Some(expected.len() as u64),
            verified: true,
            throughput_kbps: Some(500),
            supports_ranges: true,
        };

        let proxy = TurboStreamProxy::start(client.clone(), stream, 6)
            .await
            .unwrap();

        // 1. HEAD check
        let head_resp = client.head(proxy.local_url()).send().await.unwrap();
        assert_eq!(head_resp.status(), StatusCode::OK);
        assert_eq!(head_resp.headers().get("accept-ranges").unwrap(), "bytes");
        assert_eq!(
            head_resp
                .headers()
                .get("content-length")
                .unwrap()
                .to_str()
                .unwrap(),
            expected.len().to_string()
        );

        // 2. Full GET reassembly (parallel workers beat sequential 5 * 60ms = 300ms)
        let started = Instant::now();
        let full_resp = client.get(proxy.local_url()).send().await.unwrap();
        assert_eq!(full_resp.status(), StatusCode::OK);
        let got_full = full_resp.bytes().await.unwrap();
        let elapsed = started.elapsed();
        assert_eq!(got_full.len(), expected.len());
        assert_eq!(&got_full[..], &expected[..]);
        assert!(
            elapsed < Duration::from_millis(240),
            "expected parallel 6x speedup (<240ms vs 300ms sequential), took {elapsed:?}"
        );

        // 3. Mid-file seek (206 Partial Content across chunk boundaries)
        let seek_start = 600_000usize;
        let seek_end = 1_200_000usize;
        let seek_resp = client
            .get(proxy.local_url())
            .header(RANGE, format!("bytes={seek_start}-{seek_end}"))
            .send()
            .await
            .unwrap();
        assert_eq!(seek_resp.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            seek_resp
                .headers()
                .get("content-range")
                .unwrap()
                .to_str()
                .unwrap(),
            format!("bytes {seek_start}-{seek_end}/{}", expected.len())
        );
        let got_slice = seek_resp.bytes().await.unwrap();
        assert_eq!(&got_slice[..], &expected[seek_start..=seek_end]);

        proxy.shutdown().await;
    }

    #[tokio::test]
    async fn adaptive_step_down_drops_from_3x_to_1x_on_429_and_completes_transfer() {
        // 4 chunks of 512 KiB = 2 MiB. Mock server allows only 1 concurrent upstream
        // request at a time; concurrent >1 returns 429 Too Many Requests.
        let payload: Vec<u8> = (0..(2048 * 1024))
            .map(|i| ((i * 11 + 5) % 251) as u8)
            .collect();
        let expected = Arc::new(payload);
        let in_flight = Arc::new(AtomicUsize::new(0));
        let saw_429 = Arc::new(AtomicUsize::new(0));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let data_clone = Arc::clone(&expected);
        let inflight_clone = Arc::clone(&in_flight);
        let saw_429_clone = Arc::clone(&saw_429);

        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                let data = Arc::clone(&data_clone);
                let active = Arc::clone(&inflight_clone);
                let rate_hits = Arc::clone(&saw_429_clone);
                tokio::spawn(async move {
                    let prev = active.fetch_add(1, Ordering::SeqCst);
                    let mut buf = [0u8; 2048];
                    let n = sock.read(&mut buf).await.unwrap_or(0);
                    let req = String::from_utf8_lossy(&buf[..n]);
                    let range_hdr = req
                        .lines()
                        .find(|l| l.to_ascii_lowercase().starts_with("range:"))
                        .and_then(|l| l.split_once(':').map(|(_, v)| v.trim().to_string()));

                    tokio::time::sleep(Duration::from_millis(35)).await;

                    if prev >= 1 {
                        rate_hits.fetch_add(1, Ordering::SeqCst);
                        let resp = "HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                        let _ = sock.write_all(resp.as_bytes()).await;
                        active.fetch_sub(1, Ordering::SeqCst);
                        return;
                    }

                    if let Some(r) =
                        range_hdr.and_then(|h| h.strip_prefix("bytes=").map(str::to_string))
                    {
                        if let Some((s, e)) = r.split_once('-') {
                            let start: usize = s.parse().unwrap_or(0);
                            let end: usize = e
                                .parse::<usize>()
                                .unwrap_or(data.len() - 1)
                                .min(data.len() - 1);
                            let slice = &data[start..=end];
                            let hdr = format!(
                                "HTTP/1.1 206 Partial Content\r\nAccept-Ranges: bytes\r\nContent-Range: bytes {start}-{end}/{}\r\nContent-Length: {}\r\nContent-Type: video/mp4\r\nConnection: close\r\n\r\n",
                                data.len(),
                                slice.len()
                            );
                            let _ = sock.write_all(hdr.as_bytes()).await;
                            let _ = sock.write_all(slice).await;
                        }
                    }
                    active.fetch_sub(1, Ordering::SeqCst);
                });
            }
        });

        let client = build_http_client(Duration::from_secs(5)).unwrap();
        let stream = StreamSource {
            url: format!("http://{addr}/strict-host.mp4"),
            resolution: Resolution::Fhd1080,
            provider: ProviderId::Animerco,
            server_name: "mp4upload".into(),
            referer: None,
            content_length: Some(expected.len() as u64),
            verified: true,
            throughput_kbps: Some(400),
            supports_ranges: true,
        };

        let proxy = TurboStreamProxy::start(client.clone(), stream, 3)
            .await
            .unwrap();
        assert_eq!(proxy.workers(), 3);

        let resp = client.get(proxy.local_url()).send().await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let got = resp.bytes().await.unwrap();
        assert_eq!(&got[..], &expected[..]);

        assert!(
            saw_429.load(Ordering::SeqCst) >= 1,
            "expected mock server to reject concurrent requests with 429"
        );
        assert_eq!(
            proxy.workers(),
            1,
            "proxy must adaptively step down from 3x -> 1x after 429"
        );

        proxy.shutdown().await;
    }

    #[tokio::test]
    async fn proxy_respects_8mib_lookahead_bound_and_pauses_under_backpressure() {
        // 16 MiB = 32 chunks of 512 KiB. When reader connects and pauses before reading body,
        // proxy must NOT fetch all 32 chunks (bounded to <= PROXY_MAX_WINDOW_CHUNKS + OS send buf).
        let total_size = 16 * 1024 * 1024usize;
        let payload = Arc::new(vec![0x42u8; total_size]);
        let upstream_hits = Arc::new(AtomicUsize::new(0));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let data_clone = Arc::clone(&payload);
        let hits_clone = Arc::clone(&upstream_hits);

        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                let data = Arc::clone(&data_clone);
                let hits = Arc::clone(&hits_clone);
                tokio::spawn(async move {
                    let mut buf = [0u8; 2048];
                    let n = sock.read(&mut buf).await.unwrap_or(0);
                    let req = String::from_utf8_lossy(&buf[..n]);
                    let range_hdr = req
                        .lines()
                        .find(|l| l.to_ascii_lowercase().starts_with("range:"))
                        .and_then(|l| l.split_once(':').map(|(_, v)| v.trim().to_string()));
                    if let Some(r) =
                        range_hdr.and_then(|h| h.strip_prefix("bytes=").map(str::to_string))
                    {
                        if let Some((s, e)) = r.split_once('-') {
                            hits.fetch_add(1, Ordering::SeqCst);
                            let start: usize = s.parse().unwrap_or(0);
                            let end: usize = e
                                .parse::<usize>()
                                .unwrap_or(data.len() - 1)
                                .min(data.len() - 1);
                            let slice = &data[start..=end];
                            let hdr = format!(
                                "HTTP/1.1 206 Partial Content\r\nAccept-Ranges: bytes\r\nContent-Range: bytes {start}-{end}/{}\r\nContent-Length: {}\r\nContent-Type: video/mp4\r\nConnection: close\r\n\r\n",
                                data.len(),
                                slice.len()
                            );
                            let _ = sock.write_all(hdr.as_bytes()).await;
                            let _ = sock.write_all(slice).await;
                        }
                    }
                });
            }
        });

        assert_eq!(MAX_AHEAD_BYTES, 8 * 1024 * 1024);
        assert_eq!(PROXY_MAX_WINDOW_CHUNKS, 16);

        let client = build_http_client(Duration::from_secs(5)).unwrap();
        let stream = StreamSource {
            url: format!("http://{addr}/video.mp4"),
            resolution: Resolution::Fhd1080,
            provider: ProviderId::Animerco,
            server_name: "cdn-bp".into(),
            referer: None,
            content_length: Some(total_size as u64),
            verified: true,
            throughput_kbps: Some(5000),
            supports_ranges: true,
        };

        let proxy = TurboStreamProxy::start(client, stream, 3).await.unwrap();
        let proxy_addr = proxy
            .local_url()
            .strip_prefix("http://")
            .unwrap()
            .split('/')
            .next()
            .unwrap()
            .to_string();

        // Connect raw TCP client with small receive buffer, send GET, and stop reading!
        let tcp = std::net::TcpStream::connect(&proxy_addr).unwrap();
        let mut reader = tokio::net::TcpStream::from_std({
            tcp.set_nonblocking(true).unwrap();
            tcp
        })
        .unwrap();
        reader
            .write_all(b"GET /stream HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(250)).await;
        let hits_paused = upstream_hits.load(Ordering::SeqCst);
        assert!(
            hits_paused <= PROXY_MAX_WINDOW_CHUNKS + 6,
            "expected upstream fetches to stop under backpressure (<=22 of 32), got {hits_paused}"
        );
        assert!(
            hits_paused < 32,
            "must not fetch entire 16 MiB file when reader pauses"
        );

        // Now drain the socket -> proxy resumes and finishes all 32 chunks cleanly.
        let mut total_rx = Vec::with_capacity(total_size + 512);
        reader.read_to_end(&mut total_rx).await.unwrap();
        let hdr_end = total_rx.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
        assert_eq!(total_rx.len() - hdr_end, total_size);
        assert_eq!(upstream_hits.load(Ordering::SeqCst), 32);

        proxy.shutdown().await;
    }
}
