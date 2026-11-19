use async_trait::async_trait;
use chrono::Utc;
use crossterm::event::KeyCode;
use senku_anime_cli::downloader::Downloader;
use senku_anime_cli::error::{Result, SenkuError};
use senku_anime_cli::history::HistoryStore;
use senku_anime_cli::metadata::{
    atomic_write_bytes, cache_slug, fetch_preview_metadata_with_opts, save_cached_metadata,
    AnimeMetadata, PREVIEW_DEBOUNCE,
};
use senku_anime_cli::models::{AnimeResult, Episode, ProviderId, Resolution, StreamSource};
use senku_anime_cli::player::{
    apply_speed_delta, apply_volume_delta, map_hotkey_action, parse_ipc_line, IpcSignal,
    LiveRemoteAction, MpvPlayer, PlaybackStatus, PlaybackStop,
};
use senku_anime_cli::providers::animerco::AnimercoProvider;
use senku_anime_cli::providers::common::expand_hls_streams;
use senku_anime_cli::providers::okanime::OkAnimeProvider;
use senku_anime_cli::providers::{
    build_http_client, fuse_catalog_results, normalize_fusion_key, AnimeProvider, ProviderEngine,
};
use senku_anime_cli::proxy::{TurboStreamProxy, MAX_AHEAD_BYTES};
use senku_anime_cli::resolve_episode_streams_with_fallback;
use senku_anime_cli::resolver::StreamResolver;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::tempdir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;

fn make_deterministic_payload(len: usize) -> Arc<Vec<u8>> {
    let mut data = vec![0u8; len];
    for (i, b) in data.iter_mut().enumerate() {
        *b = ((i.wrapping_mul(31) ^ (i >> 7)) & 0xff) as u8;
    }
    Arc::new(data)
}

async fn read_http_response_body(mut sock: TcpStream) -> (String, Vec<u8>) {
    let mut raw = Vec::new();
    let mut tmp = [0u8; 65_536];
    while let Ok(n) = sock.read(&mut tmp).await {
        if n == 0 {
            break;
        }
        raw.extend_from_slice(&tmp[..n]);
    }
    let sep = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("HTTP header separator");
    let hdr = String::from_utf8_lossy(&raw[..sep]).into_owned();
    let body = raw[sep + 4..].to_vec();
    (hdr, body)
}

fn parse_range_header(req: &str, total: usize) -> Option<(usize, usize)> {
    let hdr = req
        .lines()
        .find(|l| l.to_ascii_lowercase().starts_with("range:"))?
        .split_once(':')?
        .1
        .trim();
    let spec = hdr.strip_prefix("bytes=")?;
    let (s, e) = spec.split_once('-')?;
    let start: usize = s.trim().parse().ok()?;
    let end: usize = if e.trim().is_empty() {
        total - 1
    } else {
        e.trim().parse::<usize>().ok()?.min(total - 1)
    };
    Some((start, end))
}

#[derive(Clone, Copy)]
enum UpstreamFaultMode {
    Clean,
    MidChunkTcpDropEvery { drop_first_n: usize },
    Hostile429AndSlowLoris,
}

async fn spawn_chaos_range_upstream(
    payload: Arc<Vec<u8>>,
    mode: UpstreamFaultMode,
    bytes_served: Arc<AtomicUsize>,
    req_counter: Arc<AtomicUsize>,
) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let data = Arc::clone(&payload);
            let served = Arc::clone(&bytes_served);
            let counter = Arc::clone(&req_counter);
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                let n = sock.read(&mut buf).await.unwrap_or(0);
                if n == 0 {
                    return;
                }
                let req = String::from_utf8_lossy(&buf[..n]);
                let first = req.lines().next().unwrap_or("");
                if first.starts_with("HEAD ") {
                    let hdr = format!(
                        "HTTP/1.1 200 OK\r\nAccept-Ranges: bytes\r\nContent-Length: {}\r\nContent-Type: video/mp4\r\nConnection: close\r\n\r\n",
                        data.len()
                    );
                    let _ = sock.write_all(hdr.as_bytes()).await;
                    return;
                }

                let call_idx = counter.fetch_add(1, Ordering::SeqCst);
                let (start, end) =
                    parse_range_header(&req, data.len()).unwrap_or((0, data.len() - 1));
                let slice = &data[start..=end];

                match mode {
                    UpstreamFaultMode::Clean => {}
                    UpstreamFaultMode::MidChunkTcpDropEvery { drop_first_n } => {
                        if call_idx < drop_first_n {
                            let hdr = format!(
                                "HTTP/1.1 206 Partial Content\r\nAccept-Ranges: bytes\r\nContent-Range: bytes {start}-{end}/{}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                data.len(),
                                slice.len()
                            );
                            let _ = sock.write_all(hdr.as_bytes()).await;
                            let partial = (slice.len() / 3).max(1);
                            let _ = sock.write_all(&slice[..partial]).await;
                            // Abruptly close socket mid-chunk before Content-Length completes.
                            return;
                        }
                    }
                    UpstreamFaultMode::Hostile429AndSlowLoris => {
                        if call_idx == 0 {
                            // Slow-loris stall past CHUNK_TIMEOUT (6s) to trigger timeout step-down.
                            tokio::time::sleep(Duration::from_millis(6350)).await;
                            return;
                        }
                        if call_idx <= 2 {
                            let resp = "HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                            let _ = sock.write_all(resp.as_bytes()).await;
                            return;
                        }
                        if call_idx == 3 {
                            let resp = "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                            let _ = sock.write_all(resp.as_bytes()).await;
                            return;
                        }
                    }
                }

                let hdr = format!(
                    "HTTP/1.1 206 Partial Content\r\nAccept-Ranges: bytes\r\nContent-Range: bytes {start}-{end}/{}\r\nContent-Length: {}\r\nContent-Type: video/mp4\r\nConnection: close\r\n\r\n",
                    data.len(),
                    slice.len()
                );
                if sock.write_all(hdr.as_bytes()).await.is_ok()
                    && sock.write_all(slice).await.is_ok()
                {
                    served.fetch_add(slice.len(), Ordering::Relaxed);
                }
            });
        }
    });
    format!("http://{addr}/video.mp4")
}

// ============================================================================
// Phase 2.1: TurboStreamProxy Chaos & Performance Torture Tests
// ============================================================================

#[tokio::test]
async fn proxy_recovers_mid_stream_tcp_drop_byte_exact() {
    let payload = make_deterministic_payload(2 * 1024 * 1024); // 2 MiB (4 chunks)
    let served = Arc::new(AtomicUsize::new(0));
    let reqs = Arc::new(AtomicUsize::new(0));
    let url = spawn_chaos_range_upstream(
        Arc::clone(&payload),
        UpstreamFaultMode::MidChunkTcpDropEvery { drop_first_n: 3 },
        served,
        reqs,
    )
    .await;

    let stream = StreamSource {
        url,
        resolution: Resolution::Fhd1080,
        provider: ProviderId::OkAnime,
        server_name: "drop-upstream".into(),
        referer: None,
        content_length: Some(payload.len() as u64),
        verified: true,
        throughput_kbps: None,
        supports_ranges: true,
    };

    let client = build_http_client(Duration::from_secs(10)).unwrap();
    let handle = TurboStreamProxy::start(client, stream, 4).await.unwrap();
    let addr = handle
        .local_url()
        .trim_start_matches("http://")
        .trim_end_matches("/stream");

    let mut sock = TcpStream::connect(addr).await.unwrap();
    sock.write_all(b"GET /stream HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let (hdr, body) = read_http_response_body(sock).await;
    assert!(hdr.starts_with("HTTP/1.1 200 OK"));
    assert_eq!(body.len(), payload.len());
    assert_eq!(body, *payload);
    assert_eq!(handle.stats().in_flight_workers(), 0);
    handle.shutdown().await;
}

#[tokio::test]
async fn proxy_rapid_seek_storm_and_zero_worker_leak() {
    let payload = make_deterministic_payload(6 * 1024 * 1024);
    let served = Arc::new(AtomicUsize::new(0));
    let reqs = Arc::new(AtomicUsize::new(0));
    let url =
        spawn_chaos_range_upstream(Arc::clone(&payload), UpstreamFaultMode::Clean, served, reqs)
            .await;

    let stream = StreamSource {
        url,
        resolution: Resolution::Fhd1080,
        provider: ProviderId::Animerco,
        server_name: "seek-storm".into(),
        referer: None,
        content_length: Some(payload.len() as u64),
        verified: true,
        throughput_kbps: None,
        supports_ranges: true,
    };

    let client = build_http_client(Duration::from_secs(10)).unwrap();
    let handle = TurboStreamProxy::start(client, stream, 4).await.unwrap();
    let stats = handle.stats();
    let addr = handle
        .local_url()
        .trim_start_matches("http://")
        .trim_end_matches("/stream")
        .to_string();

    let total = payload.len();
    for i in 0..20usize {
        let offset = (i * 7919 * 37) % (total - 200_000);
        let len = 64 * 1024 + (i * 1319) % (128 * 1024);
        let end = (offset + len - 1).min(total - 1);

        let mut sock = TcpStream::connect(&addr).await.unwrap();
        if i % 4 == 0 {
            // Abruptly disconnect mid-transfer after receiving headers/early bytes.
            let req = format!(
                "GET /stream HTTP/1.1\r\nHost: 127.0.0.1\r\nRange: bytes={offset}-\r\nConnection: close\r\n\r\n"
            );
            sock.write_all(req.as_bytes()).await.unwrap();
            let mut mini = [0u8; 256];
            let _ = sock.read(&mut mini).await;
            drop(sock);
        } else {
            let req = format!(
                "GET /stream HTTP/1.1\r\nHost: 127.0.0.1\r\nRange: bytes={offset}-{end}\r\nConnection: close\r\n\r\n"
            );
            sock.write_all(req.as_bytes()).await.unwrap();
            let (hdr, body) = read_http_response_body(sock).await;
            assert!(hdr.starts_with("HTTP/1.1 206 Partial Content"));
            assert_eq!(body.len(), (end - offset) + 1);
            assert_eq!(&body[..], &payload[offset..=end]);
        }
    }

    tokio::time::sleep(Duration::from_millis(80)).await;
    handle.shutdown().await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        stats.in_flight_workers(),
        0,
        "all workers must terminate cleanly"
    );
}

#[tokio::test]
async fn proxy_adaptive_step_down_under_429_503_and_slow_loris_stall() {
    let payload = make_deterministic_payload(1_536 * 1024); // 1.5 MiB (3 chunks)
    let served = Arc::new(AtomicUsize::new(0));
    let reqs = Arc::new(AtomicUsize::new(0));
    let url = spawn_chaos_range_upstream(
        Arc::clone(&payload),
        UpstreamFaultMode::Hostile429AndSlowLoris,
        served,
        reqs,
    )
    .await;

    let stream = StreamSource {
        url,
        resolution: Resolution::Fhd1080,
        provider: ProviderId::OkAnime,
        server_name: "hostile-cdn".into(),
        referer: None,
        content_length: Some(payload.len() as u64),
        verified: true,
        throughput_kbps: None,
        supports_ranges: true,
    };

    let client = build_http_client(Duration::from_secs(15)).unwrap();
    let handle = TurboStreamProxy::start(client, stream, 3).await.unwrap();
    let addr = handle
        .local_url()
        .trim_start_matches("http://")
        .trim_end_matches("/stream");

    let mut sock = TcpStream::connect(addr).await.unwrap();
    sock.write_all(b"GET /stream HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let (_hdr, body) = read_http_response_body(sock).await;
    assert_eq!(
        handle.workers(),
        1,
        "expected adaptive step-down from 3x -> 1x"
    );
    assert_eq!(body.len(), payload.len());
    assert_eq!(body, *payload);
    handle.shutdown().await;
}

#[tokio::test]
async fn proxy_strict_8mib_backpressure_on_32mib_slow_reader() {
    let total_size = 32 * 1024 * 1024;
    let payload = make_deterministic_payload(total_size);
    let served = Arc::new(AtomicUsize::new(0));
    let reqs = Arc::new(AtomicUsize::new(0));
    let url = spawn_chaos_range_upstream(
        Arc::clone(&payload),
        UpstreamFaultMode::Clean,
        Arc::clone(&served),
        reqs,
    )
    .await;

    let stream = StreamSource {
        url,
        resolution: Resolution::Fhd1080,
        provider: ProviderId::Animerco,
        server_name: "bp-32mib".into(),
        referer: None,
        content_length: Some(total_size as u64),
        verified: true,
        throughput_kbps: None,
        supports_ranges: true,
    };

    let client = build_http_client(Duration::from_secs(10)).unwrap();
    let handle = TurboStreamProxy::start(client, stream, 3).await.unwrap();
    let addr = handle
        .local_url()
        .trim_start_matches("http://")
        .trim_end_matches("/stream");

    let mut sock = TcpStream::connect(addr).await.unwrap();
    sock.write_all(b"GET /stream HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();

    // Read only 4 KiB then stall as a slow player for 350ms.
    let mut head = [0u8; 4096];
    let _ = sock.read(&mut head).await.unwrap();
    tokio::time::sleep(Duration::from_millis(350)).await;

    let fetched = served.load(Ordering::Relaxed);
    let max_expected = MAX_AHEAD_BYTES + 4 * 1024 * 1024; // 8 MiB window + active worker/kernel buffer slack
    assert!(
        fetched <= max_expected,
        "upstream read {fetched} bytes exceeded bounded lookahead ceiling {max_expected} on 32 MiB stream"
    );

    drop(sock);
    handle.shutdown().await;
}

#[tokio::test]
async fn proxy_loopback_16mib_throughput_benchmark() {
    let total_size = 16 * 1024 * 1024;
    let payload = make_deterministic_payload(total_size);
    let served = Arc::new(AtomicUsize::new(0));
    let reqs = Arc::new(AtomicUsize::new(0));
    let url =
        spawn_chaos_range_upstream(Arc::clone(&payload), UpstreamFaultMode::Clean, served, reqs)
            .await;

    let stream = StreamSource {
        url,
        resolution: Resolution::Fhd1080,
        provider: ProviderId::OkAnime,
        server_name: "bench-16mib".into(),
        referer: None,
        content_length: Some(total_size as u64),
        verified: true,
        throughput_kbps: None,
        supports_ranges: true,
    };

    let client = build_http_client(Duration::from_secs(10)).unwrap();
    let handle = TurboStreamProxy::start(client, stream, 4).await.unwrap();
    let addr = handle
        .local_url()
        .trim_start_matches("http://")
        .trim_end_matches("/stream");

    let started = Instant::now();
    let mut sock = TcpStream::connect(addr).await.unwrap();
    sock.write_all(b"GET /stream HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let (_hdr, body) = read_http_response_body(sock).await;
    let elapsed = started.elapsed();

    assert_eq!(body.len(), total_size);
    assert_eq!(body, *payload);
    let mb_per_sec = (total_size as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64().max(0.0001);
    eprintln!(
        "[BENCH] TurboStreamProxy 16 MiB loopback: {mb_per_sec:.1} MB/s in {:.2}ms",
        elapsed.as_secs_f64() * 1000.0
    );
    assert!(
        mb_per_sec >= 15.0,
        "unexpected loopback bottleneck: {mb_per_sec:.1} MB/s"
    );
    handle.shutdown().await;
}

// ============================================================================
// Phase 2.2: Resolver, Portal Fusion & Metadata Chaos Tests
// ============================================================================

#[tokio::test]
async fn malformed_provider_and_hls_chaos_never_panics() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 2048];
                let n = sock.read(&mut buf).await.unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]);
                let first = req.lines().next().unwrap_or("");
                if first.contains("/bad-utf8") {
                    let mut resp = b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: 16\r\nConnection: close\r\n\r\n\xff\xfe<html\x80\xff><div".to_vec();
                    resp.extend_from_slice(&[0xff, 0xfe]);
                    let _ = sock.write_all(&resp).await;
                } else if first.contains("/corrupt.m3u8") {
                    let body = b"#NOT-EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=broken\n///\n\xff\xfe\n";
                    let hdr = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/vnd.apple.mpegurl\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = sock.write_all(hdr.as_bytes()).await;
                    let _ = sock.write_all(body).await;
                } else if first.contains("/loop1") {
                    let _ = sock
                        .write_all(b"HTTP/1.1 302 Found\r\nLocation: /loop2\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                        .await;
                } else if first.contains("/loop2") {
                    let _ = sock
                        .write_all(b"HTTP/1.1 302 Found\r\nLocation: /loop1\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                        .await;
                } else if first.contains("/fake-video.mp4") {
                    let html = b"<html><body>403 Cloudflare block</body></html>";
                    let hdr = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        html.len()
                    );
                    let _ = sock.write_all(hdr.as_bytes()).await;
                    let _ = sock.write_all(html).await;
                } else {
                    // 0-byte response
                    let _ = sock
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                        )
                        .await;
                }
            });
        }
    });

    let base = format!("http://{addr}");
    let client = build_http_client(Duration::from_secs(3)).unwrap();

    let oka = OkAnimeProvider::with_base_url(client.clone(), format!("{base}/bad-utf8"));
    let _ = oka.search("invalid").await;
    let mrc = AnimercoProvider::with_base_url(client.clone(), format!("{base}/zero"));
    let _ = mrc.search("empty").await;

    let hls_cand = vec![StreamSource {
        url: format!("{base}/corrupt.m3u8"),
        resolution: Resolution::Auto,
        provider: ProviderId::OkAnime,
        server_name: "corrupt-hls".into(),
        referer: None,
        content_length: None,
        verified: false,
        throughput_kbps: None,
        supports_ranges: false,
    }];
    let expanded = expand_hls_streams(&client, hls_cand).await;
    assert!(!expanded.is_empty());

    let resolver = StreamResolver::new(client);
    // Strict verify must reject fake HTML-as-200-mp4 and redirect loop!
    let bad_cands = vec![
        StreamSource {
            url: format!("{base}/fake-video.mp4"),
            resolution: Resolution::Fhd1080,
            provider: ProviderId::OkAnime,
            server_name: "fake-html".into(),
            referer: None,
            content_length: None,
            verified: false,
            throughput_kbps: None,
            supports_ranges: false,
        },
        StreamSource {
            url: format!("{base}/loop1"),
            resolution: Resolution::Hd720,
            provider: ProviderId::Animerco,
            server_name: "redirect-loop".into(),
            referer: None,
            content_length: None,
            verified: false,
            throughput_kbps: None,
            supports_ranges: false,
        },
    ];
    assert!(resolver
        .resolve_strict_verified(1, bad_cands, None)
        .await
        .is_err());
}

#[test]
fn portal_fusion_1000_entries_under_25ms_with_exact_grouping() {
    let mut items = Vec::with_capacity(1000);
    for i in 0..200 {
        let base_title = match i % 5 {
            0 => format!("ون بيس {i}: 'Adventure'! $Arc"),
            1 => format!("Hunter x Hunter {i}"),
            2 => format!("Black Clover {i}"),
            3 => format!("Attack on Titan {i} 2nd Season"),
            _ => format!("Fate/Zero {i}: Part 2 OVA"),
        };
        items.push(AnimeResult {
            slug: format!("mrc-{i}"),
            title: base_title.clone(),
            alt_title: None,
            url: format!("https://mrc.invalid/a/{i}"),
            provider: ProviderId::Animerco,
            year: Some("2011".into()),
            episode_count: Some(24),
            latency_ms: 20 + (i as u64 % 30),
            sources: Vec::new(),
        });
        // Matching variant on OkAnime with `(2011)` or trailing `2011` and punctuation differences:
        let variant_title = match i % 5 {
            1 => format!("Hunter x Hunter {i} (2011)"),
            2 => format!("Black Clover  {i}  2011"),
            _ => base_title.clone(),
        };
        items.push(AnimeResult {
            slug: format!("oka-{i}"),
            title: variant_title,
            alt_title: Some("عربي".into()),
            url: format!("https://oka.invalid/a/{i}"),
            provider: ProviderId::OkAnime,
            year: None,
            episode_count: Some(25),
            latency_ms: 35 + (i as u64 % 30),
            sources: Vec::new(),
        });
        // 3 distinct sequel/movie/part entries per group -> 5 * 200 = 1000 inputs!
        items.push(AnimeResult {
            slug: format!("oka-s2-{i}"),
            title: format!("{base_title} Season 2"),
            alt_title: None,
            url: format!("https://oka.invalid/s2/{i}"),
            provider: ProviderId::OkAnime,
            year: Some("2020".into()),
            episode_count: Some(12),
            latency_ms: 40,
            sources: Vec::new(),
        });
        items.push(AnimeResult {
            slug: format!("oka-mov-{i}"),
            title: format!("{base_title}: Mahou Tei no Ken Movie"),
            alt_title: None,
            url: format!("https://oka.invalid/mov/{i}"),
            provider: ProviderId::OkAnime,
            year: Some("2023".into()),
            episode_count: Some(1),
            latency_ms: 45,
            sources: Vec::new(),
        });
        items.push(AnimeResult {
            slug: format!("mrc-p2-{i}"),
            title: format!("{base_title} Part 2"),
            alt_title: None,
            url: format!("https://mrc.invalid/p2/{i}"),
            provider: ProviderId::Animerco,
            year: Some("2021".into()),
            episode_count: Some(12),
            latency_ms: 50,
            sources: Vec::new(),
        });
    }
    assert_eq!(items.len(), 1000);
    assert_eq!(
        normalize_fusion_key("Hunter x Hunter (2011)"),
        normalize_fusion_key("Hunter x Hunter")
    );

    let started = Instant::now();
    let fused = fuse_catalog_results(items, "Hunter x Hunter");
    let elapsed = started.elapsed();
    eprintln!(
        "[BENCH] Portal Fusion 1000 entries -> {} fused buckets in {:.2}ms",
        fused.len(),
        elapsed.as_secs_f64() * 1000.0
    );

    // 200 base (each fused from 2 providers) + 600 distinct sequel/movie/part entries = 800 buckets
    assert_eq!(fused.len(), 800);
    assert!(fused.iter().filter(|a| a.sources.len() == 2).count() == 200);
    assert!(
        elapsed < Duration::from_millis(25),
        "fusion took {:?}, expected < 25ms",
        elapsed
    );
}

#[tokio::test]
async fn concurrent_preview_cache_contention_16_tasks() {
    let dir = tempdir().unwrap();
    let target_meta = AnimeMetadata {
        id_mal: Some(11061),
        title_romaji: "Hunter x Hunter".into(),
        title_english: Some("Hunter x Hunter (2011)".into()),
        average_score: Some(90),
        episodes: Some(148),
        next_airing_episode: None,
        status: Some("FINISHED".into()),
        season_year: Some(2011),
        genres: vec!["Action".into()],
        description: "Gon Freecss.".into(),
        cover_image_url: None,
        cached_at: Utc::now().timestamp(),
    };

    let meta_path = dir
        .path()
        .join("meta")
        .join(format!("{}.json", cache_slug("Hunter x Hunter")));
    save_cached_metadata(&meta_path, &target_meta).unwrap();

    let client = build_http_client(Duration::from_secs(2)).unwrap();
    let mut set = JoinSet::new();
    for i in 0..16usize {
        let p = meta_path.clone();
        let m = target_meta.clone();
        let c = client.clone();
        let root = dir.path().to_path_buf();
        set.spawn(async move {
            let payload = serde_json::to_vec(&m).unwrap();
            atomic_write_bytes(&p, &payload).unwrap();
            if i % 2 == 0 {
                let hit_start = Instant::now();
                let res = fetch_preview_metadata_with_opts(
                    &c,
                    "  1 | [MRC+OKA] Hunter x Hunter (2011) (50ms)",
                    "http://127.0.0.1:9/unused",
                    "http://127.0.0.1:9/unused",
                    &root,
                    PREVIEW_DEBOUNCE,
                )
                .await
                .expect("cached hit must never corrupt");
                assert_eq!(res.id_mal, Some(11061));
                assert!(hit_start.elapsed() < Duration::from_millis(60));
            } else {
                let miss_start = Instant::now();
                let miss = fetch_preview_metadata_with_opts(
                    &c,
                    &format!("Uncached Title {i}"),
                    "http://127.0.0.1:9/fail",
                    "http://127.0.0.1:9/fail",
                    &root,
                    PREVIEW_DEBOUNCE,
                )
                .await;
                assert!(miss.is_none());
                assert!(miss_start.elapsed() >= Duration::from_millis(110));
            }
        });
    }
    while let Some(res) = set.join_next().await {
        res.unwrap();
    }

    // Verify no leftover temp files exist in meta/
    let leftover: Vec<_> = fs::read_dir(dir.path().join("meta"))
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".tmp"))
        .collect();
    assert!(leftover.is_empty(), "leftover tmp files: {leftover:?}");
}

#[tokio::test]
async fn cascade_priority_verified_primary_to_alt_provider_to_unverified() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let n = sock.read(&mut buf).await.unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]);
                let first = req.lines().next().unwrap_or("");
                if first.contains("/healthy.mp4") {
                    let _ = sock
                        .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: video/mp4\r\nContent-Length: 4096\r\nConnection: close\r\n\r\n")
                        .await;
                } else {
                    let _ = sock
                        .write_all(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                        .await;
                }
            });
        }
    });
    let base = format!("http://{addr}");

    struct FixedProvider {
        id: ProviderId,
        search_items: Vec<AnimeResult>,
        ep_items: Vec<Episode>,
        streams: Vec<StreamSource>,
    }

    #[async_trait]
    impl AnimeProvider for FixedProvider {
        fn id(&self) -> ProviderId {
            self.id
        }
        fn base_url(&self) -> &str {
            "https://mock.invalid"
        }
        async fn search(&self, _q: &str) -> Result<Vec<AnimeResult>> {
            Ok(self.search_items.clone())
        }
        async fn get_episodes(&self, _a: &AnimeResult) -> Result<Vec<Episode>> {
            Ok(self.ep_items.clone())
        }
        async fn get_streams(&self, _ep: &Episode) -> Result<Vec<StreamSource>> {
            Ok(self.streams.clone())
        }
    }

    let client = build_http_client(Duration::from_secs(3)).unwrap();
    let resolver = Arc::new(StreamResolver::new(client));

    let anime = AnimeResult {
        slug: "test-show".into(),
        title: "Test Show".into(),
        alt_title: None,
        url: "https://mock.invalid/a".into(),
        provider: ProviderId::OkAnime,
        year: None,
        episode_count: None,
        latency_ms: 5,
        sources: Vec::new(),
    };
    let ep = Episode {
        number: 1,
        title: None,
        url: "https://mock.invalid/e/1".into(),
        provider: ProviderId::OkAnime,
        sources: Vec::new(),
    };

    // Case A: Primary strict fails (403), Alternate provider Animerco returns healthy 200 -> picks Alternate!
    let eng_alt = Arc::new(ProviderEngine::with_providers(vec![
        Arc::new(FixedProvider {
            id: ProviderId::OkAnime,
            search_items: vec![],
            ep_items: vec![],
            streams: vec![StreamSource {
                url: format!("{base}/blocked-primary.mp4"),
                resolution: Resolution::Fhd1080,
                provider: ProviderId::OkAnime,
                server_name: "oka-403".into(),
                referer: None,
                content_length: None,
                verified: false,
                throughput_kbps: None,
                supports_ranges: false,
            }],
        }),
        Arc::new(FixedProvider {
            id: ProviderId::Animerco,
            search_items: vec![AnimeResult {
                provider: ProviderId::Animerco,
                ..anime.clone()
            }],
            ep_items: vec![Episode {
                provider: ProviderId::Animerco,
                ..ep.clone()
            }],
            streams: vec![StreamSource {
                url: format!("{base}/healthy.mp4"),
                resolution: Resolution::Hd720,
                provider: ProviderId::Animerco,
                server_name: "mrc-ok".into(),
                referer: None,
                content_length: None,
                verified: false,
                throughput_kbps: None,
                supports_ranges: false,
            }],
        }),
    ]));

    let picked_alt = resolve_episode_streams_with_fallback(&anime, &ep, None, &eng_alt, &resolver)
        .await
        .unwrap();
    assert_eq!(picked_alt[0].provider, ProviderId::Animerco);
    assert!(picked_alt[0].verified);

    // Case B: Both return 403 -> falls back to [Unverified] with verified = false!
    let eng_unv = Arc::new(ProviderEngine::with_providers(vec![Arc::new(
        FixedProvider {
            id: ProviderId::OkAnime,
            search_items: vec![],
            ep_items: vec![],
            streams: vec![StreamSource {
                url: format!("{base}/blocked-only.mp4"),
                resolution: Resolution::Fhd1080,
                provider: ProviderId::OkAnime,
                server_name: "oka-unverified".into(),
                referer: None,
                content_length: None,
                verified: false,
                throughput_kbps: None,
                supports_ranges: false,
            }],
        },
    )]));
    let picked_unv = resolve_episode_streams_with_fallback(&anime, &ep, None, &eng_unv, &resolver)
        .await
        .unwrap();
    assert_eq!(picked_unv.len(), 1);
    assert!(!picked_unv[0].verified);

    // Case C: 0 valid http(s) URLs -> clean NoValidStreams error (Zero-Exit menu catches cleanly).
    let eng_empty = Arc::new(ProviderEngine::with_providers(vec![]));
    let err = resolve_episode_streams_with_fallback(&anime, &ep, None, &eng_empty, &resolver)
        .await
        .unwrap_err();
    assert!(matches!(err, SenkuError::NoValidStreams { episode: 1 }));
}

// ============================================================================
// Phase 2.3: Player IPC, Hotkey Spam, History & Failover Chaos Tests
// ============================================================================

#[test]
fn player_malformed_ipc_frames_zero_panics() {
    let weird_inputs = [
        "",
        "   ",
        "{",
        "{\"event\":\"property-change\"",
        "{\"event\":\"property-change\",\"name\":\"percent-pos\",\"data\":\"not-a-number\"}",
        "{\"event\":\"property-change\",\"name\":\"time-pos\",\"data\":null}",
        "{\"event\":\"end-file\",\"reason\":null}",
        "{\"error\":\"error\",\"request_id\":999999,\"data\":[]}",
        "\0\x01\x02invalid",
    ];
    for frame in weird_inputs {
        let _ = parse_ipc_line(frame);
    }
    assert_eq!(
        parse_ipc_line(r#"{"event":"end-file","reason":"eof"}"#),
        Some(IpcSignal::EndFile {
            reason: "eof".into()
        })
    );
}

#[tokio::test]
async fn player_full_failover_cascade_mock_mpv() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempdir().unwrap();
    let mock_mpv = dir.path().join("mock-mpv-cascade.py");
    let sock_path = dir.path().join("cascade.sock");
    let log_path = Path::new(&format!("{}.events.log", sock_path.display())).to_path_buf();

    let script = r#"#!/usr/bin/env python3
import json, os, socket, sys, time

if "--version" in sys.argv:
    print("mpv 0.41.0-mock")
    sys.exit(0)

sock_path = None
for a in sys.argv:
    if a.startswith("--input-ipc-server="):
        sock_path = a.split("=", 1)[1]
log_file = sock_path + ".events.log"
with open(log_file, "a") as f:
    f.write("ARGS:" + " ".join(sys.argv[1:]) + "\n")

if os.path.exists(sock_path):
    os.unlink(sock_path)
srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
srv.bind(sock_path)
srv.listen(4)
conn, _ = srv.accept()

# Step 1: Primary mp4upload 1080p stalls (paused-for-cache: true for >=8.2s)
conn.sendall((json.dumps({"event": "property-change", "id": 7, "name": "paused-for-cache", "data": True}) + "\n").encode())

buf = ""
load_count = 0
deadline = time.time() + 18
while time.time() < deadline:
    conn.settimeout(0.25)
    try:
        chunk = conn.recv(4096)
        if not chunk:
            break
        buf += chunk.decode(errors="ignore")
        while "\n" in buf:
            line, buf = buf.split("\n", 1)
            if not line.strip():
                continue
            with open(log_file, "a") as f:
                f.write(line + "\n")
            msg = json.loads(line)
            cmd = msg.get("command") or []
            if len(cmd) >= 2 and cmd[0] == "loadfile":
                load_count += 1
                url = cmd[1]
                if "yourupload.com" in url or "720-mpu" in url:
                    # Simulate immediate end-file error on mirror #2 / #3
                    conn.sendall((json.dumps({"event": "end-file", "reason": "error"}) + "\n").encode())
                elif "vidcache.net" in url:
                    # Cross-provider Animerco backup succeeds!
                    conn.sendall((json.dumps({"event": "property-change", "id": 1, "name": "percent-pos", "data": 99.6}) + "\n").encode())
            elif len(cmd) >= 1 and cmd[0] == "quit":
                conn.close()
                srv.close()
                sys.exit(0)
    except socket.timeout:
        pass
    time.sleep(0.05)
"#;
    tokio::fs::write(&mock_mpv, script).await.unwrap();
    let mut perms = tokio::fs::metadata(&mock_mpv).await.unwrap().permissions();
    perms.set_mode(0o755);
    tokio::fs::set_permissions(&mock_mpv, perms).await.unwrap();

    struct EmptyOkProvider;
    struct BackupAnimerco;

    #[async_trait]
    impl AnimeProvider for EmptyOkProvider {
        fn id(&self) -> ProviderId {
            ProviderId::OkAnime
        }
        fn base_url(&self) -> &str {
            "https://okanime.invalid"
        }
        async fn search(&self, _q: &str) -> Result<Vec<AnimeResult>> {
            Ok(vec![])
        }
        async fn get_episodes(&self, _a: &AnimeResult) -> Result<Vec<Episode>> {
            Ok(vec![])
        }
        async fn get_streams(&self, _ep: &Episode) -> Result<Vec<StreamSource>> {
            Ok(vec![])
        }
    }

    #[async_trait]
    impl AnimeProvider for BackupAnimerco {
        fn id(&self) -> ProviderId {
            ProviderId::Animerco
        }
        fn base_url(&self) -> &str {
            "https://animerco.invalid"
        }
        async fn search(&self, _q: &str) -> Result<Vec<AnimeResult>> {
            Ok(vec![AnimeResult {
                slug: "cascade-anime".into(),
                title: "Cascade Anime".into(),
                alt_title: None,
                url: "https://animerco.invalid/cascade".into(),
                provider: ProviderId::Animerco,
                year: None,
                episode_count: None,
                latency_ms: 5,
                sources: Vec::new(),
            }])
        }
        async fn get_episodes(&self, _a: &AnimeResult) -> Result<Vec<Episode>> {
            Ok(vec![Episode {
                number: 1,
                title: None,
                url: "https://animerco.invalid/ep/1".into(),
                provider: ProviderId::Animerco,
                sources: Vec::new(),
            }])
        }
        async fn get_streams(&self, _ep: &Episode) -> Result<Vec<StreamSource>> {
            Ok(vec![StreamSource {
                url: "https://cdn.vidcache.net/cascade-ep1.m3u8".into(),
                resolution: Resolution::Fhd1080,
                provider: ProviderId::Animerco,
                server_name: "vidcache FHD".into(),
                referer: Some("https://eva.animerco.org/".into()),
                content_length: None,
                verified: true,
                throughput_kbps: Some(3000),
                supports_ranges: false,
            }])
        }
    }

    let client = build_http_client(Duration::from_secs(3)).unwrap();
    let engine = Arc::new(ProviderEngine::with_providers(vec![
        Arc::new(EmptyOkProvider),
        Arc::new(BackupAnimerco),
    ]));
    let resolver = Arc::new(StreamResolver::new(client.clone()));
    let history = HistoryStore::with_path(dir.path().join("hist.json"));
    let player = MpvPlayer::with_socket(&sock_path, false)
        .with_mpv_bin(mock_mpv.to_string_lossy().into_owned())
        .with_turbo_proxy(false);

    let anime = AnimeResult {
        slug: "cascade-anime".into(),
        title: "Cascade Anime".into(),
        alt_title: None,
        url: "https://okanime.invalid/cascade".into(),
        provider: ProviderId::OkAnime,
        year: None,
        episode_count: None,
        latency_ms: 10,
        sources: Vec::new(),
    };
    let episodes = vec![Episode {
        number: 1,
        title: None,
        url: "https://okanime.invalid/ep/1".into(),
        provider: ProviderId::OkAnime,
        sources: Vec::new(),
    }];
    // Initial streams: [0] stalls (mp4upload), [1] is same stalled host (must be skipped in favor of [2]!),
    // [2] (yourupload) fails with end-file error -> cross-provider Animerco backup succeeds!
    let initial_streams = vec![
        StreamSource {
            url: "https://s1.mp4upload.com:282/d/1080-mpu.mp4".into(),
            resolution: Resolution::Fhd1080,
            provider: ProviderId::OkAnime,
            server_name: "mp4upload FHD".into(),
            referer: Some("https://www.mp4upload.com/".into()),
            content_length: None,
            verified: true,
            throughput_kbps: Some(400),
            supports_ranges: false,
        },
        StreamSource {
            url: "https://s2.mp4upload.com:282/d/720-mpu.mp4".into(),
            resolution: Resolution::Hd720,
            provider: ProviderId::OkAnime,
            server_name: "mp4upload HD".into(),
            referer: Some("https://www.mp4upload.com/".into()),
            content_length: None,
            verified: true,
            throughput_kbps: Some(300),
            supports_ranges: false,
        },
        StreamSource {
            url: "https://cdn.yourupload.com/ep1.mp4".into(),
            resolution: Resolution::Hd720,
            provider: ProviderId::OkAnime,
            server_name: "yourupload HD".into(),
            referer: Some("https://www.yourupload.com/".into()),
            content_length: None,
            verified: true,
            throughput_kbps: Some(900),
            supports_ranges: false,
        },
    ];
    let mut session_q = None;
    let stop = player
        .play_loop(
            &anime,
            &episodes,
            0,
            initial_streams,
            Some(42),
            &mut session_q,
            client,
            engine,
            resolver,
            &history,
        )
        .await
        .unwrap();
    assert_eq!(stop, PlaybackStop::CompletedSeries);

    let logged = tokio::fs::read_to_string(&log_path).await.unwrap();
    let yu_pos = logged.find("cdn.yourupload.com/ep1.mp4").expect(
        "host-aware failover must try healthy yourupload before retrying stalled mp4upload host",
    );
    if let Some(mpu_pos) = logged.find("720-mpu.mp4") {
        assert!(yu_pos < mpu_pos);
    }
    assert!(logged.contains(r#""referrer","https://eva.animerco.org/""#));
    assert!(logged.contains(r#""force-media-title","Cascade Anime — Ep 01 [1080p]""#));
    assert!(logged.contains(r#""start",42"#) || logged.contains("--start=42"));
    assert!(logged.contains("cdn.vidcache.net/cascade-ep1.m3u8"));
}

#[test]
fn player_hotkey_fuzz_all_keys_across_boundaries() {
    let mut status = PlaybackStatus {
        episode_num: 1,
        resolution: Resolution::Fhd1080,
        time_pos: 0.0,
        duration: Some(1400.0),
        percent_pos: 0.0,
        paused: false,
        autoplay: true,
        skip_intro_secs: 85,
        cache_duration_secs: Some(60.0),
        paused_for_cache: false,
        turbo_workers: None,
        proxy_bps: 0,
        volume: 0,
        muted: false,
        speed: 0.50,
        op_detected: false,
    };

    // Lower boundary clamping
    assert_eq!(apply_volume_delta(0, -5), 0);
    assert_eq!(apply_speed_delta(0.50, -0.25), 0.50);

    // Upper boundary clamping
    status.volume = 130;
    status.speed = 2.00;
    status.time_pos = 1399.5;
    assert_eq!(apply_volume_delta(130, 5), 130);
    assert_eq!(apply_speed_delta(2.00, 0.25), 2.00);

    let all_keys = [
        KeyCode::Char(' '),
        KeyCode::Left,
        KeyCode::Right,
        KeyCode::Char('-'),
        KeyCode::Char('+'),
        KeyCode::Char('m'),
        KeyCode::Char('['),
        KeyCode::Char(']'),
        KeyCode::Char('f'),
        KeyCode::Char('a'),
        KeyCode::Char('i'),
        KeyCode::Char('v'),
        KeyCode::Char('n'),
        KeyCode::Char('p'),
        KeyCode::Char('q'),
        KeyCode::Esc,
        KeyCode::Enter,
    ];
    for pos in [0.0, 12.0, 1399.0] {
        status.time_pos = pos;
        for &code in &all_keys {
            let act = map_hotkey_action(code, &status, &[]);
            match code {
                KeyCode::Enter => assert!(act.is_none()),
                KeyCode::Esc => assert_eq!(act, Some(LiveRemoteAction::Quit)),
                _ => assert!(act.is_some()),
            }
            if let Some(LiveRemoteAction::SetVolume { volume }) = act {
                assert!((0..=130).contains(&volume));
            }
            if let Some(LiveRemoteAction::SetSpeed { speed }) = act {
                assert!((0.50..=2.00).contains(&speed));
            }
        }
    }
}

#[tokio::test]
async fn history_chaos_corruption_legacy_and_20_concurrent_updates() {
    let dir = tempdir().unwrap();
    let hist_path = dir.path().join("history.json");
    let store = Arc::new(HistoryStore::with_path(&hist_path));

    // 1. Empty file, truncated JSON, and binary garbage -> backed up & reinitialized cleanly
    fs::write(&hist_path, "   \n").unwrap();
    assert!(store.load_all().unwrap().is_empty());

    fs::write(&hist_path, r#"[{"anime_slug":"cut","title":"Cut"#).unwrap();
    assert!(store.load_all().unwrap().is_empty());
    assert!(store.backup_path().exists());

    fs::write(&hist_path, [0xffu8, 0x00, 0xfe, 0x11, 0x99]).unwrap();
    assert!(store.load_all().unwrap().is_empty());
    assert!(store.backup_path().exists());

    // 2. Legacy v1.0 schema without position_secs, sources, or watched_episodes
    let v1_json =
        r#"[{"anime_slug":"bleach","title":"Bleach","episode":55,"timestamp":1600000000}]"#;
    fs::write(&hist_path, v1_json).unwrap();
    let legacy = store.latest().unwrap();
    assert_eq!(legacy.episode, 55);
    assert_eq!(legacy.position_secs, None);
    assert!(legacy.sources.is_empty());
    assert!(legacy.watched_episodes.is_empty());

    // 3. 20 rapid concurrent updates across tokio tasks -> zero JSON corruption or .tmp leaks
    let anime = AnimeResult {
        slug: "one-piece".into(),
        title: "One Piece".into(),
        alt_title: None,
        url: "https://oka.invalid/one-piece".into(),
        provider: ProviderId::OkAnime,
        year: None,
        episode_count: Some(1100),
        latency_ms: 10,
        sources: Vec::new(),
    };

    let mut set = JoinSet::new();
    for ep in 1..=20u32 {
        let st = Arc::clone(&store);
        let a = anime.clone();
        set.spawn(async move {
            st.record_watch_state(&a, ep, Some(u64::from(ep) * 10), true)
                .unwrap();
        });
    }
    while let Some(r) = set.join_next().await {
        r.unwrap();
    }

    let final_entries = store.load_all().unwrap();
    assert!(!final_entries.is_empty());
    let op = final_entries
        .iter()
        .find(|e| e.anime_slug == "one-piece")
        .unwrap();
    assert!(!op.watched_episodes.is_empty());
}

// ============================================================================
// Phase 2.4: Downloader Edge-Case Tests
// ============================================================================

#[tokio::test]
async fn downloader_arabic_traversal_range_retry_and_single_stream_fallback() {
    let expected = make_deterministic_payload(768 * 1024); // >= MIN_CHUNKED_BYTES (512 KiB)

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let attempts = Arc::new(AtomicUsize::new(0));
    tokio::spawn({
        let data = Arc::clone(&expected);
        let attempts_ref = Arc::clone(&attempts);
        async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                let data = Arc::clone(&data);
                let att = Arc::clone(&attempts_ref);
                tokio::spawn(async move {
                    let mut buf = [0u8; 2048];
                    let n = sock.read(&mut buf).await.unwrap_or(0);
                    let req = String::from_utf8_lossy(&buf[..n]);
                    let first = req.lines().next().unwrap_or("");
                    if first.contains("/single") {
                        if first.starts_with("HEAD ") {
                            let hdr = format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                data.len()
                            );
                            let _ = sock.write_all(hdr.as_bytes()).await;
                        } else {
                            let hdr = format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                data.len()
                            );
                            let _ = sock.write_all(hdr.as_bytes()).await;
                            let _ = sock.write_all(&data).await;
                        }
                        return;
                    }

                    if first.starts_with("HEAD ") {
                        let hdr = format!(
                            "HTTP/1.1 200 OK\r\nAccept-Ranges: bytes\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            data.len()
                        );
                        let _ = sock.write_all(hdr.as_bytes()).await;
                        return;
                    }

                    let idx = att.fetch_add(1, Ordering::SeqCst);
                    let (start, end) =
                        parse_range_header(&req, data.len()).unwrap_or((0, data.len() - 1));
                    let slice = &data[start..=end];
                    let hdr = format!(
                        "HTTP/1.1 206 Partial Content\r\nAccept-Ranges: bytes\r\nContent-Range: bytes {start}-{end}/{}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        data.len(),
                        slice.len()
                    );
                    let _ = sock.write_all(hdr.as_bytes()).await;
                    if idx == 1 {
                        // Drop mid-chunk on attempt #1 to exercise retry & partial overwrite.
                        let _ = sock.write_all(&slice[..slice.len() / 2]).await;
                        return;
                    }
                    let _ = sock.write_all(slice).await;
                });
            }
        }
    });

    let dir = tempdir().unwrap();
    let client = build_http_client(Duration::from_secs(5)).unwrap();
    let downloader = Downloader::new(client, 4);

    let traversal_title = r#"../../ون بيس: "الحلقة" 1000?"#;
    let range_stream = StreamSource {
        url: format!("http://{addr}/range.mp4"),
        resolution: Resolution::Fhd1080,
        provider: ProviderId::Animerco,
        server_name: "range".into(),
        referer: None,
        content_length: Some(expected.len() as u64),
        verified: true,
        throughput_kbps: None,
        supports_ranges: true,
    };

    let out_range = downloader
        .download_episode(traversal_title, 1000, &range_stream, dir.path())
        .await
        .unwrap();
    assert!(out_range.starts_with(dir.path()));
    assert_eq!(tokio::fs::read(&out_range).await.unwrap(), *expected);

    let single_stream = StreamSource {
        url: format!("http://{addr}/single.mp4"),
        resolution: Resolution::Hd720,
        provider: ProviderId::OkAnime,
        server_name: "single".into(),
        referer: None,
        content_length: Some(expected.len() as u64),
        verified: true,
        throughput_kbps: None,
        supports_ranges: false,
    };
    let out_single = downloader
        .download_episode(traversal_title, 1001, &single_stream, dir.path())
        .await
        .unwrap();
    assert_eq!(tokio::fs::read(&out_single).await.unwrap(), *expected);
}
