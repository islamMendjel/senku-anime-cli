use crate::error::{Result, SenkuError};
use crate::models::{ProviderId, Resolution, StreamSource};
use regex::Regex;
use reqwest::header::{
    HeaderMap, HeaderValue, ACCEPT, ACCEPT_LANGUAGE, CONTENT_TYPE, REFERER,
    UPGRADE_INSECURE_REQUESTS,
};
use reqwest::{Client, Response};
use scraper::{Html, Selector};
use std::collections::HashSet;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::task::JoinSet;
use url::Url;

/// Modern Linux Chrome User-Agent shared across HTTP requests and mpv playback.
pub const CHROME_USER_AGENT: &str =
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";

/// Builds a realistic, telemetry-free HTTP client with browser headers,
/// cookie store, connection pooling, and automatic redirect handling.
pub fn build_http_client(timeout: Duration) -> Result<Client> {
    let mut default_headers = HeaderMap::new();
    default_headers.insert(
        ACCEPT,
        HeaderValue::from_static(
            "text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.8,*/*;q=0.7",
        ),
    );
    default_headers.insert(
        ACCEPT_LANGUAGE,
        HeaderValue::from_static("ar,en-US;q=0.9,en;q=0.8"),
    );
    default_headers.insert(UPGRADE_INSECURE_REQUESTS, HeaderValue::from_static("1"));

    let client = Client::builder()
        .user_agent(CHROME_USER_AGENT)
        .default_headers(default_headers)
        .cookie_store(true)
        .pool_max_idle_per_host(16)
        .pool_idle_timeout(Duration::from_secs(90))
        .tcp_nodelay(true)
        .tcp_keepalive(Duration::from_secs(30))
        .tcp_keepalive_interval(Duration::from_secs(15))
        .connect_timeout(Duration::from_secs(5))
        .timeout(timeout)
        .redirect(reqwest::redirect::Policy::limited(10))
        .build()?;
    Ok(client)
}

/// Validates HTTP response status and surfaces explicit diagnostics (including
/// Cloudflare anti-bot challenge headers) instead of generic failures.
pub fn check_response_status(provider: ProviderId, resp: &Response) -> Result<()> {
    let status = resp.status();
    if status.is_success() {
        return Ok(());
    }

    let final_url = resp.url().to_string();
    let cf_mitigated = resp
        .headers()
        .get("cf-mitigated")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let reason = if cf_mitigated.eq_ignore_ascii_case("challenge") {
        format!("HTTP {status} from {final_url} (Cloudflare anti-bot challenge active)")
    } else {
        format!("HTTP {status} from {final_url}")
    };

    Err(SenkuError::ProviderFailure {
        provider: provider.to_string(),
        reason,
    })
}

/// Extracts a clean URL slug from an anime URL path.
pub fn extract_slug(raw_url: &str, fallback_title: &str) -> String {
    if let Ok(parsed) = Url::parse(raw_url) {
        if let Some(last) = parsed
            .path_segments()
            .and_then(|mut segs| segs.rfind(|s| !s.is_empty()))
        {
            let clean = last.trim().to_ascii_lowercase();
            if !clean.is_empty() {
                return clean;
            }
        }
    }
    fallback_title
        .trim()
        .to_ascii_lowercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect::<String>()
        .trim_matches('-')
        .to_string()
}

/// Resolves a potentially relative `href` or `src` against a base URL.
pub fn resolve_url(base: &str, candidate: &str) -> Option<String> {
    let trimmed = candidate.trim();
    if trimmed.is_empty() || trimmed.starts_with("javascript:") || trimmed.starts_with('#') {
        return None;
    }
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        return Some(trimmed.to_string());
    }
    if trimmed.starts_with("//") {
        return Some(format!("https:{trimmed}"));
    }
    Url::parse(base)
        .ok()
        .and_then(|b| b.join(trimmed).ok())
        .map(|u| u.to_string())
}

/// Parses an episode number out of URLs or labels like `"Episode 12"`,
/// `"الحلقة 05"`, or percent-encoded Arabic slugs.
pub fn parse_episode_number(text: &str) -> Option<u32> {
    let decoded = urlencoding::decode(text)
        .map(|c| c.into_owned())
        .unwrap_or_else(|_| text.to_string());

    static EP_RE: OnceLock<Regex> = OnceLock::new();
    let re = EP_RE.get_or_init(|| {
        Regex::new(r"(?i)(?:episode|ep|الحلقة|حلقة|e)[\s:/_-]*0*([0-9]{1,4})")
            .expect("valid episode regex")
    });
    if let Some(cap) = re.captures(&decoded) {
        if let Ok(n) = cap[1].parse::<u32>() {
            if n > 0 {
                return Some(n);
            }
        }
    }

    static NUM_RE: OnceLock<Regex> = OnceLock::new();
    let num_re = NUM_RE.get_or_init(|| Regex::new(r"([0-9]{1,4})").expect("valid num regex"));
    num_re
        .captures_iter(&decoded)
        .last()
        .and_then(|c| c[1].parse::<u32>().ok())
        .filter(|&n| n > 0)
}

/// Unpacks Dean Edwards `eval(function(p,a,c,k,e,d){...})` JavaScript blocks
/// commonly used by video embed hosts (`uqload`, `streamwish`, `filelions`, `mp4upload`).
pub fn unpack_packed_js(html: &str) -> String {
    static PACKED_RE: OnceLock<Regex> = OnceLock::new();
    let re = PACKED_RE.get_or_init(|| {
        Regex::new(
            r#"(?s)eval\(function\(p,a,c,k,e,d\)\{.*?\}\(['"](.*?)['"],(\d+),(\d+),['"](.*?)['"]\.split\(['"]\|['"]\)(?:,\d+,\{\})?\)\)"#,
        )
        .expect("valid packed js regex")
    });

    static IDENT_RE: OnceLock<Regex> = OnceLock::new();
    let ident_re = IDENT_RE.get_or_init(|| Regex::new(r"\b[0-9a-zA-Z]+\b").unwrap());

    let mut out = String::new();
    for cap in re.captures_iter(html) {
        let mut p = cap[1].to_string();
        let Ok(radix) = cap[2].parse::<u32>() else {
            continue;
        };
        let Ok(count) = cap[3].parse::<usize>() else {
            continue;
        };
        let dict: Vec<&str> = cap[4].split('|').collect();
        if !(2..=62).contains(&radix) {
            continue;
        }

        // Replace tokens using word boundaries in a single regex pass over identifiers.
        let unpacked = ident_re.replace_all(&p, |caps: &regex::Captures<'_>| {
            let tok = &caps[0];
            if let Some(idx) = decode_base_n(tok, radix) {
                if idx < count && idx < dict.len() && !dict[idx].is_empty() {
                    return dict[idx].to_string();
                }
            }
            tok.to_string()
        });
        p = unpacked.into_owned();
        out.push_str(&p);
        out.push('\n');
    }
    out
}

fn decode_base_n(token: &str, radix: u32) -> Option<usize> {
    let mut acc: usize = 0;
    for b in token.bytes() {
        let digit = match b {
            b'0'..=b'9' => (b - b'0') as u32,
            b'a'..=b'z' => (b - b'a' + 10) as u32,
            b'A'..=b'Z' => (b - b'A' + 36) as u32,
            _ => return None,
        };
        if digit >= radix {
            return None;
        }
        acc = acc
            .checked_mul(radix as usize)?
            .checked_add(digit as usize)?;
    }
    Some(acc)
}

/// Extracts candidate direct video stream URLs (`.mp4`, `.m3u8`, `og:video`,
/// `jwplayerOptions`, and unpacked Dean Edwards JS) from an HTML page.
pub fn extract_streams_from_html(
    html_body: &str,
    page_url: &str,
    provider: ProviderId,
) -> Vec<StreamSource> {
    extract_streams_with_hint(html_body, page_url, provider, "direct", Resolution::Unknown)
}

/// Same as `extract_streams_from_html`, allowing caller-supplied server name
/// and fallback resolution hint (e.g. `Michi FHD` -> `1080p`).
pub fn extract_streams_with_hint(
    html_body: &str,
    page_url: &str,
    provider: ProviderId,
    server_hint: &str,
    res_hint: Resolution,
) -> Vec<StreamSource> {
    let document = Html::parse_document(html_body);
    let mut seen = HashSet::new();
    let mut streams = Vec::new();

    let mut push_stream = |raw_url: &str, label: &str, server: &str| {
        if let Some(abs_url) = resolve_url(page_url, raw_url) {
            let lower = abs_url.to_ascii_lowercase();
            // Skip watermark/logo images or non-video static assets.
            if lower.ends_with(".png")
                || lower.ends_with(".jpg")
                || lower.ends_with(".jpeg")
                || lower.ends_with(".svg")
                || lower.ends_with(".css")
                || lower.ends_with(".js")
            {
                return;
            }
            if seen.insert(abs_url.clone()) {
                let mut res = Resolution::parse_from_label(label);
                if res == Resolution::Unknown {
                    res = Resolution::parse_from_label(&abs_url);
                }
                if res == Resolution::Unknown {
                    res = res_hint;
                }
                streams.push(StreamSource {
                    url: abs_url,
                    resolution: res,
                    provider,
                    server_name: server.to_string(),
                    referer: Some(page_url.to_string()),
                    content_length: None,
                    verified: false,
                    throughput_kbps: None,
                    supports_ranges: false,
                });
            }
        }
    };

    // 1. <meta property="og:video" content="..."> (e.g., YourUpload direct mp4)
    if let Ok(sel) = Selector::parse("meta[property='og:video'], meta[property='og:video:url']") {
        for el in document.select(&sel) {
            if let Some(content) = el.value().attr("content") {
                push_stream(content, "", server_hint);
            }
        }
    }

    // 2. <video><source src="..." label="1080p" /></video>
    if let Ok(sel) = Selector::parse("video source, video, source[src]") {
        for el in document.select(&sel) {
            if let Some(src) = el.value().attr("src") {
                let label = el
                    .value()
                    .attr("label")
                    .or_else(|| el.value().attr("size"))
                    .or_else(|| el.value().attr("data-quality"))
                    .unwrap_or("");
                push_stream(src, label, server_hint);
            }
        }
    }

    // 3. Direct MP4 / M3U8 links in anchors or data attributes
    if let Ok(sel) = Selector::parse("a[href], button[data-url], li[data-url], div[data-src]") {
        for el in document.select(&sel) {
            let link = el
                .value()
                .attr("href")
                .or_else(|| el.value().attr("data-url"))
                .or_else(|| el.value().attr("data-src"));
            if let Some(url) = link {
                let lower = url.to_ascii_lowercase();
                if lower.contains(".mp4") || lower.contains(".m3u8") {
                    let text = el.text().collect::<Vec<_>>().join(" ");
                    let quality_attr = el
                        .value()
                        .attr("data-quality")
                        .or_else(|| el.value().attr("title"))
                        .unwrap_or("");
                    let label = format!("{quality_attr} {text}");
                    push_stream(url, &label, server_hint);
                }
            }
        }
    }

    // 4. Unpack any Dean Edwards packed JS blocks and scan both raw & unpacked text
    let unpacked = unpack_packed_js(html_body);
    let combined = if unpacked.is_empty() {
        std::borrow::Cow::Borrowed(html_body)
    } else {
        std::borrow::Cow::Owned(format!("{html_body}\n{unpacked}"))
    };

    static MEDIA_URL_RE: OnceLock<Regex> = OnceLock::new();
    let media_re = MEDIA_URL_RE.get_or_init(|| {
        Regex::new(r#"(?i)(https?://[^\s"'<>\\]+\.(?:mp4|m3u8)(?:\?[^\s"'<>\\]*)?)"#)
            .expect("valid media url regex")
    });
    for cap in media_re.captures_iter(&combined) {
        let raw = cap[1].replace("\\/", "/");
        push_stream(&raw, &raw, server_hint);
    }

    // 5. jwplayer / videojs / mp4upload / sendvid direct JS patterns
    static PLAYER_FILE_RE: OnceLock<Regex> = OnceLock::new();
    let player_re = PLAYER_FILE_RE.get_or_init(|| {
        Regex::new(r#"(?i)(?:file|src|hls\d*|video_source)\s*[:=]\s*['"](https?://[^'"]+)['"]"#)
            .expect("valid player file regex")
    });
    for cap in player_re.captures_iter(&combined) {
        let candidate = cap[1].replace("\\/", "/");
        let lower = candidate.to_ascii_lowercase();
        if lower.contains("video")
            || lower.contains(".mp4")
            || lower.contains(".m3u8")
            || lower.contains("vidcache")
            || lower.contains("sendvid")
        {
            push_stream(&candidate, &candidate, server_hint);
        }
    }

    // 6. mp4upload `player.src("https://...")`
    static MP4UPLOAD_SRC_RE: OnceLock<Regex> = OnceLock::new();
    let mp4_re = MP4UPLOAD_SRC_RE.get_or_init(|| {
        Regex::new(r#"(?i)player\.src\(\s*['"](https?://[^'"]+)['"]"#)
            .expect("valid mp4upload src regex")
    });
    for cap in mp4_re.captures_iter(&combined) {
        push_stream(&cap[1], server_hint, "mp4upload");
    }

    // 7. vkvideo / vk.com `"url1080":"..."`, `"url720":"..."`, `"url480":"..."`, `"hls":"..."`
    static VK_URL_RE: OnceLock<Regex> = OnceLock::new();
    let vk_re = VK_URL_RE.get_or_init(|| {
        Regex::new(r#""(?:url(2160|1440|1080|720|480|360)|hls)"\s*:\s*"(https?:[^"]+)""#)
            .expect("valid vk regex")
    });
    for cap in vk_re.captures_iter(&combined) {
        let label = cap.get(1).map(|m| m.as_str()).unwrap_or("hls");
        let url = cap[2].replace("\\/", "/");
        push_stream(&url, label, "vkvideo");
    }

    // 8. ok.ru `data-options` or inline JSON `"videos":[{"name":"full","url":"..."}]` / `ondemandHls`
    let html_unescaped = combined
        .replace("&quot;", "\"")
        .replace("\\u0026", "&")
        .replace("\\/", "/");
    static OKRU_VIDEO_RE: OnceLock<Regex> = OnceLock::new();
    let ok_re = OKRU_VIDEO_RE.get_or_init(|| {
        Regex::new(
            r#""name"\s*:\s*"(ultra|quad|full|hd|sd|low|lowest|mobile)"\s*,\s*"url"\s*:\s*"(https?://[^"]+)""#,
        )
        .expect("valid ok.ru video regex")
    });
    for cap in ok_re.captures_iter(&html_unescaped) {
        let q = match cap[1].to_ascii_lowercase().as_str() {
            "ultra" | "quad" => "2160p",
            "full" => "1080p",
            "hd" => "720p",
            "sd" | "low" => "480p",
            "lowest" | "mobile" => "360p",
            _ => "",
        };
        push_stream(&cap[2], q, "ok.ru");
    }

    static OKRU_HLS_RE: OnceLock<Regex> = OnceLock::new();
    let ok_hls_re = OKRU_HLS_RE.get_or_init(|| {
        Regex::new(r#""(?:ondemandHls|hlsMasterPlaylistUrl)"\s*:\s*"(https?://[^"]+)""#)
            .expect("valid ok.ru hls regex")
    });
    for cap in ok_hls_re.captures_iter(&html_unescaped) {
        push_stream(&cap[1], "hls", "ok.ru");
    }

    streams
}

/// Parses an HLS master playlist (`#EXT-X-STREAM-INF`) into individual variant
/// `StreamSource`s (`1080p`, `720p`, `480p`, etc.) using `RESOLUTION=WxH` and
/// `BANDWIDTH=` tags. Returns an empty Vec if `playlist` is not a master playlist.
pub fn parse_master_m3u8(
    playlist: &str,
    base_url: &str,
    template: &StreamSource,
) -> Vec<StreamSource> {
    static RES_RE: OnceLock<Regex> = OnceLock::new();
    let res_re = RES_RE
        .get_or_init(|| Regex::new(r"(?i)RESOLUTION\s*=\s*\d+x(\d+)").expect("valid m3u8 res re"));
    static BW_RE: OnceLock<Regex> = OnceLock::new();
    let bw_re =
        BW_RE.get_or_init(|| Regex::new(r"(?i)BANDWIDTH\s*=\s*(\d+)").expect("valid m3u8 bw re"));

    let mut variants: Vec<(u64, StreamSource)> = Vec::new();
    let mut pending_info: Option<&str> = None;

    for raw_line in playlist.lines() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        if line.starts_with("#EXT-X-STREAM-INF:") {
            pending_info = Some(line);
            continue;
        }
        if line.starts_with('#') {
            continue;
        }

        if let Some(info) = pending_info.take() {
            let Some(abs_uri) = resolve_url(base_url, line) else {
                continue;
            };
            let height = res_re.captures(info).and_then(|c| c[1].parse::<u32>().ok());
            let bandwidth = bw_re
                .captures(info)
                .and_then(|c| c[1].parse::<u64>().ok())
                .unwrap_or(0);

            let resolution = match height {
                Some(h) if h >= 2160 => Resolution::Uhd2160,
                Some(h) if h >= 1440 => Resolution::Qhd1440,
                Some(h) if h >= 1080 => Resolution::Fhd1080,
                Some(h) if h >= 720 => Resolution::Hd720,
                Some(h) if h >= 480 => Resolution::Sd480,
                Some(h) if h >= 360 => Resolution::Sd360,
                Some(h) if h > 0 => Resolution::Custom(h),
                _ => {
                    if bandwidth >= 3_500_000 {
                        Resolution::Fhd1080
                    } else if bandwidth >= 1_500_000 {
                        Resolution::Hd720
                    } else if bandwidth >= 600_000 {
                        Resolution::Sd480
                    } else if bandwidth > 0 {
                        Resolution::Sd360
                    } else {
                        template.resolution
                    }
                }
            };

            let mut variant = template.clone();
            variant.url = abs_uri;
            variant.resolution = resolution;
            variant.server_name = if template.server_name.contains("HLS") {
                template.server_name.clone()
            } else {
                format!("{} HLS", template.server_name)
            };
            variants.push((bandwidth, variant));
        }
    }

    // Higher bandwidth first within same resolution.
    variants.sort_by(|a, b| {
        b.1.resolution
            .height()
            .cmp(&a.1.resolution.height())
            .then_with(|| b.0.cmp(&a.0))
    });

    let mut seen = HashSet::new();
    variants
        .into_iter()
        .map(|(_, s)| s)
        .filter(|s| seen.insert(s.url.clone()))
        .collect()
}

/// Concurrently fetches `.m3u8` master playlists and expands `#EXT-X-STREAM-INF`
/// variants into separate selectable `StreamSource`s (`1080p`, `720p`, `480p`).
pub async fn expand_hls_streams(client: &Client, streams: Vec<StreamSource>) -> Vec<StreamSource> {
    let mut set = JoinSet::new();
    for (idx, stream) in streams.into_iter().enumerate() {
        let is_hls = stream.url.to_ascii_lowercase().contains(".m3u8");
        let client_ref = client.clone();
        set.spawn(async move {
            if !is_hls {
                return (idx, vec![stream]);
            }
            let mut req = client_ref
                .get(&stream.url)
                .header(
                    ACCEPT,
                    "application/vnd.apple.mpegurl, application/x-mpegurl, */*",
                )
                .timeout(Duration::from_secs(4));
            if let Some(ref ref_url) = stream.referer {
                req = req.header(REFERER, ref_url);
            }
            if let Ok(resp) = req.send().await {
                if resp.status().is_success() {
                    let ct = resp
                        .headers()
                        .get(CONTENT_TYPE)
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("")
                        .to_ascii_lowercase();
                    let final_url = resp.url().to_string();
                    if ct.contains("mpegurl")
                        || final_url.to_ascii_lowercase().contains(".m3u8")
                        || is_hls
                    {
                        if let Ok(text) = resp.text().await {
                            let expanded = parse_master_m3u8(&text, &final_url, &stream);
                            if !expanded.is_empty() {
                                return (idx, expanded);
                            }
                        }
                    }
                }
            }
            (idx, vec![stream])
        });
    }

    let mut buckets: Vec<(usize, Vec<StreamSource>)> = Vec::new();
    while let Some(joined) = set.join_next().await {
        if let Ok(entry) = joined {
            buckets.push(entry);
        }
    }
    buckets.sort_by_key(|(idx, _)| *idx);

    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for (_, group) in buckets {
        for s in group {
            if seen.insert(s.url.clone()) {
                out.push(s);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_percent_encoded_arabic_episode_numbers() {
        assert_eq!(
            parse_episode_number("https://eva.animerco.org/episodes/one-piece-%d8%a7%d9%84%d8%ad%d9%84%d9%82%d8%a9-12/"),
            Some(12)
        );
        assert_eq!(
            parse_episode_number("https://ww3.okanime.xyz/episode/one-piece-episode-1071"),
            Some(1071)
        );
    }

    #[test]
    fn unpacks_dean_edwards_packed_js() {
        let packed = r#"eval(function(p,a,c,k,e,d){while(c--)if(k[c])p=p.replace(new RegExp('\\b'+c.toString(a)+'\\b','g'),k[c]);return p}('0.1({2:[{3:"4://5.6/7.8"}]});',9,9,'jwplayer|setup|sources|file|https|cdn|example|video|mp4'.split('|')))"#;
        let unpacked = unpack_packed_js(packed);
        assert!(unpacked.contains("https://cdn.example/video.mp4"));
    }

    #[test]
    fn parses_master_m3u8_variants_relative_and_absolute() {
        let master = r#"#EXTM3U
#EXT-X-VERSION:3
#EXT-X-STREAM-INF:BANDWIDTH=1200000,RESOLUTION=854x480
480p/index.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=2600000,RESOLUTION=1280x720
https://cdn2.example.com/hls/720p/stream.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=5200000,RESOLUTION=1920x1080
1080p/master.m3u8
"#;
        let template = StreamSource {
            url: "https://cdn.example.com/hls/master.m3u8".into(),
            resolution: Resolution::Hd720,
            provider: ProviderId::OkAnime,
            server_name: "okanime-embed".into(),
            referer: Some("https://uqload.is/".into()),
            content_length: None,
            verified: false,
            throughput_kbps: None,
            supports_ranges: false,
        };

        let variants = parse_master_m3u8(master, &template.url, &template);
        assert_eq!(variants.len(), 3);
        assert_eq!(variants[0].resolution, Resolution::Fhd1080);
        assert_eq!(
            variants[0].url,
            "https://cdn.example.com/hls/1080p/master.m3u8"
        );
        assert_eq!(variants[1].resolution, Resolution::Hd720);
        assert_eq!(
            variants[1].url,
            "https://cdn2.example.com/hls/720p/stream.m3u8"
        );
        assert_eq!(variants[2].resolution, Resolution::Sd480);
        assert_eq!(
            variants[2].url,
            "https://cdn.example.com/hls/480p/index.m3u8"
        );
    }

    #[test]
    fn extracts_okru_vkvideo_and_mp4upload_streams() {
        let okru_html = r#"<div data-options="{&quot;videos&quot;:[{&quot;name&quot;:&quot;hd&quot;,&quot;url&quot;:&quot;https://ok.example/720.mp4&quot;},{&quot;name&quot;:&quot;full&quot;,&quot;url&quot;:&quot;https://ok.example/1080.mp4&quot;}]}"></div>"#;
        let ok_streams = extract_streams_from_html(
            okru_html,
            "https://ok.ru/videoembed/123",
            ProviderId::Animerco,
        );
        assert!(
            ok_streams
                .iter()
                .any(|s| s.url == "https://ok.example/1080.mp4"
                    && s.resolution == Resolution::Fhd1080)
        );
        assert!(ok_streams
            .iter()
            .any(|s| s.url == "https://ok.example/720.mp4" && s.resolution == Resolution::Hd720));

        let vk_html = r#"<script>var playerParams = {"url1080":"https:\/\/vk.example\/1080.mp4","url720":"https:\/\/vk.example\/720.mp4"};</script>"#;
        let vk_streams = extract_streams_from_html(
            vk_html,
            "https://vk.com/video_ext.php",
            ProviderId::Animerco,
        );
        assert!(
            vk_streams
                .iter()
                .any(|s| s.url == "https://vk.example/1080.mp4"
                    && s.resolution == Resolution::Fhd1080)
        );

        let mp4_html =
            r#"<script>player.src("https://s1.mp4upload.com:282/d/abc/video.mp4");</script>"#;
        let mp4_streams = extract_streams_with_hint(
            mp4_html,
            "https://www.mp4upload.com/embed-abc.html",
            ProviderId::OkAnime,
            "mp4upload FHD",
            Resolution::Fhd1080,
        );
        assert!(mp4_streams
            .iter()
            .any(|s| s.url == "https://s1.mp4upload.com:282/d/abc/video.mp4"
                && s.resolution == Resolution::Fhd1080));
    }
}
