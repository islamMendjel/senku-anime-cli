use crate::error::Result;
use crate::models::{AnimeResult, Episode, ProviderId, Resolution, StreamSource};
use crate::providers::common::{
    check_response_status, expand_hls_streams, extract_slug, extract_streams_with_hint,
    parse_episode_number, resolve_url,
};
use crate::providers::AnimeProvider;
use async_trait::async_trait;
use regex::Regex;
use reqwest::Client;
use scraper::{Html, Selector};
use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

const PER_EMBED_TIMEOUT: Duration = Duration::from_secs(3);
const OVERALL_EXTRACT_DEADLINE: Duration = Duration::from_secs(4);
const MAX_CONCURRENT_EMBEDS: usize = 8;

pub struct OkAnimeProvider {
    client: Client,
    base_url: String,
}

impl OkAnimeProvider {
    pub fn new(client: Client) -> Self {
        Self::with_base_url(client, "https://ww3.okanime.xyz")
    }

    pub fn with_base_url(client: Client, base_url: impl Into<String>) -> Self {
        Self {
            client,
            base_url: base_url.into().trim_end_matches('/').to_string(),
        }
    }

    fn parse_search_html(&self, html: &str, latency_ms: u64) -> Vec<AnimeResult> {
        let doc = Html::parse_document(html);
        let mut results = Vec::new();
        let mut seen = HashSet::new();

        let card_sel = Selector::parse(".anime-card").expect("valid anime-card selector");
        let link_sel = Selector::parse("a[href*='/anime/']").expect("valid okanime link selector");
        let title_sel = Selector::parse("h3, .anime-details h3, span[dir='auto']")
            .expect("valid okanime title selector");

        for card in doc.select(&card_sel) {
            let Some(link_el) = card.select(&link_sel).next() else {
                continue;
            };
            let Some(href) = link_el.value().attr("href") else {
                continue;
            };
            let Some(abs_url) = resolve_url(&self.base_url, href) else {
                continue;
            };
            if !seen.insert(abs_url.clone()) {
                continue;
            }

            let title = card
                .select(&title_sel)
                .next()
                .map(|t| t.text().collect::<Vec<_>>().join(" "))
                .unwrap_or_default();
            let clean_title = title.split_whitespace().collect::<Vec<_>>().join(" ");
            if clean_title.len() < 2 {
                continue;
            }

            results.push(AnimeResult {
                slug: extract_slug(&abs_url, &clean_title),
                title: clean_title,
                alt_title: None,
                url: abs_url,
                provider: ProviderId::OkAnime,
                year: None,
                episode_count: None,
                latency_ms,
                sources: Vec::new(),
            });
        }

        results
    }

    fn parse_episodes_html(&self, html: &str, anime: &AnimeResult) -> Vec<Episode> {
        let doc = Html::parse_document(html);
        let mut by_num = BTreeMap::new();

        let ep_sel = Selector::parse(
            "[data-track-section='anime_episodes_compact'] a[href*='/episode/'], a[data-track-control='episode_list'][href*='/episode/'], a[href*='/episode/']",
        )
        .expect("valid okanime episode selector");

        let slug_prefix = format!("{}-episode-", anime.slug.to_ascii_lowercase());

        for el in doc.select(&ep_sel) {
            let Some(href) = el.value().attr("href") else {
                continue;
            };
            let Some(abs_url) = resolve_url(&anime.url, href) else {
                continue;
            };
            let is_sidebar = el
                .value()
                .attr("class")
                .map(|c| c.contains("sidebar-schedule-card"))
                .unwrap_or(false);
            if is_sidebar {
                continue;
            }

            // Ensure episode URL belongs to this anime when matching fallback `a[href*='/episode/']`.
            let is_explicit_ep_list = el.value().attr("data-track-control") == Some("episode_list");
            if !is_explicit_ep_list && !abs_url.to_ascii_lowercase().contains(&slug_prefix) {
                continue;
            }

            let num = el
                .value()
                .attr("data-umami-event-episode_number")
                .and_then(|v| v.parse::<u32>().ok())
                .or_else(|| parse_episode_number(&abs_url));

            if let Some(number) = num.filter(|&n| n > 0) {
                let label = el.text().collect::<Vec<_>>().join(" ");
                by_num.entry(number).or_insert_with(|| Episode {
                    number,
                    title: Some(label.split_whitespace().collect::<Vec<_>>().join(" "))
                        .filter(|s| !s.is_empty()),
                    url: abs_url,
                    provider: ProviderId::OkAnime,
                    sources: Vec::new(),
                });
            }
        }

        by_num.into_values().collect()
    }

    /// Extracts deduplicated embed targets `(embed_url, server_label, res_hint)` from an
    /// OkAnime episode page, preserving `FHD`/`HD`/`SD` quality badges, unescaping JS
    /// unicode entities (`\u0026`), and normalizing blocked filehost URLs (e.g. `uqload /e/`).
    fn extract_embed_targets(
        &self,
        body: &str,
        episode_url: &str,
    ) -> Vec<(String, String, Resolution)> {
        static SERVER_RE: OnceLock<Regex> = OnceLock::new();
        let srv_re = SERVER_RE.get_or_init(|| {
            Regex::new(r#"setServer\('([^']+)'\)"#).expect("valid setServer regex")
        });

        let mut embed_targets: Vec<(String, String, Resolution)> = Vec::new();
        let mut seen = HashSet::new();

        let doc = Html::parse_document(body);

        // 1. Player server buttons (`<a data-track-control="source_select" @click="setServer('...')">`)
        let btn_sel = Selector::parse("a[data-track-control='source_select'], a.ep-link")
            .expect("valid okanime source button selector");
        for el in doc.select(&btn_sel) {
            let text = el.text().collect::<Vec<_>>().join(" ");
            let clean_label = text.split_whitespace().collect::<Vec<_>>().join(" ");
            let res_hint = Resolution::parse_from_label(&clean_label);

            if let Some(click_attr) = el.value().attr("@click") {
                if let Some(cap) = srv_re.captures(click_attr) {
                    let unescaped = unescape_js_url(&cap[1]);
                    if let Some(abs) = resolve_url(episode_url, &unescaped) {
                        let normalized = normalize_embed_url(&abs);
                        if seen.insert(normalized.clone()) {
                            let srv_attr = el.value().attr("data-server").unwrap_or("");
                            let srv_name = if !clean_label.is_empty() {
                                clean_label
                            } else if !srv_attr.is_empty() {
                                srv_attr.to_string()
                            } else {
                                infer_host_label(&normalized)
                            };
                            embed_targets.push((normalized, srv_name, res_hint));
                        }
                    }
                    continue;
                }
            }

            // 2. Download tab links (`<a class="ep-link" href="..."><span>HD</span>mp4upload.com</a>`)
            if let Some(href) = el.value().attr("href") {
                if let Some(embed_url) = normalize_download_host_to_embed(href) {
                    let normalized = normalize_embed_url(&embed_url);
                    if seen.insert(normalized.clone()) {
                        let srv_name = if clean_label.is_empty() {
                            infer_host_label(&normalized)
                        } else {
                            clean_label
                        };
                        embed_targets.push((normalized, srv_name, res_hint));
                    }
                }
            }
        }

        // 3. Fallback regex pass for any `setServer('...')` outside standard buttons.
        for cap in srv_re.captures_iter(body) {
            let unescaped = unescape_js_url(&cap[1]);
            if let Some(abs) = resolve_url(episode_url, &unescaped) {
                let normalized = normalize_embed_url(&abs);
                if seen.insert(normalized.clone()) {
                    let label = infer_host_label(&normalized);
                    embed_targets.push((normalized, label, Resolution::Unknown));
                }
            }
        }

        embed_targets
    }
}

#[async_trait]
impl AnimeProvider for OkAnimeProvider {
    fn id(&self) -> ProviderId {
        ProviderId::OkAnime
    }

    fn base_url(&self) -> &str {
        &self.base_url
    }

    async fn search(&self, query: &str) -> Result<Vec<AnimeResult>> {
        let started = Instant::now();
        let search_url = format!("{}/search?q={}", self.base_url, urlencoding::encode(query));
        let resp = self
            .client
            .get(&search_url)
            .header("Referer", format!("{}/", self.base_url))
            .send()
            .await?;

        check_response_status(self.id(), &resp)?;
        let body = resp.text().await?;
        let latency_ms = started.elapsed().as_millis() as u64;
        Ok(self.parse_search_html(&body, latency_ms))
    }

    async fn get_episodes(&self, anime: &AnimeResult) -> Result<Vec<Episode>> {
        let resp = self
            .client
            .get(&anime.url)
            .header("Referer", format!("{}/", self.base_url))
            .send()
            .await?;
        check_response_status(self.id(), &resp)?;
        let body = resp.text().await?;
        Ok(self.parse_episodes_html(&body, anime))
    }

    async fn get_streams(&self, episode: &Episode) -> Result<Vec<StreamSource>> {
        let resp = self
            .client
            .get(&episode.url)
            .header("Referer", format!("{}/", self.base_url))
            .send()
            .await?;
        check_response_status(self.id(), &resp)?;
        let body = resp.text().await?;

        let mut streams = extract_streams_with_hint(
            &body,
            &episode.url,
            ProviderId::OkAnime,
            "okanime",
            Resolution::Unknown,
        );

        let embed_targets = self.extract_embed_targets(&body, &episode.url);

        let sem = Arc::new(Semaphore::new(MAX_CONCURRENT_EMBEDS));
        let mut set = JoinSet::new();
        for (embed_url, server_label, res_hint) in embed_targets {
            let client = self.client.clone();
            let referer = format!("{}/", self.base_url);
            let sem_ref = Arc::clone(&sem);
            set.spawn(async move {
                let mut found = Vec::new();
                let Ok(_permit) = sem_ref.acquire().await else {
                    return found;
                };
                if let Ok(Ok(emb_resp)) = tokio::time::timeout(
                    PER_EMBED_TIMEOUT,
                    client
                        .get(&embed_url)
                        .header("Referer", referer)
                        .timeout(PER_EMBED_TIMEOUT)
                        .send(),
                )
                .await
                {
                    if emb_resp.status().is_success() {
                        if let Ok(Ok(emb_html)) =
                            tokio::time::timeout(PER_EMBED_TIMEOUT, emb_resp.text()).await
                        {
                            found.extend(extract_streams_with_hint(
                                &emb_html,
                                &embed_url,
                                ProviderId::OkAnime,
                                &server_label,
                                res_hint,
                            ));
                        }
                    }
                }
                found
            });
        }

        let deadline = tokio::time::sleep(OVERALL_EXTRACT_DEADLINE);
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                _ = &mut deadline => {
                    set.abort_all();
                    break;
                }
                joined = set.join_next() => {
                    match joined {
                        Some(Ok(mut found)) => streams.append(&mut found),
                        Some(Err(_)) => {}
                        None => break,
                    }
                }
            }
        }

        Ok(expand_hls_streams(&self.client, streams).await)
    }
}

fn unescape_js_url(raw: &str) -> String {
    raw.replace("\\u0026", "&")
        .replace("\\/", "/")
        .replace("&amp;", "&")
}

/// Normalizes multi-format embed URLs into reliable player endpoints:
/// - `uqload.*/e/<id>?...` / `uqload.*/<id>.html` -> `https://<host>/embed-<id>.html`
/// - `vidmoly.*/w/<id>` -> `https://<host>/embed-<id>.html`
pub fn normalize_embed_url(raw: &str) -> String {
    let Ok(parsed) = url::Url::parse(raw) else {
        return raw.to_string();
    };
    let Some(host) = parsed.host_str().map(str::to_ascii_lowercase) else {
        return raw.to_string();
    };
    let path = parsed.path().trim_matches('/');

    if host.contains("uqload.") && !path.is_empty() {
        let id = path
            .strip_prefix("e/")
            .or_else(|| path.strip_prefix("embed-"))
            .unwrap_or(path)
            .split('/')
            .next()
            .unwrap_or(path);
        let id = id.strip_suffix(".html").unwrap_or(id);
        if !id.is_empty() {
            return format!("https://{host}/embed-{id}.html");
        }
    }

    if host.contains("vidmoly.") && !path.is_empty() {
        let id = path
            .strip_prefix("w/")
            .or_else(|| path.strip_prefix("embed-"))
            .unwrap_or(path)
            .split('/')
            .next()
            .unwrap_or(path);
        let id = id.strip_suffix(".html").unwrap_or(id);
        if !id.is_empty() {
            return format!("https://{host}/embed-{id}.html");
        }
    }

    raw.to_string()
}

fn infer_host_label(url: &str) -> String {
    let lower = url.to_ascii_lowercase();
    if lower.contains("uqload") {
        "uqload".to_string()
    } else if lower.contains("mp4upload") {
        "mp4upload".to_string()
    } else if lower.contains("yourupload") {
        "yourupload".to_string()
    } else if lower.contains("sendvid") {
        "sendvid".to_string()
    } else if lower.contains("ok.ru") {
        "ok.ru".to_string()
    } else if lower.contains("vk.com") || lower.contains("vkvideo") {
        "vkvideo".to_string()
    } else if lower.contains("streamwish") || lower.contains("filelions") {
        "streamwish".to_string()
    } else if lower.contains("vidmoly") {
        "vidmoly".to_string()
    } else if lower.contains("videa.hu") {
        "videa".to_string()
    } else if lower.contains("megamax") {
        "megamax".to_string()
    } else {
        "okanime-embed".to_string()
    }
}

fn normalize_download_host_to_embed(raw_url: &str) -> Option<String> {
    let parsed = url::Url::parse(raw_url).ok()?;
    let host = parsed.host_str()?.to_ascii_lowercase();
    let path = parsed.path().trim_matches('/');

    if host.contains("mp4upload.com") && !path.is_empty() {
        let id = path.strip_prefix("embed-").unwrap_or(path);
        let id = id.strip_suffix(".html").unwrap_or(id);
        return Some(format!("https://www.mp4upload.com/embed-{id}.html"));
    }
    if host.contains("sendvid.com")
        || host.contains("yourupload.com")
        || host.contains("uqload.")
        || host.contains("ok.ru")
        || host.contains("streamwish")
        || host.contains("filelions")
        || host.contains("vidmoly")
        || host.contains("vk.com")
        || host.contains("vkvideo")
    {
        return Some(normalize_embed_url(raw_url));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::build_http_client;
    use std::time::Duration;

    #[test]
    fn parses_okanime_cards_and_filters_sidebar_episodes() {
        let client = build_http_client(Duration::from_secs(5)).unwrap();
        let prov = OkAnimeProvider::new(client);

        let search_html = r#"
        <div class="anime-card anime-hover">
          <a href="https://ww3.okanime.xyz/anime/one-piece" class="clickable"></a>
          <a href="https://ww3.okanime.xyz/anime/one-piece" class="anime-details"><h3>One Piece</h3></a>
        </div>"#;
        let results = prov.parse_search_html(search_html, 85);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].title, "One Piece");
        assert_eq!(results[0].slug, "one-piece");

        let anime_html = r#"
        <a href="https://ww3.okanime.xyz/episode/kanata-kara-episode-7" class="sidebar-schedule-card">Kanata Kara</a>
        <div data-track-section="anime_episodes_compact">
          <a href="https://ww3.okanime.xyz/episode/one-piece-episode-1" data-track-control="episode_list" data-umami-event-episode_number="1">Episode 1</a>
          <a href="https://ww3.okanime.xyz/episode/one-piece-episode-2" data-track-control="episode_list" data-umami-event-episode_number="2">Episode 2</a>
        </div>"#;
        let eps = prov.parse_episodes_html(anime_html, &results[0]);
        assert_eq!(eps.len(), 2);
        assert_eq!(eps[0].number, 1);
        assert_eq!(eps[1].number, 2);
    }

    #[test]
    fn extracts_multi_host_targets_normalizes_uqload_and_unescapes_vkvideo() {
        let client = build_http_client(Duration::from_secs(5)).unwrap();
        let prov = OkAnimeProvider::new(client);

        let ep_html = r#"
        <a class="no-link ep-link" data-server="mp4upload" @click="setServer('https://mp4upload.com/embed-vtblj26uaw2s.html')" data-track-control="source_select">
          <span>FHD</span>mp4upload
        </a>
        <a class="no-link ep-link" data-server="vk" @click="setServer('https://vkvideo.ru/video_ext.php?oid=-240880334\u0026id=456239730\u0026hash=ef54ac98eb0c9f2b\u0026hd=3')" data-track-control="source_select">
          <span>HD</span>vk
        </a>
        <a class="no-link ep-link" data-server="sendvid" @click="setServer('https://sendvid.com/embed/4257su6m.html')" data-track-control="source_select">
          <span>HD</span>sendvid
        </a>
        <a class="no-link ep-link" data-server="uqload" @click="setServer('https://uqload.vc/e/2ciamyryocv5?e15.uqload.vc/i/02/06283/2ciamyryocv5.html')" data-track-control="source_select">
          <span>HD</span>uqload
        </a>
        "#;

        let targets = prov.extract_embed_targets(
            ep_html,
            "https://ww3.okanime.xyz/episode/dragon-ball-super-beerus-episode-6",
        );
        assert_eq!(targets.len(), 4);
        assert_eq!(
            targets[0].0,
            "https://mp4upload.com/embed-vtblj26uaw2s.html"
        );
        assert_eq!(targets[0].2, Resolution::Fhd1080);

        assert_eq!(
            targets[1].0,
            "https://vkvideo.ru/video_ext.php?oid=-240880334&id=456239730&hash=ef54ac98eb0c9f2b&hd=3"
        );
        assert_eq!(targets[1].2, Resolution::Hd720);

        assert_eq!(targets[2].0, "https://sendvid.com/embed/4257su6m.html");
        assert_eq!(targets[2].2, Resolution::Hd720);

        // uqload /e/<id>?... normalized to /embed-<id>.html to avoid Cloudflare 403
        assert_eq!(targets[3].0, "https://uqload.vc/embed-2ciamyryocv5.html");
        assert_eq!(targets[3].2, Resolution::Hd720);
    }
}
