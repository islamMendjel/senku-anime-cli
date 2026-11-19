use crate::error::Result;
use crate::models::{AnimeResult, Episode, ProviderId, Resolution, StreamSource};
use crate::providers::common::{
    check_response_status, extract_slug, extract_streams_from_html, parse_episode_number,
    resolve_url,
};
use crate::providers::AnimeProvider;
use async_trait::async_trait;
use reqwest::Client;
use scraper::{Html, Selector};
use serde::Deserialize;
use std::collections::{BTreeMap, HashSet};
use std::time::Instant;

pub struct AnimeSlayerProvider {
    client: Client,
    base_url: String,
}

#[derive(Debug, Deserialize)]
struct SlayerSearchEnvelope {
    #[serde(default)]
    data: Vec<SlayerAnimeItem>,
}

#[derive(Debug, Deserialize)]
struct SlayerAnimeItem {
    #[serde(default)]
    anime_id: Option<serde_json::Value>,
    #[serde(default)]
    anime_name: Option<String>,
    #[serde(default)]
    anime_slug: Option<String>,
    #[serde(default)]
    anime_release_year: Option<String>,
    #[serde(default)]
    url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SlayerEpisodesEnvelope {
    #[serde(default)]
    data: Vec<SlayerEpisodeItem>,
}

#[derive(Debug, Deserialize)]
struct SlayerEpisodeItem {
    #[serde(default)]
    episode_number: Option<serde_json::Value>,
    #[serde(default)]
    episode_name: Option<String>,
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    episode_urls: Vec<SlayerStreamItem>,
}

#[derive(Debug, Deserialize)]
struct SlayerStreamItem {
    #[serde(default)]
    episode_url: Option<String>,
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    quality: Option<String>,
    #[serde(default)]
    server: Option<String>,
}

impl AnimeSlayerProvider {
    pub fn new(client: Client) -> Self {
        Self::with_base_url(client, "https://anslayer.com")
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

        let sel = Selector::parse("a[href*='/anime/'], .anime-card a, article.post a")
            .expect("valid animeslayer selector");

        for el in doc.select(&sel) {
            let Some(href) = el.value().attr("href") else {
                continue;
            };
            let Some(abs_url) = resolve_url(&self.base_url, href) else {
                continue;
            };
            if abs_url.contains("/episode") || !seen.insert(abs_url.clone()) {
                continue;
            }
            let title = el
                .value()
                .attr("title")
                .map(str::to_string)
                .unwrap_or_else(|| el.text().collect::<Vec<_>>().join(" "));
            let clean_title = title.split_whitespace().collect::<Vec<_>>().join(" ");
            if clean_title.len() < 2 {
                continue;
            }

            results.push(AnimeResult {
                slug: extract_slug(&abs_url, &clean_title),
                title: clean_title,
                alt_title: None,
                url: abs_url,
                provider: ProviderId::AnimeSlayer,
                year: None,
                episode_count: None,
                latency_ms,
                sources: Vec::new(),
            });
        }

        results
    }

    fn parse_episodes_html(&self, html: &str, anime_url: &str) -> Vec<Episode> {
        let doc = Html::parse_document(html);
        let mut by_num = BTreeMap::new();
        let sel = Selector::parse("a[href*='episode'], a[href*='ep-'], .episodes a")
            .expect("valid slayer episode selector");

        for el in doc.select(&sel) {
            let Some(href) = el.value().attr("href") else {
                continue;
            };
            let Some(abs_url) = resolve_url(anime_url, href) else {
                continue;
            };
            let label = el.text().collect::<Vec<_>>().join(" ");
            if let Some(number) =
                parse_episode_number(&abs_url).or_else(|| parse_episode_number(&label))
            {
                by_num.entry(number).or_insert_with(|| Episode {
                    number,
                    title: Some(label.split_whitespace().collect::<Vec<_>>().join(" "))
                        .filter(|s| !s.is_empty()),
                    url: abs_url,
                    provider: ProviderId::AnimeSlayer,
                    sources: Vec::new(),
                });
            }
        }

        by_num.into_values().collect()
    }
}

#[async_trait]
impl AnimeProvider for AnimeSlayerProvider {
    fn id(&self) -> ProviderId {
        ProviderId::AnimeSlayer
    }

    fn base_url(&self) -> &str {
        &self.base_url
    }

    async fn search(&self, query: &str) -> Result<Vec<AnimeResult>> {
        let started = Instant::now();
        let search_url = format!(
            "{}/anime/public/animes?search={}",
            self.base_url,
            urlencoding::encode(query)
        );
        let resp = self
            .client
            .get(&search_url)
            .header("Accept", "application/json, text/html;q=0.9")
            .header("Referer", format!("{}/", self.base_url))
            .send()
            .await?;

        check_response_status(self.id(), &resp)?;

        let body = resp.text().await?;
        let latency_ms = started.elapsed().as_millis() as u64;

        if let Ok(json) = serde_json::from_str::<SlayerSearchEnvelope>(&body) {
            let mapped = json
                .data
                .into_iter()
                .filter_map(|item| {
                    let title = item.anime_name?.trim().to_string();
                    if title.is_empty() {
                        return None;
                    }
                    let id_str = item
                        .anime_id
                        .map(|v| v.to_string().trim_matches('"').to_string())
                        .unwrap_or_default();
                    let url = item
                        .url
                        .and_then(|u| resolve_url(&self.base_url, &u))
                        .unwrap_or_else(|| {
                            if id_str.is_empty() {
                                format!("{}/anime/{}", self.base_url, urlencoding::encode(&title))
                            } else {
                                format!("{}/anime/{}", self.base_url, id_str)
                            }
                        });
                    let slug = item
                        .anime_slug
                        .filter(|s| !s.trim().is_empty())
                        .unwrap_or_else(|| extract_slug(&url, &title));
                    Some(AnimeResult {
                        slug,
                        title,
                        alt_title: None,
                        url,
                        provider: ProviderId::AnimeSlayer,
                        year: item.anime_release_year,
                        episode_count: None,
                        latency_ms,
                        sources: Vec::new(),
                    })
                })
                .collect::<Vec<_>>();
            if !mapped.is_empty() {
                return Ok(mapped);
            }
        }

        Ok(self.parse_search_html(&body, latency_ms))
    }

    async fn get_episodes(&self, anime: &AnimeResult) -> Result<Vec<Episode>> {
        let resp = self
            .client
            .get(&anime.url)
            .header("Accept", "application/json, text/html;q=0.9")
            .header("Referer", format!("{}/", self.base_url))
            .send()
            .await?;

        check_response_status(self.id(), &resp)?;

        let body = resp.text().await?;
        if let Ok(json) = serde_json::from_str::<SlayerEpisodesEnvelope>(&body) {
            let mut eps: Vec<Episode> = json
                .data
                .into_iter()
                .filter_map(|item| {
                    let number = item
                        .episode_number
                        .and_then(|v| v.as_u64().or_else(|| v.as_str()?.parse::<u64>().ok()))
                        .and_then(|n| u32::try_from(n).ok())?;
                    let ep_url = item
                        .url
                        .and_then(|u| resolve_url(&anime.url, &u))
                        .unwrap_or_else(|| {
                            format!("{}/episode/{}", anime.url.trim_end_matches('/'), number)
                        });
                    Some(Episode {
                        number,
                        title: item.episode_name,
                        url: ep_url,
                        provider: ProviderId::AnimeSlayer,
                        sources: Vec::new(),
                    })
                })
                .collect();
            eps.sort_by_key(|e| e.number);
            if !eps.is_empty() {
                return Ok(eps);
            }
        }

        Ok(self.parse_episodes_html(&body, &anime.url))
    }

    async fn get_streams(&self, episode: &Episode) -> Result<Vec<StreamSource>> {
        let resp = self
            .client
            .get(&episode.url)
            .header("Accept", "application/json, text/html;q=0.9")
            .header("Referer", format!("{}/", self.base_url))
            .send()
            .await?;

        check_response_status(self.id(), &resp)?;

        let body = resp.text().await?;
        if let Ok(item) = serde_json::from_str::<SlayerEpisodeItem>(&body) {
            let streams = item
                .episode_urls
                .into_iter()
                .filter_map(|s| {
                    let raw = s.episode_url.or(s.url)?;
                    let abs = resolve_url(&episode.url, &raw)?;
                    let res = s
                        .quality
                        .as_deref()
                        .map(Resolution::parse_from_label)
                        .filter(|r| *r != Resolution::Unknown)
                        .unwrap_or_else(|| Resolution::parse_from_label(&abs));
                    Some(StreamSource {
                        url: abs,
                        resolution: res,
                        provider: ProviderId::AnimeSlayer,
                        server_name: s.server.unwrap_or_else(|| "slayer-cdn".to_string()),
                        referer: Some(episode.url.clone()),
                        content_length: None,
                        verified: false,
                        throughput_kbps: None,
                        supports_ranges: false,
                    })
                })
                .collect::<Vec<_>>();
            if !streams.is_empty() {
                return Ok(streams);
            }
        }

        Ok(extract_streams_from_html(
            &body,
            &episode.url,
            ProviderId::AnimeSlayer,
        ))
    }
}
