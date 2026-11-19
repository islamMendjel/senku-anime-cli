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

pub struct AnimeifyProvider {
    client: Client,
    base_url: String,
}

#[derive(Debug, Deserialize)]
struct AnimeifySearchResponse {
    #[serde(default)]
    results: Vec<AnimeifyEntry>,
}

#[derive(Debug, Deserialize)]
struct AnimeifyEntry {
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    slug: Option<String>,
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    year: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AnimeifyStreamPayload {
    #[serde(default)]
    sources: Vec<AnimeifySource>,
}

#[derive(Debug, Deserialize)]
struct AnimeifySource {
    file: String,
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    server: Option<String>,
}

impl AnimeifyProvider {
    pub fn new(client: Client) -> Self {
        Self::with_base_url(client, "https://animeify.net")
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

        let sel = Selector::parse("a[href*='/anime/'], .anime-item a, .search-results a")
            .expect("valid animeify search selector");

        for el in doc.select(&sel) {
            let Some(href) = el.value().attr("href") else {
                continue;
            };
            let Some(abs_url) = resolve_url(&self.base_url, href) else {
                continue;
            };
            if abs_url.contains("/watch/")
                || abs_url.contains("/episode")
                || !seen.insert(abs_url.clone())
            {
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
                provider: ProviderId::Animeify,
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

        let sel = Selector::parse("a[href*='/watch/'], a[href*='/episode/'], .ep-list a")
            .expect("valid animeify episode selector");

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
                    provider: ProviderId::Animeify,
                    sources: Vec::new(),
                });
            }
        }

        by_num.into_values().collect()
    }
}

#[async_trait]
impl AnimeProvider for AnimeifyProvider {
    fn id(&self) -> ProviderId {
        ProviderId::Animeify
    }

    fn base_url(&self) -> &str {
        &self.base_url
    }

    async fn search(&self, query: &str) -> Result<Vec<AnimeResult>> {
        let started = Instant::now();
        let search_url = format!("{}/?s={}", self.base_url, urlencoding::encode(query));
        let resp = self
            .client
            .get(&search_url)
            .header("Accept", "text/html, application/json;q=0.9")
            .header("Referer", format!("{}/", self.base_url))
            .send()
            .await?;

        check_response_status(self.id(), &resp)?;

        let body = resp.text().await?;
        let latency_ms = started.elapsed().as_millis() as u64;

        if let Ok(json) = serde_json::from_str::<AnimeifySearchResponse>(&body) {
            let mapped = json
                .results
                .into_iter()
                .filter_map(|e| {
                    let title = e.title?.trim().to_string();
                    let url = e.url.and_then(|u| resolve_url(&self.base_url, &u))?;
                    let slug = e
                        .slug
                        .filter(|s| !s.trim().is_empty())
                        .unwrap_or_else(|| extract_slug(&url, &title));
                    Some(AnimeResult {
                        slug,
                        title,
                        alt_title: None,
                        url,
                        provider: ProviderId::Animeify,
                        year: e.year,
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
            .header("Referer", format!("{}/", self.base_url))
            .send()
            .await?;

        check_response_status(self.id(), &resp)?;

        let body = resp.text().await?;
        Ok(self.parse_episodes_html(&body, &anime.url))
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
        if let Ok(payload) = serde_json::from_str::<AnimeifyStreamPayload>(&body) {
            let streams = payload
                .sources
                .into_iter()
                .filter_map(|src| {
                    let abs = resolve_url(&episode.url, &src.file)?;
                    let res = src
                        .label
                        .as_deref()
                        .map(Resolution::parse_from_label)
                        .filter(|r| *r != Resolution::Unknown)
                        .unwrap_or_else(|| Resolution::parse_from_label(&abs));
                    Some(StreamSource {
                        url: abs,
                        resolution: res,
                        provider: ProviderId::Animeify,
                        server_name: src.server.unwrap_or_else(|| "animeify-cdn".to_string()),
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
            ProviderId::Animeify,
        ))
    }
}
