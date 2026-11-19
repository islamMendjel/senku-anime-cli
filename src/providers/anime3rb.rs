use crate::error::Result;
use crate::models::{AnimeResult, Episode, ProviderId, StreamSource};
use crate::providers::common::{
    check_response_status, extract_slug, extract_streams_from_html, parse_episode_number,
    resolve_url,
};
use crate::providers::AnimeProvider;
use async_trait::async_trait;
use reqwest::Client;
use scraper::{Html, Selector};
use std::collections::{BTreeMap, HashSet};
use std::time::Instant;

pub struct Anime3rbProvider {
    client: Client,
    base_url: String,
}

impl Anime3rbProvider {
    pub fn new(client: Client) -> Self {
        Self::with_base_url(client, "https://anime3rb.com")
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

        // Anime3rb renders title cards with links to /titles/<slug> or /anime/<slug>.
        let card_sel =
            Selector::parse("a[href*='/titles/'], a[href*='/anime/'], div.title-card a, article a")
                .expect("valid anime3rb search selector");
        let title_sel = Selector::parse("h2, h3, .title-name, .anime-title, span")
            .expect("valid title selector");

        for el in doc.select(&card_sel) {
            let Some(href) = el.value().attr("href") else {
                continue;
            };
            let Some(abs_url) = resolve_url(&self.base_url, href) else {
                continue;
            };
            if abs_url.contains("/episode/") || !seen.insert(abs_url.clone()) {
                continue;
            }

            let title = el
                .select(&title_sel)
                .next()
                .map(|t| t.text().collect::<Vec<_>>().join(" ").trim().to_string())
                .filter(|t| !t.is_empty())
                .or_else(|| el.value().attr("title").map(|s| s.trim().to_string()))
                .unwrap_or_else(|| el.text().collect::<Vec<_>>().join(" ").trim().to_string());

            let clean_title = title.split_whitespace().collect::<Vec<_>>().join(" ");
            if clean_title.len() < 2 {
                continue;
            }

            let slug = extract_slug(&abs_url, &clean_title);
            results.push(AnimeResult {
                slug,
                title: clean_title,
                alt_title: None,
                url: abs_url,
                provider: ProviderId::Anime3rb,
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
        let mut by_num: BTreeMap<u32, Episode> = BTreeMap::new();

        let ep_sel = Selector::parse(
            "a[href*='/episode/'], a[href*='/watch/'], .episodes-list a, ul.episodes a",
        )
        .expect("valid episode selector");

        for el in doc.select(&ep_sel) {
            let Some(href) = el.value().attr("href") else {
                continue;
            };
            let Some(abs_url) = resolve_url(anime_url, href) else {
                continue;
            };
            let label = el.text().collect::<Vec<_>>().join(" ");
            let num = parse_episode_number(&abs_url).or_else(|| parse_episode_number(&label));
            if let Some(number) = num {
                by_num.entry(number).or_insert_with(|| Episode {
                    number,
                    title: Some(label.split_whitespace().collect::<Vec<_>>().join(" "))
                        .filter(|s| !s.is_empty()),
                    url: abs_url,
                    provider: ProviderId::Anime3rb,
                    sources: Vec::new(),
                });
            }
        }

        by_num.into_values().collect()
    }
}

#[async_trait]
impl AnimeProvider for Anime3rbProvider {
    fn id(&self) -> ProviderId {
        ProviderId::Anime3rb
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
        let mut streams = extract_streams_from_html(&body, &episode.url, ProviderId::Anime3rb);

        // Follow any embedded player iframe on Anime3rb if direct streams weren't on the main page.
        let iframe_urls: Vec<String> = {
            let doc = Html::parse_document(&body);
            let iframe_sel = Selector::parse("iframe[src]").expect("valid iframe selector");
            doc.select(&iframe_sel)
                .filter_map(|el| el.value().attr("src"))
                .filter_map(|src| resolve_url(&episode.url, src))
                .take(3)
                .collect()
        };

        for iframe_url in iframe_urls {
            if let Ok(embed_resp) = self
                .client
                .get(&iframe_url)
                .header("Referer", &episode.url)
                .send()
                .await
            {
                if embed_resp.status().is_success() {
                    if let Ok(embed_body) = embed_resp.text().await {
                        streams.extend(extract_streams_from_html(
                            &embed_body,
                            &iframe_url,
                            ProviderId::Anime3rb,
                        ));
                    }
                }
            }
        }

        Ok(streams)
    }
}
