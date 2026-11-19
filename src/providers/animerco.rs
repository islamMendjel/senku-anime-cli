use crate::error::{Result, SenkuError};
use crate::models::{AnimeResult, Episode, ProviderId, Resolution, StreamSource};
use crate::providers::common::{
    check_response_status, extract_slug, extract_streams_with_hint, parse_episode_number,
    resolve_url,
};
use crate::providers::AnimeProvider;
use async_trait::async_trait;
use reqwest::Client;
use scraper::{Html, Selector};
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

const PER_EMBED_TIMEOUT: Duration = Duration::from_secs(3);
const OVERALL_EXTRACT_DEADLINE: Duration = Duration::from_secs(4);
const MAX_CONCURRENT_EMBEDS: usize = 8;

pub struct AnimercoProvider {
    client: Client,
    base_url: String,
}

impl AnimercoProvider {
    pub fn new(client: Client) -> Self {
        Self::with_base_url(client, "https://eva.animerco.org")
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

        let card_sel = Selector::parse(".search-card").expect("valid search-card selector");
        let link_sel = Selector::parse(
            "a[href*='/animes/'], a[href*='/movies/'], a[href*='/seasons/'], a[href*='/specials/'], a[href*='/ova/'], a[href]",
        )
        .expect("valid animerco link selector");
        let title_sel = Selector::parse("h3").expect("valid h3 selector");

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
                .map(|h| h.text().collect::<Vec<_>>().join(" "))
                .or_else(|| link_el.value().attr("title").map(str::to_string))
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
                provider: ProviderId::Animerco,
                year: None,
                episode_count: None,
                latency_ms,
                sources: Vec::new(),
            });
        }

        results
    }

    fn extract_season_urls(&self, html: &str, page_url: &str) -> Vec<String> {
        let doc = Html::parse_document(html);
        let sel = Selector::parse(
            ".media-seasons ul.episodes-lists a[href*='/seasons/'], a[href*='/seasons/']",
        )
        .expect("valid season selector");
        let mut seen = HashSet::new();
        let mut seasons = Vec::new();
        for el in doc.select(&sel) {
            if let Some(href) = el.value().attr("href") {
                if let Some(abs) = resolve_url(page_url, href) {
                    if abs.ends_with("/seasons/") {
                        continue;
                    }
                    if seen.insert(abs.clone()) {
                        seasons.push(abs);
                    }
                }
            }
        }
        seasons
    }

    fn parse_episodes_from_html(&self, html: &str, page_url: &str) -> Vec<Episode> {
        let doc = Html::parse_document(html);
        let mut by_num = BTreeMap::new();

        let sel = Selector::parse("ul.episodes-lists a[href*='/episodes/'], .episodes-lists a[href*='/episodes/'], a[href*='/episodes/']")
            .expect("valid animerco episode selector");

        for el in doc.select(&sel) {
            let Some(href) = el.value().attr("href") else {
                continue;
            };
            let Some(abs_url) = resolve_url(page_url, href) else {
                continue;
            };
            if abs_url.trim_end_matches('/').ends_with("/episodes") {
                continue;
            }
            let label = el
                .value()
                .attr("title")
                .map(str::to_string)
                .unwrap_or_else(|| el.text().collect::<Vec<_>>().join(" "));
            if let Some(number) =
                parse_episode_number(&abs_url).or_else(|| parse_episode_number(&label))
            {
                by_num.entry(number).or_insert_with(|| Episode {
                    number,
                    title: Some(label.split_whitespace().collect::<Vec<_>>().join(" "))
                        .filter(|s| !s.is_empty()),
                    url: abs_url,
                    provider: ProviderId::Animerco,
                    sources: Vec::new(),
                });
            }
        }

        by_num.into_values().collect()
    }
}

#[async_trait]
impl AnimeProvider for AnimercoProvider {
    fn id(&self) -> ProviderId {
        ProviderId::Animerco
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

        let mut by_num = BTreeMap::new();
        for ep in self.parse_episodes_from_html(&body, &anime.url) {
            by_num.insert(ep.number, ep);
        }

        if by_num.is_empty() {
            let seasons = self.extract_season_urls(&body, &anime.url);
            for season_url in seasons.into_iter().take(4) {
                if let Ok(s_resp) = self
                    .client
                    .get(&season_url)
                    .header("Referer", &anime.url)
                    .send()
                    .await
                {
                    if s_resp.status().is_success() {
                        if let Ok(s_body) = s_resp.text().await {
                            for ep in self.parse_episodes_from_html(&s_body, &season_url) {
                                by_num.entry(ep.number).or_insert(ep);
                            }
                        }
                    }
                }
            }
        }

        // Movie entries on Animerco host the player directly on the movie page.
        if by_num.is_empty() && anime.url.contains("/movies/") {
            by_num.insert(
                1,
                Episode {
                    number: 1,
                    title: Some(anime.title.clone()),
                    url: anime.url.clone(),
                    provider: ProviderId::Animerco,
                    sources: Vec::new(),
                },
            );
        }

        Ok(by_num.into_values().collect())
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
            ProviderId::Animerco,
            "animerco",
            Resolution::Unknown,
        );

        // Extract Animerco signed player option URLs (`data-embed-url`).
        let options: Vec<(String, String, Resolution)> = {
            let doc = Html::parse_document(&body);
            let opt_sel = Selector::parse(
                "ul.server-list a.option[data-embed-url], a.option[data-embed-url]",
            )
            .expect("valid animerco option selector");
            let srv_sel = Selector::parse(".server").expect("valid server name selector");

            doc.select(&opt_sel)
                .filter_map(|el| {
                    let embed_raw = el.value().attr("data-embed-url")?;
                    let embed_url = resolve_url(&episode.url, embed_raw)?;
                    let label = el
                        .select(&srv_sel)
                        .next()
                        .map(|s| s.text().collect::<Vec<_>>().join(" "))
                        .unwrap_or_else(|| el.text().collect::<Vec<_>>().join(" "));
                    let clean_label = label.split_whitespace().collect::<Vec<_>>().join(" ");
                    let res = Resolution::parse_from_label(&clean_label);
                    Some((embed_url, clean_label, res))
                })
                .collect()
        };

        let sem = Arc::new(Semaphore::new(MAX_CONCURRENT_EMBEDS));
        let mut set = JoinSet::new();
        for (player_url, server_label, res_hint) in options {
            let client = self.client.clone();
            let ep_url = episode.url.clone();
            let base = self.base_url.clone();
            let sem_ref = Arc::clone(&sem);
            set.spawn(async move {
                let Ok(_permit) = sem_ref.acquire().await else {
                    return Vec::new();
                };
                tokio::time::timeout(
                    OVERALL_EXTRACT_DEADLINE,
                    resolve_animerco_embed(
                        &client,
                        &player_url,
                        &ep_url,
                        &base,
                        &server_label,
                        res_hint,
                    ),
                )
                .await
                .unwrap_or_default()
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

        if streams.is_empty() {
            return Err(SenkuError::NoValidStreams {
                episode: episode.number,
            });
        }

        Ok(streams)
    }
}

async fn resolve_animerco_embed(
    client: &Client,
    player_url: &str,
    ep_url: &str,
    base_url: &str,
    server_label: &str,
    res_hint: Resolution,
) -> Vec<StreamSource> {
    let mut out = Vec::new();
    let Ok(Ok(resp)) = tokio::time::timeout(
        PER_EMBED_TIMEOUT,
        client
            .get(player_url)
            .header("Referer", ep_url)
            .timeout(PER_EMBED_TIMEOUT)
            .send(),
    )
    .await
    else {
        return out;
    };
    if !resp.status().is_success() {
        return out;
    }
    let Ok(Ok(player_html)) = tokio::time::timeout(PER_EMBED_TIMEOUT, resp.text()).await else {
        return out;
    };

    out.extend(extract_streams_with_hint(
        &player_html,
        player_url,
        ProviderId::Animerco,
        server_label,
        res_hint,
    ));

    let inner_iframes: Vec<String> = {
        let doc = Html::parse_document(&player_html);
        let sel = Selector::parse("iframe[src]").expect("valid iframe selector");
        doc.select(&sel)
            .filter_map(|el| el.value().attr("src"))
            .filter_map(|src| resolve_url(player_url, src))
            .collect()
    };

    let mut inner_set = JoinSet::new();
    for iframe_url in inner_iframes {
        let c = client.clone();
        let ref_hdr = format!("{base_url}/");
        let label = server_label.to_string();
        inner_set.spawn(async move {
            let mut found = Vec::new();
            if let Ok(Ok(emb_resp)) = tokio::time::timeout(
                PER_EMBED_TIMEOUT,
                c.get(&iframe_url)
                    .header("Referer", ref_hdr)
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
                            &iframe_url,
                            ProviderId::Animerco,
                            &label,
                            res_hint,
                        ));
                    }
                }
            }
            found
        });
    }

    while let Some(joined) = inner_set.join_next().await {
        if let Ok(mut found) = joined {
            out.append(&mut found);
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::build_http_client;
    use std::time::Duration;

    #[test]
    fn parses_animerco_search_cards_and_seasons() {
        let client = build_http_client(Duration::from_secs(5)).unwrap();
        let prov = AnimercoProvider::new(client);

        let search_html = r#"
        <div class="col-12 media-block">
          <div class="search-card">
            <a href="https://eva.animerco.org/animes/one-piece/" title="One Piece"></a>
            <a href="https://eva.animerco.org/animes/one-piece/"><h3>One Piece</h3></a>
          </div>
        </div>"#;
        let res = prov.parse_search_html(search_html, 120);
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].title, "One Piece");
        assert_eq!(res[0].slug, "one-piece");
        assert_eq!(res[0].provider, ProviderId::Animerco);

        let anime_html = r#"
        <div class="media-seasons">
          <ul class="episodes-lists">
            <li data-number="1"><a href="https://eva.animerco.org/seasons/one_piece-season-1/">Season 1</a></li>
          </ul>
        </div>"#;
        let seasons =
            prov.extract_season_urls(anime_html, "https://eva.animerco.org/animes/one-piece/");
        assert_eq!(
            seasons,
            vec!["https://eva.animerco.org/seasons/one_piece-season-1/"]
        );

        let season_html = r#"
        <ul class="episodes-lists">
          <li><a href="https://eva.animerco.org/episodes/one-piece-%d8%a7%d9%84%d8%ad%d9%84%d9%82%d8%a9-1/">الحلقة 1</a></li>
          <li><a href="https://eva.animerco.org/episodes/one-piece-%d8%a7%d9%84%d8%ad%d9%84%d9%82%d8%a9-2/">الحلقة 2</a></li>
        </ul>"#;
        let eps = prov.parse_episodes_from_html(
            season_html,
            "https://eva.animerco.org/seasons/one_piece-season-1/",
        );
        assert_eq!(eps.len(), 2);
        assert_eq!(eps[0].number, 1);
        assert_eq!(eps[1].number, 2);
    }
}
