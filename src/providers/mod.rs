pub mod anime3rb;
pub mod animeify;
pub mod animerco;
pub mod animeslayer;
pub mod common;
pub mod okanime;

pub use common::build_http_client;

use crate::cli::log_info;
use crate::error::{Result, SenkuError};
use crate::models::{AnimeResult, AnimeSource, Episode, EpisodeSource, ProviderId, StreamSource};
use anime3rb::Anime3rbProvider;
use animeify::AnimeifyProvider;
use animerco::AnimercoProvider;
use animeslayer::AnimeSlayerProvider;
use async_trait::async_trait;
use okanime::OkAnimeProvider;
use regex::Regex;
use reqwest::Client;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::task::JoinSet;

/// Default grace window after the first healthy provider responds (`1000ms`) so
/// sibling mirrors finish cold TLS handshakes and return complete season/movie catalogs.
pub const DEFAULT_FIRST_RESULT_GRACE: Duration = Duration::from_millis(1000);
const POOLED_STREAM_DEADLINE: Duration = Duration::from_secs(18);
const POOLED_FIRST_STREAM_GRACE: Duration = Duration::from_millis(3500);

/// Common contract for all Arabic anime scraping backends.
#[async_trait]
pub trait AnimeProvider: Send + Sync {
    fn id(&self) -> ProviderId;
    fn base_url(&self) -> &str;
    async fn search(&self, query: &str) -> Result<Vec<AnimeResult>>;
    async fn get_episodes(&self, anime: &AnimeResult) -> Result<Vec<Episode>>;
    async fn get_streams(&self, episode: &Episode) -> Result<Vec<StreamSource>>;
}

struct RegisteredProvider {
    provider: Arc<dyn AnimeProvider>,
    blocked_by_default: bool,
}

type SearchCacheKey = (String, Option<ProviderId>, bool);

/// Concurrent multi-provider racing orchestrator with session episode & search caching.
pub struct ProviderEngine {
    providers: Vec<RegisteredProvider>,
    episode_cache: RwLock<HashMap<(ProviderId, String), Vec<Episode>>>,
    search_cache: RwLock<HashMap<SearchCacheKey, Vec<AnimeResult>>>,
}

impl ProviderEngine {
    /// Creates the default production engine.
    ///
    /// Active, non-Cloudflare-blocked providers (`Animerco`, `OkAnime`) run by
    /// default for `-p all`; Cloudflare-gated providers (`Anime3rb`, `AnimeSlayer`,
    /// `Animeify`) remain selectable via `-p <provider>` or `--include-blocked`.
    pub fn default_engine(client: Client) -> Self {
        Self {
            providers: vec![
                RegisteredProvider {
                    provider: Arc::new(AnimercoProvider::new(client.clone())),
                    blocked_by_default: false,
                },
                RegisteredProvider {
                    provider: Arc::new(OkAnimeProvider::new(client.clone())),
                    blocked_by_default: false,
                },
                RegisteredProvider {
                    provider: Arc::new(Anime3rbProvider::new(client.clone())),
                    blocked_by_default: true,
                },
                RegisteredProvider {
                    provider: Arc::new(AnimeSlayerProvider::new(client.clone())),
                    blocked_by_default: true,
                },
                RegisteredProvider {
                    provider: Arc::new(AnimeifyProvider::new(client)),
                    blocked_by_default: true,
                },
            ],
            episode_cache: RwLock::new(HashMap::new()),
            search_cache: RwLock::new(HashMap::new()),
        }
    }

    /// Creates an engine with an explicit list of providers (useful for testing/custom mirrors).
    pub fn with_providers(providers: Vec<Arc<dyn AnimeProvider>>) -> Self {
        Self {
            providers: providers
                .into_iter()
                .map(|provider| RegisteredProvider {
                    provider,
                    blocked_by_default: false,
                })
                .collect(),
            episode_cache: RwLock::new(HashMap::new()),
            search_cache: RwLock::new(HashMap::new()),
        }
    }

    pub fn provider(&self, id: ProviderId) -> Option<Arc<dyn AnimeProvider>> {
        self.providers
            .iter()
            .find(|p| p.provider.id() == id)
            .map(|p| Arc::clone(&p.provider))
    }

    pub fn active_provider_endpoints(&self) -> Vec<(ProviderId, String)> {
        self.providers
            .iter()
            .filter(|p| !p.blocked_by_default)
            .map(|p| (p.provider.id(), p.provider.base_url().to_string()))
            .collect()
    }

    /// Concurrently races active providers using `tokio::task::JoinSet`.
    ///
    /// **Fast-path behavior:** Once the first healthy provider returns non-empty
    /// candidates, the remaining race window shrinks to `first_result_grace`
    /// (default `650ms`) to collect sibling mirrors, then cancels slower tasks.
    ///
    /// **Smart query fallback:** If the search yields 0 matches and contains
    /// season/part tokens (`s2`, `season 2`, `session 2`, `part 2`, `2nd`),
    /// retries once with the cleaned base title.
    pub async fn search_race(
        &self,
        query: &str,
        filter: Option<ProviderId>,
        race_budget: Duration,
        first_result_grace: Duration,
        include_blocked: bool,
        verbose: bool,
    ) -> Result<(Vec<AnimeResult>, u64)> {
        let started = Instant::now();
        let cache_key = (normalize_title_tokens(query), filter, include_blocked);
        if let Some(cached) = self.search_cache.read().await.get(&cache_key).cloned() {
            return Ok((cached, 0));
        }

        let active: Vec<Arc<dyn AnimeProvider>> = self
            .providers
            .iter()
            .filter(|entry| match filter {
                Some(f) => entry.provider.id() == f,
                None => include_blocked || !entry.blocked_by_default,
            })
            .map(|entry| Arc::clone(&entry.provider))
            .collect();

        if active.is_empty() {
            return Err(SenkuError::ProviderFailure {
                provider: "engine".to_string(),
                reason: "no active providers configured".to_string(),
            });
        }

        let (mut collected, mut errors) =
            Self::execute_race_once(&active, query, race_budget, first_result_grace, verbose).await;

        let mut effective_query = query.to_string();
        if collected.is_empty() {
            if let Some(base_title) = strip_season_tokens(query) {
                log_info(&format!("Retrying base title: \"{base_title}\"..."));
                let (retry_items, retry_errors) = Self::execute_race_once(
                    &active,
                    &base_title,
                    race_budget,
                    first_result_grace,
                    verbose,
                )
                .await;
                if !retry_items.is_empty() {
                    collected = retry_items;
                    effective_query = base_title;
                } else {
                    errors.extend(retry_errors);
                }
            }
        }

        let elapsed_ms = started.elapsed().as_millis() as u64;

        if collected.is_empty() {
            if errors.is_empty() {
                return Err(SenkuError::NoSearchResults(query.to_string()));
            }
            return Err(SenkuError::ProvidersExhausted {
                timeout_ms: elapsed_ms,
                details: format!("  - {}", errors.join("\n  - ")),
            });
        }

        let fused = fuse_catalog_results(collected, &effective_query);
        self.search_cache
            .write()
            .await
            .insert(cache_key, fused.clone());

        Ok((fused, elapsed_ms))
    }

    async fn execute_race_once(
        active: &[Arc<dyn AnimeProvider>],
        query: &str,
        race_budget: Duration,
        first_result_grace: Duration,
        verbose: bool,
    ) -> (Vec<AnimeResult>, Vec<String>) {
        let mut set = JoinSet::new();
        for provider in active {
            let p = Arc::clone(provider);
            let q = query.to_string();
            set.spawn(async move {
                let id = p.id();
                (id, p.search(&q).await)
            });
        }

        let mut collected = Vec::new();
        let mut errors = Vec::new();
        let deadline = tokio::time::sleep(race_budget);
        tokio::pin!(deadline);

        loop {
            tokio::select! {
                _ = &mut deadline => {
                    break;
                }
                joined = set.join_next() => {
                    match joined {
                        Some(Ok((id, Ok(mut items)))) => {
                            if items.is_empty() {
                                errors.push(format!("{id}: returned 0 matches"));
                            } else {
                                let had_results = !collected.is_empty();
                                collected.append(&mut items);
                                // First healthy response shrinks remaining deadline to fast-path grace.
                                if !had_results && !set.is_empty() {
                                    let shrunk = tokio::time::Instant::now() + first_result_grace;
                                    if shrunk < deadline.deadline() {
                                        deadline.as_mut().reset(shrunk);
                                    }
                                }
                            }
                        }
                        Some(Ok((id, Err(e)))) => {
                            let msg = format!("{id}: {e}");
                            if verbose {
                                eprintln!("[WARN] Provider {msg}");
                            }
                            errors.push(msg);
                        }
                        Some(Err(join_err)) => {
                            let msg = format!("task panic: {join_err}");
                            if verbose {
                                eprintln!("[WARN] Provider {msg}");
                            }
                            errors.push(msg);
                        }
                        None => break,
                    }
                }
            }
        }

        if !set.is_empty() && collected.is_empty() {
            errors.push("remaining provider(s) exceeded race deadline".to_string());
        }
        set.abort_all();

        (collected, errors)
    }

    /// Fetches episode list for `anime` (union-merged concurrently across all linked
    /// Portal Fusion sources), serving per-provider episode lists from the in-memory
    /// session cache on subsequent lookups (`[n]`, `[p]`, `[s]`).
    pub async fn get_episodes(&self, anime: &AnimeResult) -> Result<Vec<Episode>> {
        let sources = anime.all_sources();
        if sources.len() == 1 {
            let src = &sources[0];
            let eps = self.get_episodes_for_source(anime, src).await?;
            if eps.is_empty() {
                return Err(SenkuError::NoEpisodesFound(anime.title.clone()));
            }
            return Ok(eps);
        }

        let mut set = JoinSet::new();
        for (order, src) in sources.into_iter().enumerate() {
            let cache_key = (src.provider, src.url.clone());
            if let Some(cached) = self.episode_cache.read().await.get(&cache_key).cloned() {
                set.spawn(async move { (order, src.provider, src.url, Ok(cached)) });
                continue;
            }
            let Some(provider) = self.provider(src.provider) else {
                continue;
            };
            let mut src_anime = anime.clone();
            src_anime.provider = src.provider;
            src_anime.url = src.url.clone();
            src_anime.slug = src.slug.clone();
            set.spawn(async move {
                let prov_id = src.provider;
                let src_url = src.url;
                let res = provider.get_episodes(&src_anime).await;
                (
                    order,
                    prov_id,
                    src_url,
                    res.map(|mut v| {
                        v.sort_by_key(|e| e.number);
                        v
                    }),
                )
            });
        }

        let mut per_source: Vec<(usize, Vec<Episode>)> = Vec::new();
        let mut last_err: Option<SenkuError> = None;
        while let Some(joined) = set.join_next().await {
            match joined {
                Ok((order, prov_id, src_url, Ok(mut eps))) => {
                    if !eps.is_empty() {
                        for ep in &mut eps {
                            if ep.sources.is_empty() {
                                ep.sources.push(EpisodeSource {
                                    provider: ep.provider,
                                    url: ep.url.clone(),
                                });
                            }
                        }
                        self.episode_cache
                            .write()
                            .await
                            .insert((prov_id, src_url), eps.clone());
                        per_source.push((order, eps));
                    }
                }
                Ok((_, _, _, Err(e))) => last_err = Some(e),
                Err(_) => {}
            }
        }

        if per_source.is_empty() {
            return Err(
                last_err.unwrap_or_else(|| SenkuError::NoEpisodesFound(anime.title.clone()))
            );
        }

        per_source.sort_by_key(|(order, _)| *order);
        let merged = merge_union_episodes(per_source.into_iter().map(|(_, eps)| eps));
        if merged.is_empty() {
            return Err(SenkuError::NoEpisodesFound(anime.title.clone()));
        }
        Ok(merged)
    }

    async fn get_episodes_for_source(
        &self,
        anime: &AnimeResult,
        src: &AnimeSource,
    ) -> Result<Vec<Episode>> {
        let cache_key = (src.provider, src.url.clone());
        if let Some(cached) = self.episode_cache.read().await.get(&cache_key) {
            return Ok(cached.clone());
        }

        let provider = self
            .provider(src.provider)
            .ok_or_else(|| SenkuError::ProviderFailure {
                provider: src.provider.to_string(),
                reason: "provider not registered".to_string(),
            })?;
        let mut src_anime = anime.clone();
        src_anime.provider = src.provider;
        src_anime.url = src.url.clone();
        src_anime.slug = src.slug.clone();
        let mut episodes = provider.get_episodes(&src_anime).await?;
        episodes.sort_by_key(|e| e.number);
        for ep in &mut episodes {
            if ep.sources.is_empty() {
                ep.sources.push(EpisodeSource {
                    provider: ep.provider,
                    url: ep.url.clone(),
                });
            }
        }
        if !episodes.is_empty() {
            self.episode_cache
                .write()
                .await
                .insert(cache_key, episodes.clone());
        }
        Ok(episodes)
    }

    /// Extracts stream candidates concurrently across all Portal Fusion providers
    /// hosting `episode` within a bounded `4.0s` deadline, tagging every stream
    /// with its originating `ProviderId`.
    pub async fn get_streams(&self, episode: &Episode) -> Result<Vec<StreamSource>> {
        let sources = episode.all_sources();
        if sources.len() == 1 {
            let src = &sources[0];
            let provider =
                self.provider(src.provider)
                    .ok_or_else(|| SenkuError::ProviderFailure {
                        provider: src.provider.to_string(),
                        reason: "provider not registered".to_string(),
                    })?;
            let mut single_ep = episode.clone();
            single_ep.provider = src.provider;
            single_ep.url = src.url.clone();
            let mut streams = provider.get_streams(&single_ep).await?;
            for s in &mut streams {
                s.provider = src.provider;
            }
            return Ok(streams);
        }

        let mut set = JoinSet::new();
        for src in sources {
            let Some(provider) = self.provider(src.provider) else {
                continue;
            };
            let mut src_ep = episode.clone();
            src_ep.provider = src.provider;
            src_ep.url = src.url.clone();
            set.spawn(async move {
                let prov_id = src.provider;
                let res = provider.get_streams(&src_ep).await;
                (prov_id, res)
            });
        }

        let mut pooled: Vec<StreamSource> = Vec::new();
        let mut seen_urls: HashSet<String> = HashSet::new();
        let mut last_err: Option<SenkuError> = None;
        let deadline = tokio::time::sleep(POOLED_STREAM_DEADLINE);
        tokio::pin!(deadline);

        loop {
            tokio::select! {
                _ = &mut deadline => {
                    set.abort_all();
                    break;
                }
                joined = set.join_next() => {
                    match joined {
                        Some(Ok((prov_id, Ok(streams)))) => {
                            let had_streams = !pooled.is_empty();
                            for mut s in streams {
                                s.provider = prov_id;
                                if seen_urls.insert(s.url.clone()) {
                                    pooled.push(s);
                                }
                            }
                            if !had_streams && !pooled.is_empty() && !set.is_empty() {
                                let shrunk = tokio::time::Instant::now() + POOLED_FIRST_STREAM_GRACE;
                                if shrunk < deadline.deadline() {
                                    deadline.as_mut().reset(shrunk);
                                }
                            }
                        }
                        Some(Ok((_, Err(e)))) => last_err = Some(e),
                        Some(Err(_)) => {}
                        None => break,
                    }
                }
            }
        }

        if pooled.is_empty() {
            return Err(last_err.unwrap_or(SenkuError::NoValidStreams {
                episode: episode.number,
            }));
        }

        Ok(pooled)
    }

    /// Queries alternative active providers (excluding `tried_providers`, e.g.
    /// `OkAnime` <-> `Animerco`) for `anime_title` and locates the matching
    /// `episode_num` for cross-provider auto-failover.
    pub async fn find_alternate_provider_episode(
        &self,
        anime_title: &str,
        episode_num: u32,
        tried_providers: &HashSet<ProviderId>,
        race_budget: Duration,
    ) -> Option<(AnimeResult, Episode)> {
        let candidates: Vec<Arc<dyn AnimeProvider>> = self
            .providers
            .iter()
            .filter(|entry| {
                !entry.blocked_by_default && !tried_providers.contains(&entry.provider.id())
            })
            .map(|entry| Arc::clone(&entry.provider))
            .collect();

        if candidates.is_empty() {
            return None;
        }

        let (mut matches, _) = Self::execute_race_once(
            &candidates,
            anime_title,
            race_budget,
            DEFAULT_FIRST_RESULT_GRACE,
            false,
        )
        .await;

        if matches.is_empty() {
            let cleaned: String = anime_title
                .chars()
                .map(|c| {
                    if c.is_alphanumeric() || c.is_whitespace() {
                        c
                    } else {
                        ' '
                    }
                })
                .collect::<String>()
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ");
            if !cleaned.is_empty() && !cleaned.eq_ignore_ascii_case(anime_title.trim()) {
                let (retry_hits, _) = Self::execute_race_once(
                    &candidates,
                    &cleaned,
                    race_budget,
                    DEFAULT_FIRST_RESULT_GRACE,
                    false,
                )
                .await;
                matches = retry_hits;
            }
        }

        if matches.is_empty() {
            return None;
        }

        let target_norm = normalize_title_tokens(anime_title);
        matches.sort_by_key(|a| {
            let cand_norm = normalize_title_tokens(&a.title);
            let alt_norm = a
                .alt_title
                .as_deref()
                .map(normalize_title_tokens)
                .unwrap_or_default();
            let exact =
                cand_norm == target_norm || (!alt_norm.is_empty() && alt_norm == target_norm);
            let contains = cand_norm.contains(&target_norm) || target_norm.contains(&cand_norm);
            (
                if exact {
                    0u8
                } else if contains {
                    1u8
                } else {
                    2u8
                },
                a.latency_ms,
            )
        });

        for alt_anime in matches.into_iter().take(4) {
            if let Ok(eps) = self.get_episodes(&alt_anime).await {
                if let Some(alt_ep) = eps.into_iter().find(|e| e.number == episode_num) {
                    return Some((alt_anime, alt_ep));
                }
            }
        }

        None
    }
}

fn normalize_title_tokens(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// Normalizes an anime title into a Portal Fusion matching key (case-insensitive,
/// stripping punctuation, extra spaces, and parenthesized/trailing 4-digit release
/// years like `(2011)` / `2011` while strictly preserving season/part/movie/OVA
/// distinctions so `Black Clover`, `Black Clover 2nd Season`, and
/// `Black Clover: Mahou Tei no Ken` remain separate while `Hunter x Hunter (2011)`
/// fuses cleanly with `Hunter x Hunter`).
pub fn normalize_fusion_key(title: &str) -> String {
    let mut cleaned = String::with_capacity(title.len());
    let mut prev_space = true;

    let mut iter = title.char_indices().peekable();
    while let Some((idx, ch)) = iter.next() {
        if ch == '(' {
            if let Some(close_rel) = title[idx + 1..].find(')') {
                let inner = title[idx + 1..idx + 1 + close_rel].trim();
                if inner.len() == 4
                    && inner.bytes().all(|b| b.is_ascii_digit())
                    && (inner.starts_with("19") || inner.starts_with("20"))
                {
                    let target_end = idx + 1 + close_rel;
                    while iter.peek().is_some_and(|&(nidx, _)| nidx <= target_end) {
                        iter.next();
                    }
                    if !prev_space {
                        cleaned.push(' ');
                        prev_space = true;
                    }
                    continue;
                }
            }
        }
        if ch.is_alphanumeric() {
            cleaned.push(ch.to_ascii_lowercase());
            prev_space = false;
        } else if !prev_space {
            cleaned.push(' ');
            prev_space = true;
        }
    }
    if cleaned.ends_with(' ') {
        cleaned.pop();
    }

    if let Some((head, tail)) = cleaned.rsplit_once(' ') {
        if !head.is_empty()
            && tail.len() == 4
            && tail.bytes().all(|b| b.is_ascii_digit())
            && (tail.starts_with("19") || tail.starts_with("20"))
        {
            let new_len = head.len();
            cleaned.truncate(new_len);
        }
    }
    cleaned
}

/// Merges episode lists from multiple providers into a sorted union by episode number,
/// attaching every hosting provider endpoint to `Episode::sources`.
pub fn merge_union_episodes<I>(lists: I) -> Vec<Episode>
where
    I: IntoIterator<Item = Vec<Episode>>,
{
    let mut by_num: BTreeMap<u32, Episode> = BTreeMap::new();
    for eps in lists {
        for mut ep in eps {
            let ep_sources = ep.all_sources();
            match by_num.get_mut(&ep.number) {
                Some(existing) => {
                    if existing.sources.is_empty() {
                        existing.sources.push(EpisodeSource {
                            provider: existing.provider,
                            url: existing.url.clone(),
                        });
                    }
                    for src in ep_sources {
                        if !existing
                            .sources
                            .iter()
                            .any(|s| s.provider == src.provider && s.url == src.url)
                        {
                            existing.sources.push(src);
                        }
                    }
                    if existing.title.is_none() && ep.title.is_some() {
                        existing.title = ep.title;
                    }
                }
                None => {
                    if ep.sources.is_empty() {
                        ep.sources = ep_sources;
                    }
                    by_num.insert(ep.number, ep);
                }
            }
        }
    }
    by_num.into_values().collect()
}

/// Strips season/part tokens (`s2`, `season 2`, `session 2`, `part 2`, `cour 2`, `2nd`)
/// to produce a clean base anime title for retry fallback when initial search returns 0 matches.
pub fn strip_season_tokens(query: &str) -> Option<String> {
    static SEASON_RE: OnceLock<Regex> = OnceLock::new();
    let re = SEASON_RE.get_or_init(|| {
        Regex::new(
            r"(?i)\b(?:(?:season|session|part|cour)\s*\d+|s\d{1,2}|\d+(?:st|nd|rd|th)(?:\s+(?:season|part))?)\b",
        )
        .expect("valid season token regex")
    });

    let orig = query.split_whitespace().collect::<Vec<_>>().join(" ");
    let cleaned = re.replace_all(&orig, " ");
    let normalized = cleaned
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim_matches(|c: char| c == '-' || c == ':' || c.is_whitespace())
        .to_string();

    if normalized.is_empty() || normalized.eq_ignore_ascii_case(&orig) {
        None
    } else {
        Some(normalized)
    }
}

/// Deduplicates exact URLs, ranks candidates against `query`, and merges identical
/// normalized titles across providers into Portal Fusion `AnimeResult` rows with
/// combined `sources`.
pub fn fuse_catalog_results(mut items: Vec<AnimeResult>, query: &str) -> Vec<AnimeResult> {
    let q_lower = query.trim().to_ascii_lowercase();
    let mut seen_urls = HashSet::with_capacity(items.len());
    items.retain(|item| seen_urls.insert(item.url.clone()));

    let mut ranked: Vec<(bool, bool, bool, AnimeResult)> = items
        .into_iter()
        .map(|item| {
            let low = item.title.to_ascii_lowercase();
            let exact = low == q_lower;
            let starts = low.starts_with(&q_lower);
            let contains = low.contains(&q_lower);
            (exact, starts, contains, item)
        })
        .collect();

    ranked.sort_by(|a, b| {
        b.0.cmp(&a.0)
            .then_with(|| b.1.cmp(&a.1))
            .then_with(|| b.2.cmp(&a.2))
            .then_with(|| a.3.latency_ms.cmp(&b.3.latency_ms))
            .then_with(|| a.3.title.cmp(&b.3.title))
    });

    let mut fused: Vec<AnimeResult> = Vec::with_capacity(ranked.len());
    let mut by_key: HashMap<String, Vec<usize>> = HashMap::with_capacity(ranked.len());
    for (_, _, _, mut item) in ranked {
        let key = normalize_fusion_key(&item.title);
        if item.sources.is_empty() {
            item.sources = item.all_sources();
        }

        let target_idx = if key.is_empty() {
            None
        } else {
            by_key.get(&key).and_then(|indices| {
                indices.iter().copied().find(|&idx| {
                    let existing = &fused[idx];
                    if let (Some(ya), Some(yb)) = (&existing.year, &item.year) {
                        if ya != yb {
                            return false;
                        }
                    }
                    !existing
                        .all_sources()
                        .iter()
                        .any(|s| s.provider == item.provider && s.url != item.url)
                })
            })
        };

        if let Some(idx) = target_idx {
            let bucket = &mut fused[idx];
            if bucket.sources.is_empty() {
                bucket.sources = bucket.all_sources();
            }
            for src in item.sources {
                if !bucket
                    .sources
                    .iter()
                    .any(|s| s.provider == src.provider && s.url == src.url)
                {
                    bucket.sources.push(src);
                }
            }
            if bucket.alt_title.is_none() {
                bucket.alt_title = item.alt_title;
            }
            if bucket.year.is_none() {
                bucket.year = item.year;
            }
            bucket.episode_count = bucket.episode_count.max(item.episode_count);
            bucket.latency_ms = bucket.latency_ms.min(item.latency_ms);
        } else {
            let new_idx = fused.len();
            if !key.is_empty() {
                by_key.entry(key).or_default().push(new_idx);
            }
            fused.push(item);
        }
    }

    fused
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    async fn spawn_mock_http(body: &'static str, delay_ms: u64) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = [0u8; 1024];
                    let _ = sock.read(&mut buf).await;
                    if delay_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    }
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                });
            }
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn fast_path_returns_under_400ms_even_when_mirror_hangs_for_5s() {
        let fast_html =
            r#"<html><body><a href="/titles/steins-gate"><h3>Steins;Gate</h3></a></body></html>"#;
        let hanging_html =
            r#"<html><body><a href="/anime/steins-gate-slow">Steins;Gate Slow</a></body></html>"#;

        let fast_url = spawn_mock_http(fast_html, 10).await;
        let hang_url = spawn_mock_http(hanging_html, 5000).await;

        let client = build_http_client(Duration::from_secs(10)).unwrap();
        let engine = ProviderEngine::with_providers(vec![
            Arc::new(Anime3rbProvider::with_base_url(client.clone(), fast_url)),
            Arc::new(AnimeifyProvider::with_base_url(client, hang_url)),
        ]);

        // Even with a 4000ms race_budget, fast-path grace (200ms) cuts off the 5s mirror in < 400ms.
        let (results, elapsed_ms) = engine
            .search_race(
                "Steins;Gate",
                None,
                Duration::from_millis(4000),
                Duration::from_millis(200),
                false,
                false,
            )
            .await
            .unwrap();

        assert!(
            elapsed_ms < 400,
            "expected fast-path race under 400ms, got {elapsed_ms}ms"
        );
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].title, "Steins;Gate");
        assert_eq!(results[0].provider, ProviderId::Anime3rb);
    }

    #[tokio::test]
    async fn full_1s_grace_collects_750ms_sibling_and_returns_immediately_when_all_finish_fast() {
        let fast_html = r#"<html><body><a href="/titles/hunter-x-hunter"><h3>Hunter x Hunter</h3></a></body></html>"#;
        let sibling_html = r#"<html><body><a href="/anime/hunter-x-hunter-2011">Hunter x Hunter (2011)</a></body></html>"#;

        // Case 1: 750ms sibling completes within DEFAULT_FIRST_RESULT_GRACE (1000ms).
        let fast_url = spawn_mock_http(fast_html, 10).await;
        let slow_sibling_url = spawn_mock_http(sibling_html, 750).await;
        let client = build_http_client(Duration::from_secs(5)).unwrap();
        let engine = ProviderEngine::with_providers(vec![
            Arc::new(Anime3rbProvider::with_base_url(client.clone(), fast_url)),
            Arc::new(AnimeifyProvider::with_base_url(
                client.clone(),
                slow_sibling_url,
            )),
        ]);

        let (results, elapsed) = engine
            .search_race(
                "Hunter x Hunter",
                None,
                Duration::from_millis(4000),
                DEFAULT_FIRST_RESULT_GRACE,
                false,
                false,
            )
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].sources.len(),
            2,
            "expected both providers fused within 1000ms grace"
        );
        assert!(elapsed >= 700, "elapsed {elapsed}ms");

        // Case 2: Both finish quickly (<100ms) -> returns immediately without sleeping 1000ms!
        let quick_a = spawn_mock_http(fast_html, 15).await;
        let quick_b = spawn_mock_http(sibling_html, 25).await;
        let quick_engine = ProviderEngine::with_providers(vec![
            Arc::new(Anime3rbProvider::with_base_url(client.clone(), quick_a)),
            Arc::new(AnimeifyProvider::with_base_url(client, quick_b)),
        ]);
        let (quick_res, quick_ms) = quick_engine
            .search_race(
                "Hunter x Hunter",
                None,
                Duration::from_millis(4000),
                DEFAULT_FIRST_RESULT_GRACE,
                false,
                false,
            )
            .await
            .unwrap();
        assert_eq!(quick_res.len(), 1);
        assert_eq!(quick_res[0].sources.len(), 2);
        assert!(
            quick_ms < 250,
            "expected immediate return when all providers finish fast, took {quick_ms}ms"
        );
    }

    #[tokio::test]
    async fn caches_episodes_in_memory_across_lookups() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct CountingProvider {
            base: String,
            hits: Arc<AtomicUsize>,
        }

        #[async_trait]
        impl AnimeProvider for CountingProvider {
            fn id(&self) -> ProviderId {
                ProviderId::Animerco
            }
            fn base_url(&self) -> &str {
                &self.base
            }
            async fn search(&self, _query: &str) -> Result<Vec<AnimeResult>> {
                Ok(vec![])
            }
            async fn get_episodes(&self, _anime: &AnimeResult) -> Result<Vec<Episode>> {
                self.hits.fetch_add(1, Ordering::SeqCst);
                Ok(vec![Episode {
                    number: 1,
                    title: Some("Ep 1".into()),
                    url: format!("{}/ep/1", self.base),
                    provider: ProviderId::Animerco,
                    sources: Vec::new(),
                }])
            }
            async fn get_streams(&self, _episode: &Episode) -> Result<Vec<StreamSource>> {
                Ok(vec![])
            }
        }

        let hits = Arc::new(AtomicUsize::new(0));
        let engine = ProviderEngine::with_providers(vec![Arc::new(CountingProvider {
            base: "https://example.invalid".into(),
            hits: Arc::clone(&hits),
        })]);

        let anime = AnimeResult {
            slug: "steins-gate".into(),
            title: "Steins;Gate".into(),
            alt_title: None,
            url: "https://example.invalid/anime/steins-gate".into(),
            provider: ProviderId::Animerco,
            year: None,
            episode_count: None,
            latency_ms: 5,
            sources: Vec::new(),
        };

        let first = engine.get_episodes(&anime).await.unwrap();
        let second = engine.get_episodes(&anime).await.unwrap();
        assert_eq!(first, second);
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn strips_season_tokens_and_retries_base_title_on_zero_matches() {
        assert_eq!(
            strip_season_tokens("black clover s2").as_deref(),
            Some("black clover")
        );
        assert_eq!(
            strip_season_tokens("Attack on Titan Season 4").as_deref(),
            Some("Attack on Titan")
        );
        assert_eq!(
            strip_season_tokens("Jujutsu Kaisen session 2").as_deref(),
            Some("Jujutsu Kaisen")
        );
        assert_eq!(
            strip_season_tokens("Mob Psycho 100 2nd Season").as_deref(),
            Some("Mob Psycho 100")
        );
        assert_eq!(strip_season_tokens("Steins;Gate 0"), None);

        struct BaseTitleMockProvider;

        #[async_trait]
        impl AnimeProvider for BaseTitleMockProvider {
            fn id(&self) -> ProviderId {
                ProviderId::OkAnime
            }
            fn base_url(&self) -> &str {
                "https://example.invalid"
            }
            async fn search(&self, query: &str) -> Result<Vec<AnimeResult>> {
                if query.eq_ignore_ascii_case("black clover") {
                    Ok(vec![AnimeResult {
                        slug: "black-clover".into(),
                        title: "Black Clover".into(),
                        alt_title: None,
                        url: "https://example.invalid/anime/black-clover".into(),
                        provider: ProviderId::OkAnime,
                        year: None,
                        episode_count: None,
                        latency_ms: 15,
                        sources: Vec::new(),
                    }])
                } else {
                    Ok(vec![])
                }
            }
            async fn get_episodes(&self, _anime: &AnimeResult) -> Result<Vec<Episode>> {
                Ok(vec![])
            }
            async fn get_streams(&self, _episode: &Episode) -> Result<Vec<StreamSource>> {
                Ok(vec![])
            }
        }

        let engine = ProviderEngine::with_providers(vec![Arc::new(BaseTitleMockProvider)]);
        let (results, _) = engine
            .search_race(
                "black clover s2",
                None,
                Duration::from_millis(500),
                Duration::from_millis(100),
                false,
                false,
            )
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].title, "Black Clover");
    }

    #[tokio::test]
    async fn cross_provider_fallback_matches_episode_on_backup_provider() {
        struct PrimaryEmptyProvider;
        struct BackupAnimercoProvider;

        #[async_trait]
        impl AnimeProvider for PrimaryEmptyProvider {
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
        impl AnimeProvider for BackupAnimercoProvider {
            fn id(&self) -> ProviderId {
                ProviderId::Animerco
            }
            fn base_url(&self) -> &str {
                "https://animerco.invalid"
            }
            async fn search(&self, query: &str) -> Result<Vec<AnimeResult>> {
                if query.to_ascii_lowercase().contains("dragon ball super") {
                    Ok(vec![
                        AnimeResult {
                            slug: "dragon-ball-super".into(),
                            title: "Dragon Ball Super".into(),
                            alt_title: None,
                            url: "https://animerco.invalid/animes/dragon-ball-super".into(),
                            provider: ProviderId::Animerco,
                            year: None,
                            episode_count: None,
                            latency_ms: 10,
                            sources: Vec::new(),
                        },
                        AnimeResult {
                            slug: "dragon-ball-super-beerus".into(),
                            title: "Dragon Ball Super: Beerus".into(),
                            alt_title: None,
                            url: "https://animerco.invalid/animes/dragon-ball-super-beerus".into(),
                            provider: ProviderId::Animerco,
                            year: None,
                            episode_count: None,
                            latency_ms: 25,
                            sources: Vec::new(),
                        },
                    ])
                } else {
                    Ok(vec![])
                }
            }
            async fn get_episodes(&self, anime: &AnimeResult) -> Result<Vec<Episode>> {
                if anime.slug == "dragon-ball-super-beerus" {
                    Ok(vec![Episode {
                        number: 6,
                        title: Some("Ep 6".into()),
                        url: "https://animerco.invalid/episodes/dbs-beerus-6".into(),
                        provider: ProviderId::Animerco,
                        sources: Vec::new(),
                    }])
                } else {
                    Ok(vec![])
                }
            }
            async fn get_streams(&self, _ep: &Episode) -> Result<Vec<StreamSource>> {
                Ok(vec![])
            }
        }

        let engine = ProviderEngine::with_providers(vec![
            Arc::new(PrimaryEmptyProvider),
            Arc::new(BackupAnimercoProvider),
        ]);
        let mut tried = HashSet::new();
        tried.insert(ProviderId::OkAnime);

        let matched = engine
            .find_alternate_provider_episode(
                "Dragon Ball Super: Beerus",
                6,
                &tried,
                Duration::from_millis(500),
            )
            .await
            .expect("should match Episode 6 on Animerco backup provider");

        assert_eq!(matched.0.provider, ProviderId::Animerco);
        assert_eq!(matched.0.title, "Dragon Ball Super: Beerus");
        assert_eq!(matched.1.number, 6);
        assert_eq!(matched.1.provider, ProviderId::Animerco);
    }

    #[tokio::test]
    async fn portal_fusion_merges_duplicate_titles_unions_episodes_and_pools_streams() {
        struct FusionAnimercoProvider;
        struct FusionOkAnimeProvider;

        #[async_trait]
        impl AnimeProvider for FusionAnimercoProvider {
            fn id(&self) -> ProviderId {
                ProviderId::Animerco
            }
            fn base_url(&self) -> &str {
                "https://animerco.invalid"
            }
            async fn search(&self, _q: &str) -> Result<Vec<AnimeResult>> {
                Ok(vec![
                    AnimeResult {
                        slug: "black-clover".into(),
                        title: "Black Clover".into(),
                        alt_title: None,
                        url: "https://animerco.invalid/animes/black-clover".into(),
                        provider: ProviderId::Animerco,
                        year: Some("2017".into()),
                        episode_count: Some(170),
                        latency_ms: 20,
                        sources: Vec::new(),
                    },
                    AnimeResult {
                        slug: "black-clover-s2".into(),
                        title: "Black Clover 2nd Season".into(),
                        alt_title: None,
                        url: "https://animerco.invalid/animes/black-clover-s2".into(),
                        provider: ProviderId::Animerco,
                        year: None,
                        episode_count: Some(12),
                        latency_ms: 22,
                        sources: Vec::new(),
                    },
                ])
            }
            async fn get_episodes(&self, _a: &AnimeResult) -> Result<Vec<Episode>> {
                Ok(vec![
                    Episode {
                        number: 1,
                        title: Some("Ep 1".into()),
                        url: "https://animerco.invalid/episodes/bc-1".into(),
                        provider: ProviderId::Animerco,
                        sources: Vec::new(),
                    },
                    Episode {
                        number: 2,
                        title: Some("Ep 2".into()),
                        url: "https://animerco.invalid/episodes/bc-2".into(),
                        provider: ProviderId::Animerco,
                        sources: Vec::new(),
                    },
                ])
            }
            async fn get_streams(&self, _ep: &Episode) -> Result<Vec<StreamSource>> {
                Ok(vec![StreamSource {
                    url: "https://cdn.animerco.invalid/ep1-1080p.m3u8".into(),
                    resolution: crate::models::Resolution::Fhd1080,
                    provider: ProviderId::Animerco,
                    server_name: "vidcache FHD".into(),
                    referer: None,
                    content_length: None,
                    verified: false,
                    throughput_kbps: None,
                    supports_ranges: false,
                }])
            }
        }

        #[async_trait]
        impl AnimeProvider for FusionOkAnimeProvider {
            fn id(&self) -> ProviderId {
                ProviderId::OkAnime
            }
            fn base_url(&self) -> &str {
                "https://okanime.invalid"
            }
            async fn search(&self, _q: &str) -> Result<Vec<AnimeResult>> {
                Ok(vec![
                    AnimeResult {
                        slug: "black-clover-oka".into(),
                        title: "Black Clover  ".into(),
                        alt_title: Some("بلاك كلوفر".into()),
                        url: "https://okanime.invalid/anime/black-clover".into(),
                        provider: ProviderId::OkAnime,
                        year: None,
                        episode_count: None,
                        latency_ms: 35,
                        sources: Vec::new(),
                    },
                    AnimeResult {
                        slug: "black-clover-movie".into(),
                        title: "Black Clover: Mahou Tei no Ken".into(),
                        alt_title: None,
                        url: "https://okanime.invalid/anime/black-clover-movie".into(),
                        provider: ProviderId::OkAnime,
                        year: Some("2023".into()),
                        episode_count: Some(1),
                        latency_ms: 40,
                        sources: Vec::new(),
                    },
                ])
            }
            async fn get_episodes(&self, _a: &AnimeResult) -> Result<Vec<Episode>> {
                Ok(vec![
                    Episode {
                        number: 2,
                        title: None,
                        url: "https://okanime.invalid/episode/bc-2".into(),
                        provider: ProviderId::OkAnime,
                        sources: Vec::new(),
                    },
                    Episode {
                        number: 3,
                        title: Some("Ep 3 (OKA exclusive)".into()),
                        url: "https://okanime.invalid/episode/bc-3".into(),
                        provider: ProviderId::OkAnime,
                        sources: Vec::new(),
                    },
                ])
            }
            async fn get_streams(&self, _ep: &Episode) -> Result<Vec<StreamSource>> {
                Ok(vec![StreamSource {
                    url: "https://cdn.okanime.invalid/ep2-720p.mp4".into(),
                    resolution: crate::models::Resolution::Hd720,
                    provider: ProviderId::OkAnime,
                    server_name: "okanime-embed HD".into(),
                    referer: None,
                    content_length: None,
                    verified: false,
                    throughput_kbps: None,
                    supports_ranges: false,
                }])
            }
        }

        let engine = ProviderEngine::with_providers(vec![
            Arc::new(FusionAnimercoProvider),
            Arc::new(FusionOkAnimeProvider),
        ]);
        let (catalog, _) = engine
            .search_race(
                "Black Clover",
                None,
                Duration::from_secs(2),
                Duration::from_millis(500),
                false,
                false,
            )
            .await
            .unwrap();

        // Base series fused across MRC+OKA while 2nd Season & Movie stay separate!
        assert_eq!(catalog.len(), 3);
        let fused = &catalog[0];
        assert_eq!(fused.title, "Black Clover");
        assert_eq!(fused.provider_badge(), "MRC+OKA");
        assert_eq!(fused.sources.len(), 2);
        assert_eq!(fused.alt_title.as_deref(), Some("بلاك كلوفر"));

        assert!(catalog
            .iter()
            .any(|a| a.title == "Black Clover 2nd Season" && a.sources.len() == 1));
        assert!(catalog
            .iter()
            .any(|a| a.title == "Black Clover: Mahou Tei no Ken" && a.sources.len() == 1));

        // Union episodes: {1 (MRC), 2 (MRC+OKA), 3 (OKA)}
        let episodes = engine.get_episodes(fused).await.unwrap();
        assert_eq!(
            episodes.iter().map(|e| e.number).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(episodes[0].all_sources().len(), 1);
        assert_eq!(episodes[1].all_sources().len(), 2);
        assert_eq!(episodes[2].all_sources().len(), 1);

        // Pooled cross-provider streams for Episode 2 preserve each StreamSource's ProviderId!
        let pooled = engine.get_streams(&episodes[1]).await.unwrap();
        assert_eq!(pooled.len(), 2);
        assert!(pooled.iter().any(|s| s.provider == ProviderId::Animerco));
        assert!(pooled.iter().any(|s| s.provider == ProviderId::OkAnime));

        // 1. Release-year normalization in normalize_fusion_key
        assert_eq!(
            normalize_fusion_key("Hunter x Hunter (2011)"),
            normalize_fusion_key("Hunter x Hunter")
        );
        assert_eq!(
            normalize_fusion_key("Hunter x Hunter 2011"),
            "hunter x hunter"
        );
        assert_ne!(
            normalize_fusion_key("Black Clover"),
            normalize_fusion_key("Black Clover 2nd Season")
        );
        assert_ne!(
            normalize_fusion_key("Black Clover"),
            normalize_fusion_key("Black Clover: Mahou Tei no Ken")
        );
        assert_eq!(normalize_fusion_key("Steins;Gate 0"), "steins gate 0");

        // 2. Repeated search in the same session hits search_cache in 0ms.
        let (cached_hits, cached_ms) = engine
            .search_race(
                "black clover ",
                None,
                Duration::from_secs(2),
                Duration::from_millis(500),
                false,
                false,
            )
            .await
            .unwrap();
        assert_eq!(cached_hits.len(), 3);
        assert_eq!(cached_ms, 0);
    }
}
