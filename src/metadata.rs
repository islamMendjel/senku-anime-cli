use chrono::Utc;
use regex::Regex;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Duration;

pub const ANILIST_GRAPHQL_URL: &str = "https://graphql.anilist.co";
pub const JIKAN_SEARCH_URL: &str = "https://api.jikan.moe/v4/anime";
pub const ANISKIP_API_BASE: &str = "https://api.aniskip.com/v2/skip-times";

pub const META_CACHE_TTL_SECS: i64 = 7 * 24 * 60 * 60; // 7 days
pub const EPISODE_GUIDE_CACHE_TTL_SECS: i64 = 14 * 24 * 60 * 60; // 14 days
pub const ANISKIP_TIMEOUT: Duration = Duration::from_millis(2500);
pub const ANILIST_TIMEOUT: Duration = Duration::from_millis(3000);
pub const JIKAN_TIMEOUT: Duration = Duration::from_millis(2500);
pub const JIKAN_EPISODES_TIMEOUT: Duration = Duration::from_millis(1800);

const ANILIST_MEDIA_QUERY: &str = r#"
query ($search: String) {
  Page(page: 1, perPage: 5) {
    media(search: $search, type: ANIME, sort: SEARCH_MATCH) {
      idMal
      title {
        romaji
        english
        native
      }
      synonyms
      averageScore
      episodes
      nextAiringEpisode {
        episode
      }
      status
      seasonYear
      genres
      description(asHtml: false)
      coverImage {
        large
      }
    }
  }
}
"#;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AnimeMetadata {
    pub id_mal: Option<u32>,
    pub title_romaji: String,
    pub title_english: Option<String>,
    pub average_score: Option<u32>,
    pub episodes: Option<u32>,
    #[serde(default)]
    pub next_airing_episode: Option<u32>,
    pub status: Option<String>,
    pub season_year: Option<u32>,
    pub genres: Vec<String>,
    pub description: String,
    pub cover_image_url: Option<String>,
    pub cached_at: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SkipType {
    Op,
    Ed,
    Recap,
    MixedOp,
    MixedEd,
}

impl SkipType {
    pub fn from_api_str(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "op" => Some(Self::Op),
            "ed" => Some(Self::Ed),
            "recap" => Some(Self::Recap),
            "mixed-op" => Some(Self::MixedOp),
            "mixed-ed" => Some(Self::MixedEd),
            _ => None,
        }
    }

    pub fn is_op(self) -> bool {
        matches!(self, Self::Op | Self::MixedOp)
    }

    pub fn is_ed(self) -> bool {
        matches!(self, Self::Ed | Self::MixedEd)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SkipInterval {
    pub skip_type: SkipType,
    pub start_time: f64,
    pub end_time: f64,
    pub episode_length: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChapterEntry {
    pub title: String,
    pub time: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SkipTarget {
    ExactSeek {
        target_secs: f64,
        label: &'static str,
    },
    RelativeFallback {
        delta_secs: u64,
    },
}

/// Resolves `~/.cache/senku-anime-cli` (or `$XDG_CACHE_HOME/senku-anime-cli`).
pub fn cache_root_dir() -> PathBuf {
    dirs::cache_dir()
        .or_else(|| dirs::home_dir().map(|h| h.join(".cache")))
        .unwrap_or_else(|| PathBuf::from("/tmp"))
        .join("senku-anime-cli")
}

/// Atomically writes `bytes` to `path` using a PID + monotonic sequence temp file
/// in the same directory, preventing torn reads or `.tmp` collisions across concurrent
/// `--preview` processes.
pub fn atomic_write_bytes(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    static TMP_SEQ: AtomicU64 = AtomicU64::new(0);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let stem = path.file_name().and_then(|n| n.to_str()).unwrap_or("cache");
    let tmp_name = format!(".{stem}.{pid}.{seq}.tmp");
    let tmp_path = path.with_file_name(tmp_name);
    if let Err(e) = fs::write(&tmp_path, bytes) {
        let _ = fs::remove_file(&tmp_path);
        return Err(e);
    }
    if let Err(e) = fs::rename(&tmp_path, path) {
        let _ = fs::remove_file(&tmp_path);
        return Err(e);
    }
    Ok(())
}

/// Normalizes an anime title into a safe lowercase filename slug for metadata cache files.
/// Strictly restricts characters to ASCII alphanumeric + `-`, eliminates path traversal,
/// and caps length (`<= 160` bytes) with a deterministic FNV-1a suffix on long titles so
/// ext4/btrfs `NAME_MAX` is never exceeded.
pub fn cache_slug(title: &str) -> String {
    let mut slug = String::with_capacity(title.len().min(160));
    let mut prev_dash = false;
    for ch in title.trim().chars() {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch.to_ascii_lowercase());
            prev_dash = false;
        } else if !prev_dash && !slug.is_empty() {
            slug.push('-');
            prev_dash = true;
        }
    }
    let trimmed = slug.trim_matches('-').to_string();
    if trimmed.is_empty() {
        return "unknown-anime".to_string();
    }
    if trimmed.len() <= 160 {
        return trimmed;
    }
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in trimmed.as_bytes() {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    let prefix = trimmed[..140].trim_end_matches('-');
    format!("{prefix}-{hash:016x}")
}

/// Normalizes an anime search query by stripping `fzf` row numbering, provider
/// tags (`[MRC]`, `[OKA]`), latency badges (`(142ms)`), year/episode suffixes,
/// Arabic noise words (`مترجم`, `انمي`, `فيلم`, `الحلقة`), and extra spaces.
pub fn clean_metadata_query(raw: &str) -> String {
    let mut s = raw.trim().to_string();

    if let Some((_, rest)) = s.split_once('|') {
        s = rest.trim().to_string();
    }

    if s.starts_with('[') {
        if let Some(close_idx) = s.find(']') {
            s = s[close_idx + 1..].trim().to_string();
        }
    }

    if let Some((before_ep, _)) = s.split_once(" — Ep ") {
        s = before_ep.trim().to_string();
    }

    static TRAILING_PARENS_RE: OnceLock<Regex> = OnceLock::new();
    let paren_re = TRAILING_PARENS_RE.get_or_init(|| {
        Regex::new(r"(?:\s*\((?:\d+ms|\d{4}|\d{4}-\d{2}-\d{2}[^)]*)\))+$")
            .expect("valid trailing parens regex")
    });
    s = paren_re.replace(&s, "").trim().to_string();

    const ARABIC_NOISE: &[&str] = &[
        "مترجم",
        "انمي",
        "أنمي",
        "فيلم",
        "الحلقة",
        "حلقة",
        "الموسم",
        "الجزء",
        "اوفا",
        "اونا",
    ];

    s.split_whitespace()
        .filter(|tok| !ARABIC_NOISE.contains(tok))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Strips HTML tags (`<br>`, `<i>`, `<b>`, etc.) and decodes named and numeric
/// HTML entities (`&mdash;`, `&ndash;`, `&#039;`, `&#x27;`, etc.).
pub fn strip_html_tags(input: &str) -> String {
    static BR_RE: OnceLock<Regex> = OnceLock::new();
    static TAG_RE: OnceLock<Regex> = OnceLock::new();
    static DEC_ENTITY_RE: OnceLock<Regex> = OnceLock::new();
    static HEX_ENTITY_RE: OnceLock<Regex> = OnceLock::new();

    let br_re = BR_RE.get_or_init(|| Regex::new(r"(?i)<br\s*/?>").expect("valid br regex"));
    let tag_re = TAG_RE.get_or_init(|| Regex::new(r"<[^>]+>").expect("valid html tag regex"));
    let dec_re =
        DEC_ENTITY_RE.get_or_init(|| Regex::new(r"&#(\d+);").expect("valid dec entity regex"));
    let hex_re = HEX_ENTITY_RE
        .get_or_init(|| Regex::new(r"(?i)&#x([0-9a-f]+);").expect("valid hex entity regex"));

    let with_spaces = br_re.replace_all(input, " ");
    let no_tags = tag_re.replace_all(&with_spaces, "");
    let named = no_tags
        .replace("&amp;", "&")
        .replace("&quot;", "\"")
        .replace("&#039;", "'")
        .replace("&#39;", "'")
        .replace("&apos;", "'")
        .replace("&rsquo;", "'")
        .replace("&lsquo;", "'")
        .replace("&ldquo;", "\"")
        .replace("&rdquo;", "\"")
        .replace("&mdash;", "—")
        .replace("&ndash;", "–")
        .replace("&hellip;", "...")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&nbsp;", " ");

    let dec_decoded = dec_re.replace_all(&named, |caps: &regex::Captures<'_>| {
        caps[1]
            .parse::<u32>()
            .ok()
            .and_then(char::from_u32)
            .map(|c| c.to_string())
            .unwrap_or_else(|| caps[0].to_string())
    });
    let hex_decoded = hex_re.replace_all(&dec_decoded, |caps: &regex::Captures<'_>| {
        u32::from_str_radix(&caps[1], 16)
            .ok()
            .and_then(char::from_u32)
            .map(|c| c.to_string())
            .unwrap_or_else(|| caps[0].to_string())
    });

    hex_decoded.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn norm_match_str(s: &str) -> String {
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

/// Scores candidate titles (`romaji`, `english`, `native`, `synonyms`) against `query`.
fn score_title_candidate(
    query: &str,
    candidate_titles: &[&str],
    id_mal: Option<u32>,
    score: Option<u32>,
) -> u32 {
    let q_norm = norm_match_str(query);
    if q_norm.is_empty() {
        return 0;
    }
    let q_tokens: Vec<&str> = q_norm.split_whitespace().collect();

    let mut best = 0u32;
    for raw_t in candidate_titles {
        let t_norm = norm_match_str(raw_t);
        if t_norm.is_empty() {
            continue;
        }
        let s = if t_norm == q_norm {
            10_000
        } else if t_norm.starts_with(&q_norm) || q_norm.starts_with(&t_norm) {
            let len_penalty = (t_norm.len().abs_diff(q_norm.len()) as u32).min(500);
            7_000 - len_penalty
        } else if t_norm.contains(&q_norm) || q_norm.contains(&t_norm) {
            let len_penalty = (t_norm.len().abs_diff(q_norm.len()) as u32).min(500);
            5_000 - len_penalty
        } else {
            let t_tokens: Vec<&str> = t_norm.split_whitespace().collect();
            let hits = q_tokens.iter().filter(|qt| t_tokens.contains(qt)).count() as u32;
            if hits == 0 {
                0
            } else {
                (hits * 1_000) / (q_tokens.len().max(1) as u32)
            }
        };
        if s > best {
            best = s;
        }
    }

    best + if id_mal.is_some() { 50 } else { 0 } + score.unwrap_or(0).min(100) / 4
}

#[derive(Debug, Deserialize)]
struct AniListGraphQlResp {
    data: Option<AniListData>,
}

#[derive(Debug, Deserialize)]
struct AniListData {
    #[serde(rename = "Page")]
    page: Option<AniListPage>,
    #[serde(rename = "Media")]
    media: Option<AniListMedia>,
}

#[derive(Debug, Deserialize)]
struct AniListPage {
    #[serde(default)]
    media: Vec<AniListMedia>,
}

#[derive(Debug, Deserialize)]
struct AniListMedia {
    #[serde(rename = "idMal")]
    id_mal: Option<u32>,
    title: AniListTitle,
    #[serde(default)]
    synonyms: Vec<String>,
    #[serde(rename = "averageScore")]
    average_score: Option<u32>,
    episodes: Option<u32>,
    #[serde(rename = "nextAiringEpisode")]
    next_airing_episode: Option<AniListNextAiring>,
    status: Option<String>,
    #[serde(rename = "seasonYear")]
    season_year: Option<u32>,
    #[serde(default)]
    genres: Vec<String>,
    description: Option<String>,
    #[serde(rename = "coverImage")]
    cover_image: Option<AniListCoverImage>,
}

#[derive(Debug, Deserialize)]
struct AniListNextAiring {
    episode: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct AniListTitle {
    romaji: Option<String>,
    english: Option<String>,
    native: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AniListCoverImage {
    large: Option<String>,
}

impl AniListMedia {
    fn into_metadata(self, now_ts: i64) -> Option<AnimeMetadata> {
        let title_romaji = self
            .title
            .romaji
            .filter(|s| !s.trim().is_empty())
            .or_else(|| self.title.english.clone())
            .or_else(|| self.title.native.clone())?;
        let description = self
            .description
            .as_deref()
            .map(strip_html_tags)
            .unwrap_or_default();

        Some(AnimeMetadata {
            id_mal: self.id_mal,
            title_romaji,
            title_english: self.title.english.filter(|s| !s.trim().is_empty()),
            average_score: self.average_score,
            episodes: self.episodes,
            next_airing_episode: self.next_airing_episode.and_then(|n| n.episode),
            status: self.status,
            season_year: self.season_year,
            genres: self.genres,
            description,
            cover_image_url: self.cover_image.and_then(|c| c.large),
            cached_at: now_ts,
        })
    }
}

/// Parses an AniList `Page.media` (or legacy single `Media`) GraphQL JSON response
/// and picks the highest-scoring candidate matching `query`.
pub fn parse_anilist_response(body: &str, query: &str, now_ts: i64) -> Option<AnimeMetadata> {
    let parsed: AniListGraphQlResp = serde_json::from_str(body).ok()?;
    let data = parsed.data?;

    let mut items = Vec::new();
    if let Some(page) = data.page {
        items.extend(page.media);
    }
    if let Some(single) = data.media {
        items.push(single);
    }
    if items.is_empty() {
        return None;
    }

    let best = items.into_iter().max_by_key(|m| {
        let mut titles: Vec<&str> = Vec::new();
        if let Some(ref r) = m.title.romaji {
            titles.push(r.as_str());
        }
        if let Some(ref e) = m.title.english {
            titles.push(e.as_str());
        }
        if let Some(ref n) = m.title.native {
            titles.push(n.as_str());
        }
        for syn in &m.synonyms {
            titles.push(syn.as_str());
        }
        score_title_candidate(query, &titles, m.id_mal, m.average_score)
    })?;

    best.into_metadata(now_ts)
}

#[derive(Debug, Deserialize)]
struct JikanSearchResp {
    #[serde(default)]
    data: Vec<JikanAnime>,
}

#[derive(Debug, Deserialize)]
struct JikanAnime {
    mal_id: u32,
    title: String,
    title_english: Option<String>,
    title_japanese: Option<String>,
    #[serde(default)]
    title_synonyms: Vec<String>,
    score: Option<f64>,
    episodes: Option<u32>,
    status: Option<String>,
    year: Option<u32>,
    #[serde(default)]
    genres: Vec<JikanGenre>,
    synopsis: Option<String>,
    images: Option<JikanImages>,
}

#[derive(Debug, Deserialize)]
struct JikanGenre {
    name: String,
}

#[derive(Debug, Deserialize)]
struct JikanImages {
    jpg: Option<JikanImageSet>,
    webp: Option<JikanImageSet>,
}

#[derive(Debug, Deserialize)]
struct JikanImageSet {
    large_image_url: Option<String>,
    image_url: Option<String>,
}

/// Parses Jikan v4 `/anime?q=...` JSON and picks the best matching candidate.
pub fn parse_jikan_response(body: &str, query: &str, now_ts: i64) -> Option<AnimeMetadata> {
    let parsed: JikanSearchResp = serde_json::from_str(body).ok()?;
    let best = parsed.data.into_iter().max_by_key(|m| {
        let mut titles: Vec<&str> = vec![m.title.as_str()];
        if let Some(ref e) = m.title_english {
            titles.push(e.as_str());
        }
        if let Some(ref j) = m.title_japanese {
            titles.push(j.as_str());
        }
        for syn in &m.title_synonyms {
            titles.push(syn.as_str());
        }
        let score_pct = m.score.map(|s| (s * 10.0).round() as u32);
        score_title_candidate(query, &titles, Some(m.mal_id), score_pct)
    })?;

    let cover_image_url = best.images.and_then(|imgs| {
        imgs.jpg
            .and_then(|j| j.large_image_url.or(j.image_url))
            .or_else(|| imgs.webp.and_then(|w| w.large_image_url.or(w.image_url)))
    });
    let average_score = best
        .score
        .map(|s| (s * 10.0).round().clamp(0.0, 100.0) as u32);
    let description = best
        .synopsis
        .as_deref()
        .map(strip_html_tags)
        .unwrap_or_default();

    Some(AnimeMetadata {
        id_mal: Some(best.mal_id),
        title_romaji: best.title,
        title_english: best.title_english.filter(|s| !s.trim().is_empty()),
        average_score,
        episodes: best.episodes,
        next_airing_episode: None,
        status: best.status,
        season_year: best.year,
        genres: best.genres.into_iter().map(|g| g.name).collect(),
        description,
        cover_image_url,
        cached_at: now_ts,
    })
}

/// Returns true only for non-empty, cache-worthy metadata (prevents poisoning 7-day cache).
pub fn is_valid_metadata_for_cache(meta: &AnimeMetadata) -> bool {
    let title_ok = !meta.title_romaji.trim().is_empty() && meta.title_romaji != "Unknown Title";
    let has_signal =
        meta.id_mal.is_some() || !meta.description.is_empty() || meta.cover_image_url.is_some();
    title_ok && has_signal
}

pub const PREVIEW_DEBOUNCE: Duration = Duration::from_millis(120);

/// Checks local metadata cache for `raw_title` immediately (0ms).
pub fn load_cached_metadata_for_title(raw_title: &str, cache_root: &Path) -> Option<AnimeMetadata> {
    let cleaned = clean_metadata_query(raw_title);
    if cleaned.is_empty() {
        return None;
    }
    let meta_path = cache_root
        .join("meta")
        .join(format!("{}.json", cache_slug(&cleaned)));
    load_cached_metadata(&meta_path, Utc::now().timestamp())
}

/// Zero-429 preview metadata fetcher:
/// - Cache hit -> returns immediately (`0ms`).
/// - Cache miss -> debounces (`120ms`) before issuing network requests so rapid
///   `fzf` scrolling cancels superseded preview spawns before hitting AniList/Jikan.
pub async fn fetch_preview_metadata(
    client: &Client,
    raw_title: &str,
    debounce: Duration,
) -> Option<AnimeMetadata> {
    fetch_preview_metadata_with_opts(
        client,
        raw_title,
        ANILIST_GRAPHQL_URL,
        JIKAN_SEARCH_URL,
        &cache_root_dir(),
        debounce,
    )
    .await
}

pub async fn fetch_preview_metadata_with_opts(
    client: &Client,
    raw_title: &str,
    anilist_endpoint: &str,
    jikan_endpoint: &str,
    cache_root: &Path,
    debounce: Duration,
) -> Option<AnimeMetadata> {
    if let Some(cached) = load_cached_metadata_for_title(raw_title, cache_root) {
        return Some(cached);
    }
    if !debounce.is_zero() {
        tokio::time::sleep(debounce).await;
    }
    fetch_metadata_with_endpoints(
        client,
        raw_title,
        anilist_endpoint,
        jikan_endpoint,
        cache_root,
    )
    .await
}

/// Fetches AniList metadata (with automatic Jikan MAL fallback and 7-day local cache).
pub async fn fetch_anilist_metadata(client: &Client, title: &str) -> Option<AnimeMetadata> {
    fetch_metadata_with_endpoints(
        client,
        title,
        ANILIST_GRAPHQL_URL,
        JIKAN_SEARCH_URL,
        &cache_root_dir(),
    )
    .await
}

pub async fn fetch_anilist_metadata_with_opts(
    client: &Client,
    title: &str,
    endpoint: &str,
    cache_root: &Path,
) -> Option<AnimeMetadata> {
    fetch_metadata_with_endpoints(client, title, endpoint, JIKAN_SEARCH_URL, cache_root).await
}

pub async fn fetch_metadata_with_endpoints(
    client: &Client,
    raw_title: &str,
    anilist_endpoint: &str,
    jikan_endpoint: &str,
    cache_root: &Path,
) -> Option<AnimeMetadata> {
    let cleaned = clean_metadata_query(raw_title);
    if cleaned.is_empty() {
        return None;
    }

    let meta_path = cache_root
        .join("meta")
        .join(format!("{}.json", cache_slug(&cleaned)));
    let now_ts = Utc::now().timestamp();

    if let Some(cached) = load_cached_metadata(&meta_path, now_ts) {
        return Some(cached);
    }

    let payload = json!({
        "query": ANILIST_MEDIA_QUERY,
        "variables": { "search": cleaned }
    });

    let anilist_hit = match client
        .post(anilist_endpoint)
        .json(&payload)
        .timeout(ANILIST_TIMEOUT)
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => resp
            .text()
            .await
            .ok()
            .and_then(|body| parse_anilist_response(&body, &cleaned, now_ts)),
        _ => None,
    };

    let meta = match anilist_hit {
        Some(m) => m,
        None => fetch_jikan_fallback(client, &cleaned, jikan_endpoint, now_ts).await?,
    };

    if is_valid_metadata_for_cache(&meta) {
        let _ = save_cached_metadata(&meta_path, &meta);
    }
    Some(meta)
}

async fn fetch_jikan_fallback(
    client: &Client,
    cleaned_query: &str,
    jikan_base: &str,
    now_ts: i64,
) -> Option<AnimeMetadata> {
    let url = format!(
        "{}?q={}&limit=5",
        jikan_base.trim_end_matches('/'),
        urlencoding::encode(cleaned_query)
    );
    let resp = client.get(&url).timeout(JIKAN_TIMEOUT).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let body = resp.text().await.ok()?;
    parse_jikan_response(&body, cleaned_query, now_ts)
}

pub fn load_cached_metadata(path: &Path, now_ts: i64) -> Option<AnimeMetadata> {
    let raw = fs::read_to_string(path).ok()?;
    let meta: AnimeMetadata = serde_json::from_str(&raw).ok()?;
    if is_valid_metadata_for_cache(&meta)
        && now_ts.saturating_sub(meta.cached_at) <= META_CACHE_TTL_SECS
    {
        Some(meta)
    } else {
        None
    }
}

pub fn save_cached_metadata(path: &Path, meta: &AnimeMetadata) -> std::io::Result<()> {
    if !is_valid_metadata_for_cache(meta) {
        return Ok(());
    }
    let serialized = serde_json::to_string_pretty(meta).map_err(std::io::Error::other)?;
    atomic_write_bytes(path, serialized.as_bytes())
}

/// Episode title & canon/filler/recap flags retrieved from Jikan `/v4/anime/{mal_id}/episodes`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EpisodeMeta {
    pub number: u32,
    pub title: Option<String>,
    #[serde(default)]
    pub filler: bool,
    #[serde(default)]
    pub recap: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedEpisodeGuidePage {
    cached_at: i64,
    episodes: Vec<EpisodeMeta>,
}

#[derive(Debug, Deserialize)]
struct JikanEpisodesResp {
    #[serde(default)]
    data: Vec<JikanEpisodeItem>,
}

#[derive(Debug, Deserialize)]
struct JikanEpisodeItem {
    mal_id: Option<u32>,
    title: Option<String>,
    title_romanji: Option<String>,
    #[serde(default)]
    filler: bool,
    #[serde(default)]
    recap: bool,
}

/// Computes the 1-based Jikan episodes pagination page (`100` episodes per page).
pub fn episode_guide_page(episode_num: u32) -> u32 {
    (episode_num.saturating_sub(1) / 100) + 1
}

/// Parses Jikan v4 `/anime/{mal_id}/episodes` JSON into `EpisodeMeta` items.
pub fn parse_jikan_episodes_response(body: &str) -> Vec<EpisodeMeta> {
    let Ok(parsed) = serde_json::from_str::<JikanEpisodesResp>(body) else {
        return Vec::new();
    };
    parsed
        .data
        .into_iter()
        .filter_map(|item| {
            let number = item.mal_id.filter(|&n| n > 0)?;
            let raw_title = item
                .title
                .as_deref()
                .filter(|t| !t.trim().is_empty())
                .or_else(|| {
                    item.title_romanji
                        .as_deref()
                        .filter(|t| !t.trim().is_empty())
                })
                .map(|t| strip_html_tags(t.trim()));
            Some(EpisodeMeta {
                number,
                title: raw_title,
                filler: item.filler,
                recap: item.recap,
            })
        })
        .collect()
}

/// Fetches a 100-episode Jikan guide page with a `1.8s` timeout and 14-day atomic disk cache
/// at `~/.cache/senku-anime-cli/episodes/{mal_id}_p{page}.json`. Never blocks or fails.
pub async fn fetch_episode_guide(
    client: &Client,
    mal_id: u32,
    target_page: u32,
) -> HashMap<u32, EpisodeMeta> {
    fetch_episode_guide_with_opts(
        client,
        mal_id,
        target_page,
        JIKAN_SEARCH_URL,
        &cache_root_dir(),
    )
    .await
}

pub async fn fetch_episode_guide_with_opts(
    client: &Client,
    mal_id: u32,
    target_page: u32,
    jikan_base: &str,
    cache_root: &Path,
) -> HashMap<u32, EpisodeMeta> {
    let page = target_page.max(1);
    let cache_path = cache_root
        .join("episodes")
        .join(format!("{mal_id}_p{page}.json"));
    let now_ts = Utc::now().timestamp();

    if let Some(cached) = load_cached_episode_page(&cache_path, now_ts) {
        return cached.into_iter().map(|e| (e.number, e)).collect();
    }

    let url = format!(
        "{}/{mal_id}/episodes?page={page}",
        jikan_base.trim_end_matches('/')
    );
    let resp = match client
        .get(&url)
        .timeout(JIKAN_EPISODES_TIMEOUT)
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => r,
        _ => return HashMap::new(),
    };
    let Ok(body) = resp.text().await else {
        return HashMap::new();
    };
    let parsed = parse_jikan_episodes_response(&body);
    if !parsed.is_empty() {
        let _ = save_cached_episode_page(&cache_path, &parsed, now_ts);
    }
    parsed.into_iter().map(|e| (e.number, e)).collect()
}

/// Convenience helper resolving `idMal` (via cached/live AniList+Jikan lookup) and
/// fetching the Jikan episode guide page for `target_episode`.
pub async fn fetch_episode_guide_for_title(
    client: &Client,
    title: &str,
    target_episode: u32,
) -> HashMap<u32, EpisodeMeta> {
    let Some(meta) = fetch_anilist_metadata(client, title).await else {
        return HashMap::new();
    };
    let Some(mal_id) = meta.id_mal else {
        return HashMap::new();
    };
    fetch_episode_guide(client, mal_id, episode_guide_page(target_episode)).await
}

fn load_cached_episode_page(path: &Path, now_ts: i64) -> Option<Vec<EpisodeMeta>> {
    let raw = fs::read_to_string(path).ok()?;
    let page: CachedEpisodeGuidePage = serde_json::from_str(&raw).ok()?;
    if !page.episodes.is_empty()
        && now_ts.saturating_sub(page.cached_at) <= EPISODE_GUIDE_CACHE_TTL_SECS
    {
        Some(page.episodes)
    } else {
        None
    }
}

fn save_cached_episode_page(
    path: &Path,
    episodes: &[EpisodeMeta],
    now_ts: i64,
) -> std::io::Result<()> {
    if episodes.is_empty() {
        return Ok(());
    }
    let wrapper = CachedEpisodeGuidePage {
        cached_at: now_ts,
        episodes: episodes.to_vec(),
    };
    let serialized = serde_json::to_string_pretty(&wrapper).map_err(std::io::Error::other)?;
    atomic_write_bytes(path, serialized.as_bytes())
}

#[derive(Debug, Deserialize)]
struct AniSkipResp {
    #[serde(default)]
    found: bool,
    #[serde(default)]
    results: Vec<AniSkipItem>,
}

#[derive(Debug, Deserialize)]
struct AniSkipItem {
    interval: AniSkipIntervalJson,
    #[serde(rename = "skipType")]
    skip_type: String,
    #[serde(rename = "episodeLength", default)]
    episode_length: f64,
}

#[derive(Debug, Deserialize)]
struct AniSkipIntervalJson {
    #[serde(rename = "startTime")]
    start_time: f64,
    #[serde(rename = "endTime")]
    end_time: f64,
}

/// Parses an AniSkip v2 JSON payload into validated, sorted `SkipInterval` entries.
pub fn parse_aniskip_response(body: &str) -> Vec<SkipInterval> {
    let Ok(parsed) = serde_json::from_str::<AniSkipResp>(body) else {
        return Vec::new();
    };
    if !parsed.found {
        return Vec::new();
    }

    let mut items: Vec<SkipInterval> = parsed
        .results
        .into_iter()
        .filter_map(|item| {
            let skip_type = SkipType::from_api_str(&item.skip_type)?;
            let start = item.interval.start_time.max(0.0);
            let end = item.interval.end_time;
            if !start.is_finite() || !end.is_finite() || end <= start {
                return None;
            }
            Some(SkipInterval {
                skip_type,
                start_time: start,
                end_time: end,
                episode_length: item.episode_length.max(0.0),
            })
        })
        .collect();

    items.sort_by(|a, b| a.start_time.total_cmp(&b.start_time));
    items
}

/// Queries the public AniSkip API with a strict 2.5s timeout.
pub async fn fetch_aniskip_times(client: &Client, mal_id: u32, episode: u32) -> Vec<SkipInterval> {
    fetch_aniskip_times_with_base(client, ANISKIP_API_BASE, mal_id, episode).await
}

pub async fn fetch_aniskip_times_with_base(
    client: &Client,
    api_base: &str,
    mal_id: u32,
    episode: u32,
) -> Vec<SkipInterval> {
    let url = format!(
        "{}/{mal_id}/{episode}?types=op&types=ed&types=recap&episodeLength=0",
        api_base.trim_end_matches('/')
    );
    let Ok(resp) = client.get(&url).timeout(ANISKIP_TIMEOUT).send().await else {
        return Vec::new();
    };
    if !resp.status().is_success() {
        return Vec::new();
    }
    let Ok(body) = resp.text().await else {
        return Vec::new();
    };
    parse_aniskip_response(&body)
}

/// Non-blocking combined AniList `idMal` lookup + AniSkip interval fetch for an episode.
pub async fn fetch_episode_skips(
    client: &Client,
    anime_title: &str,
    episode: u32,
) -> Vec<SkipInterval> {
    let Some(meta) = fetch_anilist_metadata(client, anime_title).await else {
        return Vec::new();
    };
    let Some(mal_id) = meta.id_mal else {
        return Vec::new();
    };
    fetch_aniskip_times(client, mal_id, episode).await
}

/// Builds an ordered `mpv` chapter list (`Intro`, `Opening (OP)`, `Episode`,
/// `Outro (ED)`, `Recap`) from AniSkip intervals.
pub fn build_chapter_list(intervals: &[SkipInterval]) -> Vec<ChapterEntry> {
    if intervals.is_empty() {
        return Vec::new();
    }

    let mut sorted = intervals.to_vec();
    sorted.sort_by(|a, b| a.start_time.total_cmp(&b.start_time));

    let mut raw_chapters: Vec<ChapterEntry> = Vec::new();

    if sorted[0].start_time > 2.0 {
        raw_chapters.push(ChapterEntry {
            title: "Intro".to_string(),
            time: 0.0,
        });
    }

    for (i, item) in sorted.iter().enumerate() {
        let label = match item.skip_type {
            SkipType::Op | SkipType::MixedOp => "Opening (OP)",
            SkipType::Ed | SkipType::MixedEd => "Outro (ED)",
            SkipType::Recap => "Recap",
        };
        raw_chapters.push(ChapterEntry {
            title: label.to_string(),
            time: item.start_time,
        });

        let next_start = sorted.get(i + 1).map(|n| n.start_time);
        let has_gap_after = match next_start {
            Some(ns) => ns - item.end_time > 2.0,
            None => item.episode_length > item.end_time + 2.0 || item.skip_type.is_op(),
        };
        if has_gap_after {
            let after_label = if item.skip_type.is_ed() {
                "Post-Credits"
            } else {
                "Episode"
            };
            raw_chapters.push(ChapterEntry {
                title: after_label.to_string(),
                time: item.end_time,
            });
        }
    }

    // Deduplicate timestamps within 0.5s while keeping chronological order.
    let mut clean: Vec<ChapterEntry> = Vec::with_capacity(raw_chapters.len());
    for entry in raw_chapters {
        if let Some(last) = clean.last_mut() {
            if (entry.time - last.time).abs() < 0.5 {
                // Prefer named OP/ED/Recap over generic Intro/Episode if same timestamp.
                if last.title == "Intro" || last.title == "Episode" {
                    *last = entry;
                }
                continue;
            }
        }
        clean.push(entry);
    }

    clean
}

/// Converts chapters into JSON array suitable for `mpv`'s `chapter-list` property.
pub fn chapters_to_mpv_json(chapters: &[ChapterEntry]) -> Value {
    Value::Array(
        chapters
            .iter()
            .map(|c| {
                json!({
                    "title": c.title,
                    "time": c.time,
                })
            })
            .collect(),
    )
}

/// Computes the exact `[i]` Skip Intro hotkey action:
/// - If playback is before or inside a detected `op` interval (`time_pos < op.end_time`),
///   seeks directly to `op.end_time` with OSD `"Skipped Opening (exact)"`.
/// - Otherwise falls back cleanly to relative `+skip_intro_secs`.
pub fn resolve_skip_intro_action(
    time_pos: f64,
    intervals: &[SkipInterval],
    fallback_secs: u64,
) -> SkipTarget {
    if let Some(op) = intervals.iter().find(|i| i.skip_type.is_op()) {
        if time_pos < op.end_time - 0.25 {
            return SkipTarget::ExactSeek {
                target_secs: op.end_time,
                label: "Skipped Opening (exact)",
            };
        }
    }
    SkipTarget::RelativeFallback {
        delta_secs: fallback_secs,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn parses_aniskip_response_builds_chapter_list_and_resolves_skip_target() {
        let payload = r#"{
          "found": true,
          "results": [
            {
              "interval": { "startTime": 1315.5, "endTime": 1405.5 },
              "skipType": "ed",
              "skipId": "ed-1",
              "episodeLength": 1420.0
            },
            {
              "interval": { "startTime": 92.0, "endTime": 182.0 },
              "skipType": "op",
              "skipId": "op-1",
              "episodeLength": 1420.0
            }
          ]
        }"#;

        let intervals = parse_aniskip_response(payload);
        assert_eq!(intervals.len(), 2);
        assert!(intervals[0].skip_type.is_op());
        assert!(intervals[1].skip_type.is_ed());

        let chapters = build_chapter_list(&intervals);
        assert_eq!(
            chapters,
            vec![
                ChapterEntry {
                    title: "Intro".into(),
                    time: 0.0
                },
                ChapterEntry {
                    title: "Opening (OP)".into(),
                    time: 92.0
                },
                ChapterEntry {
                    title: "Episode".into(),
                    time: 182.0
                },
                ChapterEntry {
                    title: "Outro (ED)".into(),
                    time: 1315.5
                },
                ChapterEntry {
                    title: "Post-Credits".into(),
                    time: 1405.5
                },
            ]
        );

        let json_val = chapters_to_mpv_json(&chapters);
        let arr = json_val.as_array().unwrap();
        assert_eq!(arr[1]["title"], "Opening (OP)");
        assert_eq!(arr[1]["time"], 92.0);

        // Before OP -> jumps cleanly to op.end_time (182.0)
        assert_eq!(
            resolve_skip_intro_action(15.0, &intervals, 85),
            SkipTarget::ExactSeek {
                target_secs: 182.0,
                label: "Skipped Opening (exact)",
            }
        );
        // Inside OP -> jumps cleanly to op.end_time (182.0)
        assert_eq!(
            resolve_skip_intro_action(110.0, &intervals, 85),
            SkipTarget::ExactSeek {
                target_secs: 182.0,
                label: "Skipped Opening (exact)",
            }
        );
        // Already past OP -> falls back to relative +85s
        assert_eq!(
            resolve_skip_intro_action(250.0, &intervals, 85),
            SkipTarget::RelativeFallback { delta_secs: 85 }
        );
        // No OP intervals at all -> falls back to relative +85s
        assert_eq!(
            resolve_skip_intro_action(10.0, &[], 85),
            SkipTarget::RelativeFallback { delta_secs: 85 }
        );
    }

    #[test]
    fn parses_anilist_response_strips_html_and_round_trips_cache() {
        let graphql_json = r#"{
          "data": {
            "Media": {
              "idMal": 9253,
              "title": { "romaji": "Steins;Gate", "english": "Steins;Gate" },
              "averageScore": 91,
              "episodes": 24,
              "status": "FINISHED",
              "seasonYear": 2011,
              "genres": ["Drama", "Sci-Fi", "Thriller"],
              "description": "Self-proclaimed <i>mad scientist</i> Okabe Rintarou.<br><br>El &amp; Psy Kongroo.",
              "coverImage": { "large": "https://s4.anilist.co/file/anilistcdn/media/anime/cover/medium/bx9253.jpg" }
            }
          }
        }"#;

        let now = 1_700_000_000;
        let meta = parse_anilist_response(graphql_json, "Steins;Gate", now).unwrap();
        assert_eq!(meta.id_mal, Some(9253));
        assert_eq!(meta.title_romaji, "Steins;Gate");
        assert_eq!(meta.average_score, Some(91));
        assert_eq!(meta.episodes, Some(24));
        assert_eq!(meta.season_year, Some(2011));
        assert_eq!(meta.genres, vec!["Drama", "Sci-Fi", "Thriller"]);
        assert_eq!(
            meta.description,
            "Self-proclaimed mad scientist Okabe Rintarou. El & Psy Kongroo."
        );

        let dir = tempdir().unwrap();
        let cache_file = dir
            .path()
            .join("meta")
            .join(format!("{}.json", cache_slug(&meta.title_romaji)));
        save_cached_metadata(&cache_file, &meta).unwrap();

        // Within 7-day TTL -> loads from disk
        let loaded = load_cached_metadata(&cache_file, now + 3 * 86_400).unwrap();
        assert_eq!(loaded, meta);

        // Expired after 7 days -> returns None
        assert!(load_cached_metadata(&cache_file, now + META_CACHE_TTL_SECS + 10).is_none());
    }

    #[test]
    fn scores_multi_candidate_anilist_decodes_entities_and_falls_back_to_jikan() {
        assert_eq!(
            clean_metadata_query("  2 | [OKA] انمي Dragon Ball Super: Beerus مترجم (2026) (142ms)"),
            "Dragon Ball Super: Beerus"
        );
        assert_eq!(
            strip_html_tags("<i>Hero&#039;s</i> journey &mdash; part &#x32;&ndash;3&hellip;"),
            "Hero's journey — part 2–3..."
        );

        let page_json = r#"{
          "data": {
            "Page": {
              "media": [
                {
                  "idMal": 30694,
                  "title": { "romaji": "Dragon Ball Super", "english": "Dragon Ball Super", "native": "ドラゴンボール超" },
                  "synonyms": ["DBS"],
                  "averageScore": 75,
                  "episodes": 131,
                  "status": "FINISHED",
                  "seasonYear": 2015,
                  "genres": ["Action"],
                  "description": "Original series.",
                  "coverImage": { "large": "https://img/orig.jpg" }
                },
                {
                  "idMal": 99999,
                  "title": { "romaji": "Dragon Ball Super: Beerus", "english": "Dragon Ball Super: Beerus", "native": null },
                  "synonyms": [],
                  "averageScore": 82,
                  "episodes": null,
                  "nextAiringEpisode": { "episode": 15 },
                  "status": "RELEASING",
                  "seasonYear": 2026,
                  "genres": ["Action"],
                  "description": "Beerus arc &mdash; airing now.",
                  "coverImage": { "large": "https://img/beerus.jpg" }
                }
              ]
            }
          }
        }"#;

        let chosen = parse_anilist_response(page_json, "Dragon Ball Super: Beerus", 1000).unwrap();
        assert_eq!(chosen.id_mal, Some(99999));
        assert_eq!(chosen.title_romaji, "Dragon Ball Super: Beerus");
        assert_eq!(chosen.next_airing_episode, Some(15));
        assert_eq!(chosen.description, "Beerus arc — airing now.");

        // Jikan fallback parsing & scoring
        let jikan_json = r#"{
          "data": [
            {
              "mal_id": 21,
              "title": "One Piece Film: Red",
              "title_english": "One Piece Film: Red",
              "score": 7.8,
              "episodes": 1,
              "status": "Finished Airing",
              "year": 2022,
              "genres": [{"name": "Adventure"}],
              "synopsis": "Uta &mdash; the most beloved singer.",
              "images": { "jpg": { "large_image_url": "https://cdn.myanimelist.net/red.jpg" } }
            },
            {
              "mal_id": 22,
              "title": "One Piece",
              "title_english": "One Piece",
              "score": 8.7,
              "episodes": null,
              "status": "Currently Airing",
              "year": 1999,
              "genres": [{"name": "Action"}],
              "synopsis": "Gol D. Roger&#039;s treasure.",
              "images": { "jpg": { "large_image_url": "https://cdn.myanimelist.net/op.jpg" } }
            }
          ]
        }"#;
        let jikan = parse_jikan_response(jikan_json, "One Piece", 2000).unwrap();
        assert_eq!(jikan.id_mal, Some(22));
        assert_eq!(jikan.average_score, Some(87));
        assert_eq!(jikan.description, "Gol D. Roger's treasure.");
    }

    #[tokio::test]
    async fn preview_cache_hit_returns_immediately_and_miss_applies_debounce() {
        let dir = tempdir().unwrap();
        let now = Utc::now().timestamp();
        let meta = AnimeMetadata {
            id_mal: Some(9253),
            title_romaji: "Steins;Gate".into(),
            title_english: Some("Steins;Gate".into()),
            average_score: Some(91),
            episodes: Some(24),
            next_airing_episode: None,
            status: Some("FINISHED".into()),
            season_year: Some(2011),
            genres: vec!["Sci-Fi".into()],
            description: "Time travel.".into(),
            cover_image_url: None,
            cached_at: now,
        };
        let path = dir
            .path()
            .join("meta")
            .join(format!("{}.json", cache_slug("Steins;Gate")));
        save_cached_metadata(&path, &meta).unwrap();

        let client = Client::builder().build().unwrap();
        let started_hit = std::time::Instant::now();
        let hit = fetch_preview_metadata_with_opts(
            &client,
            "Steins;Gate",
            "http://127.0.0.1:9/unused",
            "http://127.0.0.1:9/unused",
            dir.path(),
            Duration::from_millis(120),
        )
        .await
        .unwrap();
        let hit_elapsed = started_hit.elapsed();
        assert_eq!(hit.id_mal, Some(9253));
        assert!(
            hit_elapsed < Duration::from_millis(40),
            "cache hit must bypass 120ms debounce, took {hit_elapsed:?}"
        );

        let started_miss = std::time::Instant::now();
        let miss = fetch_preview_metadata_with_opts(
            &client,
            "Uncached Anime Title",
            "http://127.0.0.1:9/fail",
            "http://127.0.0.1:9/fail",
            dir.path(),
            Duration::from_millis(90),
        )
        .await;
        assert!(miss.is_none());
        assert!(started_miss.elapsed() >= Duration::from_millis(80));
    }

    #[tokio::test]
    async fn parses_jikan_episodes_guide_and_round_trips_cache() {
        let dir = tempdir().unwrap();
        let client = Client::builder().build().unwrap();
        let json = r#"{
          "pagination": { "last_visible_page": 2, "has_next_page": false },
          "data": [
            { "mal_id": 129, "title": "Formidable Enemy and Formidable Ally", "filler": false, "recap": false },
            { "mal_id": 130, "title": "Magic to Destroy", "filler": false, "recap": false },
            { "mal_id": 132, "title": "Flash and Start &amp; Burn", "filler": true, "recap": false },
            { "mal_id": 133, "title": null, "title_romanji": "Recap Special", "filler": false, "recap": true }
          ]
        }"#;

        let parsed = parse_jikan_episodes_response(json);
        assert_eq!(parsed.len(), 4);
        assert_eq!(parsed[0].number, 129);
        assert!(!parsed[0].filler);
        assert_eq!(parsed[2].title.as_deref(), Some("Flash and Start & Burn"));
        assert!(parsed[2].filler);
        assert!(parsed[3].recap);
        assert_eq!(parsed[3].title.as_deref(), Some("Recap Special"));

        assert_eq!(episode_guide_page(1), 1);
        assert_eq!(episode_guide_page(100), 1);
        assert_eq!(episode_guide_page(130), 2);

        let cache_path = dir.path().join("episodes").join("11061_p2.json");
        save_cached_episode_page(&cache_path, &parsed, Utc::now().timestamp()).unwrap();

        // Hit should load directly from 14-day disk cache without touching dead endpoint.
        let loaded = fetch_episode_guide_with_opts(
            &client,
            11061,
            2,
            "http://127.0.0.1:9/unused",
            dir.path(),
        )
        .await;
        assert_eq!(loaded.len(), 4);
        assert_eq!(
            loaded.get(&130).and_then(|e| e.title.as_deref()),
            Some("Magic to Destroy")
        );
        assert!(loaded.get(&132).is_some_and(|e| e.filler));
    }

    #[test]
    fn hardens_cache_slug_against_traversal_and_name_max() {
        assert_eq!(cache_slug("../../etc/passwd\0"), "etc-passwd");
        assert!(!cache_slug("..hidden").starts_with('.'));
        let long_a = format!("{} alpha", "a".repeat(260));
        let long_b = format!("{} beta", "a".repeat(260));
        let slug_a = cache_slug(&long_a);
        let slug_b = cache_slug(&long_b);
        assert!(slug_a.len() <= 160);
        assert!(slug_b.len() <= 160);
        assert_ne!(slug_a, slug_b, "truncated slugs must keep distinct hashes");
    }
}
