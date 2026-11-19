use crate::error::{Result, SenkuError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;

/// Supported Arabic anime scraping backends.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProviderId {
    Anime3rb,
    AnimeSlayer,
    Animeify,
    Animerco,
    OkAnime,
}

impl ProviderId {
    pub fn display_name(self) -> &'static str {
        match self {
            ProviderId::Anime3rb => "Anime3rb",
            ProviderId::AnimeSlayer => "AnimeSlayer",
            ProviderId::Animeify => "Animeify",
            ProviderId::Animerco => "Animerco",
            ProviderId::OkAnime => "OkAnime",
        }
    }

    pub fn short_tag(self) -> &'static str {
        match self {
            ProviderId::Anime3rb => "3RB",
            ProviderId::AnimeSlayer => "SLY",
            ProviderId::Animeify => "IFY",
            ProviderId::Animerco => "MRC",
            ProviderId::OkAnime => "OKA",
        }
    }
}

impl fmt::Display for ProviderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.display_name())
    }
}

impl FromStr for ProviderId {
    type Err = SenkuError;

    fn from_str(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "anime3rb" | "3rb" => Ok(Self::Anime3rb),
            "animeslayer" | "anime-slayer" | "slayer" => Ok(Self::AnimeSlayer),
            "animeify" | "ify" => Ok(Self::Animeify),
            "animerco" | "mrc" => Ok(Self::Animerco),
            "okanime" | "oka" => Ok(Self::OkAnime),
            other => Err(SenkuError::ProviderFailure {
                provider: other.to_string(),
                reason:
                    "unknown provider (valid: animerco, okanime, anime3rb, animeslayer, animeify)"
                        .to_string(),
            }),
        }
    }
}

/// A single provider catalog source belonging to a fused anime entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnimeSource {
    pub provider: ProviderId,
    pub slug: String,
    pub url: String,
    pub latency_ms: u64,
    #[serde(default)]
    pub episode_count: Option<u32>,
}

/// Unified anime catalog entry (optionally fused across multiple providers).
pub type AnimeEntry = AnimeResult;

/// Normalized anime search result returned by a provider (or fused across providers).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnimeResult {
    pub slug: String,
    pub title: String,
    pub alt_title: Option<String>,
    pub url: String,
    pub provider: ProviderId,
    pub year: Option<String>,
    pub episode_count: Option<u32>,
    pub latency_ms: u64,
    #[serde(default)]
    pub sources: Vec<AnimeSource>,
}

impl AnimeResult {
    /// Returns all backing provider sources, falling back to this entry's
    /// primary `(provider, slug, url)` when `sources` is empty (e.g. legacy `history.json`).
    pub fn all_sources(&self) -> Vec<AnimeSource> {
        if !self.sources.is_empty() {
            return self.sources.clone();
        }
        vec![AnimeSource {
            provider: self.provider,
            slug: self.slug.clone(),
            url: self.url.clone(),
            latency_ms: self.latency_ms,
            episode_count: self.episode_count,
        }]
    }

    /// Combined short badge for Portal Fusion rows, e.g. `"MRC+OKA"` or `"OKA"`.
    pub fn provider_badge(&self) -> String {
        let mut tags: Vec<&'static str> = Vec::new();
        for src in self.all_sources() {
            let tag = src.provider.short_tag();
            if !tags.contains(&tag) {
                tags.push(tag);
            }
        }
        tags.join("+")
    }

    /// Combined human-readable provider list, e.g. `"Animerco + OkAnime"`.
    pub fn provider_display(&self) -> String {
        let mut names: Vec<&'static str> = Vec::new();
        for src in self.all_sources() {
            let name = src.provider.display_name();
            if !names.contains(&name) {
                names.push(name);
            }
        }
        names.join(" + ")
    }
}

/// A single provider endpoint hosting an episode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpisodeSource {
    pub provider: ProviderId,
    pub url: String,
}

/// A single anime episode entry (optionally pooled across multiple providers).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Episode {
    pub number: u32,
    pub title: Option<String>,
    pub url: String,
    pub provider: ProviderId,
    #[serde(default)]
    pub sources: Vec<EpisodeSource>,
}

impl Episode {
    /// Returns all provider episode URLs for concurrent pooled stream extraction.
    pub fn all_sources(&self) -> Vec<EpisodeSource> {
        if !self.sources.is_empty() {
            return self.sources.clone();
        }
        vec![EpisodeSource {
            provider: self.provider,
            url: self.url.clone(),
        }]
    }
}

/// Video stream resolution ordered from lowest to highest quality so `Ord::max`
/// picks the highest available resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Resolution {
    Unknown,
    Auto,
    Sd360,
    Sd480,
    Hd720,
    Fhd1080,
    Qhd1440,
    Uhd2160,
    Custom(u32),
}

impl Resolution {
    /// Returns the numeric vertical pixel height when known.
    pub fn height(self) -> u32 {
        match self {
            Resolution::Unknown => 0,
            Resolution::Auto => 1,
            Resolution::Sd360 => 360,
            Resolution::Sd480 => 480,
            Resolution::Hd720 => 720,
            Resolution::Fhd1080 => 1080,
            Resolution::Qhd1440 => 1440,
            Resolution::Uhd2160 => 2160,
            Resolution::Custom(h) => h,
        }
    }

    /// Parses resolution labels embedded inside arbitrary strings like
    /// `"FHD 1080p"`, `"Full HD"`, `"mp4upload HD"`, `"SD"`, `"720"`, or `"4K"`.
    /// Uses token boundaries for short tags (`hd`, `sd`, `fhd`, `uhd`, `4k`) so
    /// random alphanumeric URL hashes never trigger false matches.
    pub fn parse_from_label(label: &str) -> Self {
        let lower = label.to_ascii_lowercase();
        let tokens: Vec<&str> = lower
            .split(|c: char| !c.is_ascii_alphanumeric())
            .filter(|t| !t.is_empty())
            .collect();
        let has_tok = |needle: &str| tokens.contains(&needle);

        if lower.contains("2160") || has_tok("4k") || has_tok("uhd") {
            return Resolution::Uhd2160;
        }
        if lower.contains("1440") || has_tok("2k") || has_tok("qhd") {
            return Resolution::Qhd1440;
        }
        if lower.contains("1080")
            || lower.contains("full hd")
            || lower.contains("full-hd")
            || lower.contains("full_hd")
            || has_tok("fullhd")
            || has_tok("fhd")
        {
            return Resolution::Fhd1080;
        }
        if lower.contains("720") || has_tok("hd") {
            return Resolution::Hd720;
        }
        if lower.contains("480") || has_tok("sd") {
            return Resolution::Sd480;
        }
        if lower.contains("360") {
            return Resolution::Sd360;
        }
        if has_tok("auto") || has_tok("hls") || has_tok("m3u8") {
            return Resolution::Auto;
        }
        Resolution::Unknown
    }
}

impl fmt::Display for Resolution {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Resolution::Uhd2160 => f.write_str("2160p"),
            Resolution::Qhd1440 => f.write_str("1440p"),
            Resolution::Fhd1080 => f.write_str("1080p"),
            Resolution::Hd720 => f.write_str("720p"),
            Resolution::Sd480 => f.write_str("480p"),
            Resolution::Sd360 => f.write_str("360p"),
            Resolution::Auto => f.write_str("auto"),
            Resolution::Unknown => f.write_str("unknown"),
            Resolution::Custom(h) => write!(f, "{h}p"),
        }
    }
}

impl FromStr for Resolution {
    type Err = SenkuError;

    fn from_str(s: &str) -> Result<Self> {
        let trimmed = s.trim().to_ascii_lowercase();
        let normalized = trimmed.strip_suffix('p').unwrap_or(&trimmed);
        match normalized {
            "2160" | "4k" | "uhd" => Ok(Resolution::Uhd2160),
            "1440" | "2k" | "qhd" => Ok(Resolution::Qhd1440),
            "1080" | "fhd" | "full hd" | "full-hd" | "fullhd" => Ok(Resolution::Fhd1080),
            "720" | "hd" => Ok(Resolution::Hd720),
            "480" | "sd" => Ok(Resolution::Sd480),
            "360" => Ok(Resolution::Sd360),
            "auto" => Ok(Resolution::Auto),
            other => {
                if let Ok(val) = other.parse::<u32>() {
                    if (144..=4320).contains(&val) {
                        return Ok(Resolution::Custom(val));
                    }
                }
                Err(SenkuError::InvalidResolution(s.to_string()))
            }
        }
    }
}

fn default_verified_true() -> bool {
    true
}

/// A resolved video stream candidate for an episode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamSource {
    pub url: String,
    pub resolution: Resolution,
    pub provider: ProviderId,
    pub server_name: String,
    pub referer: Option<String>,
    pub content_length: Option<u64>,
    #[serde(default = "default_verified_true")]
    pub verified: bool,
    #[serde(default)]
    pub throughput_kbps: Option<u64>,
    #[serde(default)]
    pub supports_ranges: bool,
}

/// Parsed episode selection such as `"5"`, `"1-12"`, or `"1,3,5-8"`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EpisodeSelection {
    pub episodes: Vec<u32>,
}

impl EpisodeSelection {
    pub fn parse(spec: &str) -> Result<Self> {
        let trimmed = spec.trim();
        if trimmed.is_empty() {
            return Err(SenkuError::InvalidEpisodeSpec(spec.to_string()));
        }

        let mut set = BTreeSet::new();
        for part in trimmed.split(',') {
            let token = part.trim();
            if token.is_empty() {
                return Err(SenkuError::InvalidEpisodeSpec(spec.to_string()));
            }
            if let Some((start_s, end_s)) = token.split_once('-') {
                let start: u32 = start_s
                    .trim()
                    .parse()
                    .map_err(|_| SenkuError::InvalidEpisodeSpec(spec.to_string()))?;
                let end: u32 = end_s
                    .trim()
                    .parse()
                    .map_err(|_| SenkuError::InvalidEpisodeSpec(spec.to_string()))?;
                if start == 0 || end < start || (end - start) > 5000 {
                    return Err(SenkuError::InvalidEpisodeSpec(spec.to_string()));
                }
                for ep in start..=end {
                    set.insert(ep);
                }
            } else {
                let ep: u32 = token
                    .parse()
                    .map_err(|_| SenkuError::InvalidEpisodeSpec(spec.to_string()))?;
                if ep == 0 {
                    return Err(SenkuError::InvalidEpisodeSpec(spec.to_string()));
                }
                set.insert(ep);
            }
        }

        if set.is_empty() {
            return Err(SenkuError::InvalidEpisodeSpec(spec.to_string()));
        }

        Ok(Self {
            episodes: set.into_iter().collect(),
        })
    }
}

/// Watch history entry persisted to `~/.local/share/senku-anime-cli/history.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchHistoryEntry {
    pub anime_slug: String,
    pub title: String,
    pub episode: u32,
    pub timestamp: i64,
    #[serde(default)]
    pub provider: Option<ProviderId>,
    #[serde(default)]
    pub anime_url: Option<String>,
    #[serde(default)]
    pub position_secs: Option<u64>,
    #[serde(default)]
    pub sources: Vec<AnimeSource>,
    #[serde(default)]
    pub watched_episodes: Vec<u32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolution_orders_highest_first_via_height() {
        let mut list = vec![
            Resolution::Sd480,
            Resolution::Uhd2160,
            Resolution::Sd360,
            Resolution::Fhd1080,
            Resolution::Hd720,
        ];
        list.sort_by_key(|r| std::cmp::Reverse(r.height()));
        assert_eq!(
            list,
            vec![
                Resolution::Uhd2160,
                Resolution::Fhd1080,
                Resolution::Hd720,
                Resolution::Sd480,
                Resolution::Sd360,
            ]
        );
    }

    #[test]
    fn parses_resolution_labels() {
        assert_eq!(
            Resolution::parse_from_label("FHD 1080p MP4"),
            Resolution::Fhd1080
        );
        assert_eq!(Resolution::parse_from_label("Full HD"), Resolution::Fhd1080);
        assert_eq!(
            Resolution::parse_from_label("Michi FHD"),
            Resolution::Fhd1080
        );
        assert_eq!(
            Resolution::parse_from_label("mp4upload HD"),
            Resolution::Hd720
        );
        assert_eq!(
            Resolution::parse_from_label("server-720"),
            Resolution::Hd720
        );
        assert_eq!(Resolution::parse_from_label("Mirror SD"), Resolution::Sd480);
        assert_eq!(Resolution::parse_from_label("480p"), Resolution::Sd480);
        // Random token containing `hd`/`sd` inside hash must not false-match.
        assert_eq!(
            Resolution::parse_from_label("https://vidcache.net/a2026sd123hd/video.mp4"),
            Resolution::Unknown
        );
        assert_eq!(Resolution::from_str("1080p").unwrap(), Resolution::Fhd1080);
        assert_eq!(
            Resolution::from_str("full hd").unwrap(),
            Resolution::Fhd1080
        );
        assert_eq!(Resolution::from_str("hd").unwrap(), Resolution::Hd720);
        assert_eq!(Resolution::from_str("sd").unwrap(), Resolution::Sd480);
        assert_eq!(Resolution::from_str("720").unwrap(), Resolution::Hd720);
        assert!(Resolution::from_str("potato").is_err());
    }

    #[test]
    fn parses_episode_ranges_and_lists() {
        let single = EpisodeSelection::parse("7").unwrap();
        assert_eq!(single.episodes, vec![7]);

        let range = EpisodeSelection::parse("1-4").unwrap();
        assert_eq!(range.episodes, vec![1, 2, 3, 4]);

        let mixed = EpisodeSelection::parse("1, 3, 5-7, 3").unwrap();
        assert_eq!(mixed.episodes, vec![1, 3, 5, 6, 7]);

        assert!(EpisodeSelection::parse("0").is_err());
        assert!(EpisodeSelection::parse("12-3").is_err());
        assert!(EpisodeSelection::parse("").is_err());
    }
}
