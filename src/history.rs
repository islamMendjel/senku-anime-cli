use crate::error::{Result, SenkuError};
use crate::metadata::{cache_root_dir, load_cached_metadata_for_title};
use crate::models::{AnimeResult, WatchHistoryEntry};
use crate::providers::ProviderEngine;
use chrono::{DateTime, Utc};
use owo_colors::OwoColorize;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;

/// New episode radar discovery for a watched history entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewEpisodeAlert {
    pub anime_slug: String,
    pub title: String,
    pub watched_episode: u32,
    pub latest_available: u32,
    pub next_episode_to_watch: u32,
}

/// Evaluates whether a newer episode is available beyond `entry.episode`
/// using provider episode lists and optional AniList `nextAiringEpisode`.
pub fn detect_new_episode(
    entry: &WatchHistoryEntry,
    provider_latest: Option<u32>,
    anilist_next_airing: Option<u32>,
) -> Option<NewEpisodeAlert> {
    let from_anilist = anilist_next_airing.filter(|&n| n > 1).map(|n| n - 1);
    let effective_latest = provider_latest.into_iter().chain(from_anilist).max()?;
    if effective_latest > entry.episode {
        Some(NewEpisodeAlert {
            anime_slug: entry.anime_slug.clone(),
            title: entry.title.clone(),
            watched_episode: entry.episode,
            latest_available: effective_latest,
            next_episode_to_watch: entry.episode + 1,
        })
    } else {
        None
    }
}

/// Formats a Continue Watching row label, highlighting newly dropped episodes
/// with `✦ Ep NN NEW!`.
pub fn format_history_row_label(
    index: usize,
    item: &WatchHistoryEntry,
    alert: Option<&NewEpisodeAlert>,
) -> String {
    let dt = DateTime::<Utc>::from_timestamp(item.timestamp, 0)
        .map(|d| d.format("%Y-%m-%d %H:%M UTC").to_string())
        .unwrap_or_else(|| item.timestamp.to_string());
    let prov = item.provider.map(|p| p.short_tag()).unwrap_or("?");
    let pos_part = item
        .position_secs
        .filter(|&s| s > 10)
        .map(|s| format!(" @ {:02}:{:02}", s / 60, s % 60))
        .unwrap_or_default();
    let new_badge = alert
        .map(|a| {
            format!(
                "  {}",
                format!("✦ Ep {:02} NEW!", a.next_episode_to_watch)
                    .bright_green()
                    .bold()
            )
        })
        .unwrap_or_default();

    format!(
        "{:>3} | [{}] {} — Ep {:02}{}{} ({})",
        index + 1,
        prov,
        item.title,
        item.episode,
        pos_part,
        new_badge,
        dt
    )
}

/// Concurrent, non-blocking New Episode Radar over up to the top 6 recent entries,
/// capped at a `2.0s` overall wall-clock timeout and memoized in `session_cache`.
pub async fn check_new_episodes(
    entries: &[WatchHistoryEntry],
    engine: &Arc<ProviderEngine>,
    session_cache: &mut HashMap<String, Option<NewEpisodeAlert>>,
) -> HashMap<String, NewEpisodeAlert> {
    check_new_episodes_with_cache(entries, engine, &cache_root_dir(), session_cache).await
}

pub async fn check_new_episodes_with_cache(
    entries: &[WatchHistoryEntry],
    engine: &Arc<ProviderEngine>,
    cache_root: &Path,
    session_cache: &mut HashMap<String, Option<NewEpisodeAlert>>,
) -> HashMap<String, NewEpisodeAlert> {
    let mut set = JoinSet::new();
    for entry in entries.iter().take(6) {
        if session_cache.contains_key(&entry.anime_slug) {
            continue;
        }
        let e = entry.clone();
        let eng = Arc::clone(engine);
        let cache_dir = cache_root.to_path_buf();
        set.spawn(async move {
            let provider_latest = if let (Some(url), Some(prov)) = (e.anime_url.clone(), e.provider)
            {
                let dummy = AnimeResult {
                    slug: e.anime_slug.clone(),
                    title: e.title.clone(),
                    alt_title: None,
                    url,
                    provider: prov,
                    year: None,
                    episode_count: None,
                    latency_ms: 0,
                    sources: e.sources.clone(),
                };
                eng.get_episodes(&dummy)
                    .await
                    .ok()
                    .and_then(|eps| eps.into_iter().map(|ep| ep.number).max())
            } else {
                None
            };

            let anilist_next = load_cached_metadata_for_title(&e.title, &cache_dir)
                .and_then(|m| m.next_airing_episode);

            let alert = detect_new_episode(&e, provider_latest, anilist_next);
            (e.anime_slug, alert)
        });
    }

    let deadline = tokio::time::sleep(Duration::from_secs(2));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => {
                set.abort_all();
                break;
            }
            joined = set.join_next() => {
                match joined {
                    Some(Ok((slug, maybe_alert))) => {
                        session_cache.insert(slug, maybe_alert);
                    }
                    Some(Err(_)) => {}
                    None => break,
                }
            }
        }
    }

    entries
        .iter()
        .take(6)
        .filter_map(|e| {
            session_cache
                .get(&e.anime_slug)
                .and_then(|opt| opt.clone())
                .map(|a| (e.anime_slug.clone(), a))
        })
        .collect()
}

/// Manages local watch history at `~/.local/share/senku-anime-cli/history.json`.
/// Zero telemetry; everything remains strictly on the local filesystem.
#[derive(Debug, Clone)]
pub struct HistoryStore {
    path: PathBuf,
}

impl HistoryStore {
    /// Resolves `$XDG_DATA_HOME/senku-anime-cli/history.json` or
    /// `~/.local/share/senku-anime-cli/history.json`.
    pub fn default_store() -> Result<Self> {
        let base = dirs::data_local_dir()
            .or_else(|| dirs::home_dir().map(|h| h.join(".local/share")))
            .ok_or_else(|| {
                SenkuError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "cannot determine user data directory",
                ))
            })?;
        Ok(Self {
            path: base.join("senku-anime-cli").join("history.json"),
        })
    }

    pub fn with_path(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn backup_path(&self) -> PathBuf {
        let mut os = self.path.as_os_str().to_os_string();
        os.push(".bak");
        PathBuf::from(os)
    }

    /// Loads watch history sorted newest-first. If `history.json` is empty,
    /// truncated, invalid UTF-8, or malformed JSON, backs it up to `history.json.bak`
    /// and initializes a clean store instead of crashing.
    pub fn load_all(&self) -> Result<Vec<WatchHistoryEntry>> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        let raw_bytes = fs::read(&self.path)?;
        if raw_bytes.iter().all(|b| b.is_ascii_whitespace()) {
            return Ok(Vec::new());
        }

        let parse_res: std::result::Result<Vec<WatchHistoryEntry>, String> =
            match std::str::from_utf8(&raw_bytes) {
                Ok(raw_str) => serde_json::from_str(raw_str).map_err(|e| e.to_string()),
                Err(utf8_err) => Err(utf8_err.to_string()),
            };

        match parse_res {
            Ok(mut entries) => {
                entries.sort_by_key(|a| std::cmp::Reverse(a.timestamp));
                Ok(entries)
            }
            Err(parse_err) => {
                let bak = self.backup_path();
                if let Err(copy_err) = fs::copy(&self.path, &bak) {
                    eprintln!(
                        "[WARN] Corrupt watch history ({parse_err}); backup to {} failed: {copy_err}",
                        bak.display()
                    );
                } else {
                    eprintln!(
                        "[WARN] Corrupt watch history ({parse_err}); backed up to {} and reinitialized",
                        bak.display()
                    );
                }
                self.save_atomic(&[])?;
                Ok(Vec::new())
            }
        }
    }

    pub fn record_watch(&self, anime: &AnimeResult, episode: u32) -> Result<()> {
        self.record_watch_at(anime, episode, None)
    }

    /// Logs watch state including exact playback offset (`position_secs`),
    /// preserving previously recorded `watched_episodes`.
    pub fn record_watch_at(
        &self,
        anime: &AnimeResult,
        episode: u32,
        position_secs: Option<u64>,
    ) -> Result<()> {
        self.record_watch_state(anime, episode, position_secs, false)
    }

    /// Logs watch state and optionally marks `episode` as completed/watched inside
    /// `watched_episodes` (sorted and deduplicated).
    pub fn record_watch_state(
        &self,
        anime: &AnimeResult,
        episode: u32,
        position_secs: Option<u64>,
        mark_watched: bool,
    ) -> Result<()> {
        let mut entries = self.load_all().unwrap_or_default();
        let mut watched_episodes = entries
            .iter()
            .find(|e| e.anime_slug == anime.slug)
            .map(|e| e.watched_episodes.clone())
            .unwrap_or_default();
        if mark_watched && !watched_episodes.contains(&episode) {
            watched_episodes.push(episode);
        }
        watched_episodes.sort_unstable();
        watched_episodes.dedup();

        entries.retain(|e| e.anime_slug != anime.slug);

        entries.insert(
            0,
            WatchHistoryEntry {
                anime_slug: anime.slug.clone(),
                title: anime.title.clone(),
                episode,
                timestamp: Utc::now().timestamp(),
                provider: Some(anime.provider),
                anime_url: Some(anime.url.clone()),
                position_secs,
                sources: anime.all_sources(),
                watched_episodes,
            },
        );

        // Keep history tidy (max 200 entries).
        entries.truncate(200);
        self.save_atomic(&entries)
    }

    pub fn remove_entry(&self, anime_slug: &str) -> Result<bool> {
        let mut entries = self.load_all()?;
        let before = entries.len();
        entries.retain(|e| e.anime_slug != anime_slug);
        if entries.len() == before {
            return Ok(false);
        }
        self.save_atomic(&entries)?;
        Ok(true)
    }

    pub fn latest(&self) -> Result<WatchHistoryEntry> {
        let entries = self.load_all()?;
        entries
            .into_iter()
            .next()
            .ok_or_else(|| SenkuError::EmptyHistory {
                path: self.path.display().to_string(),
            })
    }

    pub fn clear(&self) -> Result<()> {
        if self.path.exists() {
            fs::remove_file(&self.path)?;
        }
        Ok(())
    }

    fn save_atomic(&self, entries: &[WatchHistoryEntry]) -> Result<()> {
        let payload = serde_json::to_string_pretty(entries)?;
        crate::metadata::atomic_write_bytes(&self.path, payload.as_bytes())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::ProviderId;
    use tempfile::tempdir;

    #[test]
    fn persists_updates_and_resumes_latest_entry() {
        let dir = tempdir().unwrap();
        let store = HistoryStore::with_path(dir.path().join("history.json"));

        let steins = AnimeResult {
            slug: "steins-gate".into(),
            title: "Steins;Gate".into(),
            alt_title: None,
            url: "https://eva.animerco.org/animes/steins-gate/".into(),
            provider: ProviderId::Animerco,
            year: Some("2011".into()),
            episode_count: Some(24),
            latency_ms: 42,
            sources: Vec::new(),
        };

        store.record_watch_at(&steins, 1, Some(142)).unwrap();
        assert_eq!(store.latest().unwrap().position_secs, Some(142));

        // Resetting position_secs when finishing episode (>=95%) or starting next.
        store.record_watch_at(&steins, 5, None).unwrap();

        let latest = store.latest().unwrap();
        assert_eq!(latest.anime_slug, "steins-gate");
        assert_eq!(latest.episode, 5);
        assert_eq!(latest.position_secs, None);
        assert_eq!(store.load_all().unwrap().len(), 1);

        store.clear().unwrap();
        assert!(matches!(
            store.latest(),
            Err(SenkuError::EmptyHistory { .. })
        ));
    }

    #[test]
    fn deserializes_legacy_history_without_position_secs() {
        let dir = tempdir().unwrap();
        let hist_path = dir.path().join("history.json");
        let legacy_json = r#"[{
            "anime_slug": "one-piece",
            "title": "One Piece",
            "episode": 1071,
            "timestamp": 1700000000,
            "provider": "okanime",
            "anime_url": "https://ww3.okanime.xyz/anime/one-piece"
        }]"#;
        fs::write(&hist_path, legacy_json).unwrap();

        let store = HistoryStore::with_path(&hist_path);
        let latest = store.latest().unwrap();
        assert_eq!(latest.episode, 1071);
        assert_eq!(latest.position_secs, None);
        assert!(latest.watched_episodes.is_empty());

        let hxh = AnimeResult {
            slug: "hunter-x-hunter".into(),
            title: "Hunter x Hunter".into(),
            alt_title: None,
            url: "https://ww3.okanime.xyz/anime/hunter-x-hunter".into(),
            provider: ProviderId::OkAnime,
            year: Some("2011".into()),
            episode_count: Some(148),
            latency_ms: 15,
            sources: Vec::new(),
        };
        store.record_watch_state(&hxh, 130, None, true).unwrap();
        store.record_watch_state(&hxh, 129, None, true).unwrap();
        // Calling regular record_watch_at on ep 131 @ 252s must preserve [129, 130] without marking 131 watched!
        store.record_watch_at(&hxh, 131, Some(252)).unwrap();
        let hxh_entry = store.latest().unwrap();
        assert_eq!(hxh_entry.episode, 131);
        assert_eq!(hxh_entry.position_secs, Some(252));
        assert_eq!(hxh_entry.watched_episodes, vec![129, 130]);
    }

    #[test]
    fn recovers_corrupt_and_empty_history_and_orders_multi_entries() {
        let dir = tempdir().unwrap();
        let hist_path = dir.path().join("history.json");
        let store = HistoryStore::with_path(&hist_path);

        // 1. Empty file -> returns empty cleanly without error.
        fs::write(&hist_path, "   \n").unwrap();
        assert!(store.load_all().unwrap().is_empty());

        // 2. Truncated/corrupt JSON -> backs up to history.json.bak and resets cleanly.
        let corrupt = r#"[{"anime_slug":"broken","title":"Incomplete"#;
        fs::write(&hist_path, corrupt).unwrap();
        let loaded = store.load_all().unwrap();
        assert!(loaded.is_empty());
        assert!(store.backup_path().exists());
        assert_eq!(fs::read_to_string(store.backup_path()).unwrap(), corrupt);
        assert_eq!(fs::read_to_string(&hist_path).unwrap().trim(), "[]");

        // 3. Multi-entry ordering (newest timestamp first) & selective removal.
        let entries = vec![
            WatchHistoryEntry {
                anime_slug: "older-show".into(),
                title: "Older Show".into(),
                episode: 3,
                timestamp: 1000,
                provider: Some(ProviderId::OkAnime),
                anime_url: Some("https://ww3.okanime.xyz/anime/older-show".into()),
                position_secs: Some(320),
                sources: Vec::new(),
                watched_episodes: Vec::new(),
            },
            WatchHistoryEntry {
                anime_slug: "newer-show".into(),
                title: "Newer Show".into(),
                episode: 12,
                timestamp: 2000,
                provider: Some(ProviderId::Animerco),
                anime_url: Some("https://eva.animerco.org/animes/newer-show/".into()),
                position_secs: Some(745),
                sources: Vec::new(),
                watched_episodes: Vec::new(),
            },
        ];
        fs::write(&hist_path, serde_json::to_string(&entries).unwrap()).unwrap();

        let ordered = store.load_all().unwrap();
        assert_eq!(ordered.len(), 2);
        assert_eq!(ordered[0].anime_slug, "newer-show");
        assert_eq!(ordered[0].position_secs, Some(745));
        assert_eq!(ordered[1].anime_slug, "older-show");
        assert_eq!(store.latest().unwrap().anime_slug, "newer-show");

        assert!(store.remove_entry("newer-show").unwrap());
        assert_eq!(store.latest().unwrap().anime_slug, "older-show");
    }

    #[test]
    fn detects_new_episodes_and_formats_new_badge() {
        let entry = WatchHistoryEntry {
            anime_slug: "black-clover-s2".into(),
            title: "Black Clover 2nd Season".into(),
            episode: 7,
            timestamp: 1_700_000_000,
            provider: Some(ProviderId::OkAnime),
            anime_url: Some("https://ww3.okanime.xyz/anime/black-clover-s2".into()),
            position_secs: None,
            sources: Vec::new(),
            watched_episodes: Vec::new(),
        };

        // Provider lists episode 8 -> alert Ep 08 NEW!
        let alert = detect_new_episode(&entry, Some(8), None).unwrap();
        assert_eq!(alert.next_episode_to_watch, 8);
        assert_eq!(alert.latest_available, 8);

        let formatted = format_history_row_label(0, &entry, Some(&alert));
        assert!(formatted.contains("Black Clover 2nd Season — Ep 07"));
        assert!(formatted.contains("✦ Ep 08 NEW!"));

        // AniList nextAiringEpisode = 10 -> released up to Ep 9 -> alert!
        let airing_alert = detect_new_episode(&entry, None, Some(10)).unwrap();
        assert_eq!(airing_alert.latest_available, 9);
        assert_eq!(airing_alert.next_episode_to_watch, 8);

        // Caught up (latest == 7) -> None
        assert!(detect_new_episode(&entry, Some(7), Some(8)).is_none());
    }
}
