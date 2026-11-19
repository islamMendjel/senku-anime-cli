use crate::cli::{is_kitty_terminal, log_info, log_warn};
use crate::config::DEFAULT_PROXY_THREADS;
use crate::error::{Result, SenkuError};
use crate::history::HistoryStore;
use crate::metadata::{
    build_chapter_list, cache_root_dir, chapters_to_mpv_json, fetch_episode_skips,
    load_cached_metadata_for_title, resolve_skip_intro_action, AnimeMetadata, EpisodeMeta,
    SkipInterval, SkipTarget,
};
use crate::models::{AnimeResult, Episode, ProviderId, Resolution, StreamSource};
use crate::preview::{
    capture_icat_unicode_placeholder, clear_kitty_graphics, get_existing_valid_cover, has_kitten,
    is_kitty_capable,
};
use crate::providers::common::CHROME_USER_AGENT;
use crate::providers::ProviderEngine;
use crate::proxy::{ProxyHandle, ProxyStats, TurboStreamProxy};
use crate::resolver::{stream_host_key, StreamResolver};
use crate::selector::{self, apply_quality_pick};
use crossterm::event::{Event, EventStream, KeyCode, KeyEvent, KeyModifiers};
use futures::StreamExt;
use owo_colors::OwoColorize;
use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::io::{self, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::process::{Child, Command};
use tokio::task::JoinHandle;

const REQ_PERCENT_POS: u64 = 100;
const REQ_TIME_POS: u64 = 101;
const REQ_DURATION: u64 = 102;
const REQ_PAUSE: u64 = 103;
const REQ_CACHE_DUR: u64 = 104;
const REQ_PAUSED_CACHE: u64 = 105;
const PREFETCH_TRIGGER_PERCENT: f64 = 85.0;
const PROGRESS_BAR_CELLS: usize = 12;
const STALL_FAILOVER_THRESHOLD: Duration = Duration::from_secs(8);

/// Returns a PID-scoped Unix domain socket path so concurrent instances never collide.
pub fn default_ipc_socket() -> PathBuf {
    PathBuf::from(format!("/tmp/senku-anime-cli-{}.sock", std::process::id()))
}

/// RAII guard that removes the mpv Unix IPC socket on normal exit, error, or drop.
#[derive(Debug)]
pub struct SocketGuard {
    path: PathBuf,
}

impl SocketGuard {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for SocketGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// RAII guard that hides the terminal cursor (`\x1b[?25l`) during live status
/// rendering and guarantees restoration (`\x1b[?25h`), footer clearing, and
/// Kitty graphics cleanup (`\x1b_Ga=d,d=A\x1b\\`) on exit, Ctrl+C, or panic.
#[derive(Debug)]
pub struct CursorGuard {
    active: bool,
    cinema_mode: bool,
}

impl CursorGuard {
    pub fn hide() -> Self {
        Self::hide_with_mode(false)
    }

    pub fn hide_with_mode(cinema_mode: bool) -> Self {
        let active = io::stderr().is_terminal();
        if active {
            let _ = write!(io::stderr(), "\x1b[?25l");
            let _ = io::stderr().flush();
        }
        Self {
            active,
            cinema_mode,
        }
    }
}

impl Drop for CursorGuard {
    fn drop(&mut self) {
        if self.active {
            if self.cinema_mode {
                let _ = write!(
                    io::stderr(),
                    "\r\x1b[2K\r\n\x1b[2K\r\n\x1b[2K\x1b[2A\r\x1b[?25h"
                );
            } else {
                let _ = write!(io::stderr(), "\r\x1b[2K\r\n\x1b[2K\x1b[1A\r\x1b[?25h");
            }
            let _ = io::stderr().flush();
        }
        if self.cinema_mode {
            clear_kitty_graphics();
        }
    }
}

/// RAII guard that enables non-blocking crossterm raw mode for single-key
/// playback hotkeys when stdin is a TTY and restores normal mode on drop.
#[derive(Debug)]
pub struct RawModeGuard {
    enabled: bool,
}

impl RawModeGuard {
    pub fn acquire() -> Self {
        let enabled = io::stdin().is_terminal() && crossterm::terminal::enable_raw_mode().is_ok();
        Self { enabled }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
}

impl Drop for RawModeGuard {
    fn drop(&mut self) {
        if self.enabled {
            let _ = crossterm::terminal::disable_raw_mode();
        }
    }
}

/// Outcome returned by `MpvPlayer::play_loop` so the caller can present the
/// interactive `ani-cli`-style post-playback menu when the user quits early.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlaybackStop {
    CompletedSeries,
    UserStopped { episode_index: usize },
    Interrupted,
}

#[derive(Debug, Clone, PartialEq)]
pub enum IpcSignal {
    PercentPos(f64),
    TimePos(f64),
    Duration(f64),
    Paused(bool),
    CacheDuration(f64),
    PausedForCache(bool),
    EofReached(bool),
    EndFile { reason: String },
    Other,
}

/// Auto-stall failover watchdog: triggers when `paused-for-cache` remains
/// continuously `true` for at least `threshold` (default 8s), resetting cleanly
/// whenever buffering recovers.
#[derive(Debug, Clone)]
pub struct StallWatchdog {
    stall_started_at: Option<Instant>,
    threshold: Duration,
}

impl StallWatchdog {
    pub fn new(threshold: Duration) -> Self {
        Self {
            stall_started_at: None,
            threshold,
        }
    }

    pub fn update(&mut self, paused_for_cache: bool, now: Instant) -> bool {
        if paused_for_cache {
            let start = *self.stall_started_at.get_or_insert(now);
            now.saturating_duration_since(start) >= self.threshold
        } else {
            self.stall_started_at = None;
            false
        }
    }
}

#[derive(Debug, Deserialize)]
struct RawIpcMessage {
    #[serde(default)]
    event: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    data: Option<Value>,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    request_id: Option<u64>,
    #[serde(default)]
    error: Option<String>,
}

/// Parses a single newline-delimited mpv JSON-IPC message.
pub fn parse_ipc_line(line: &str) -> Option<IpcSignal> {
    let msg: RawIpcMessage = serde_json::from_str(line.trim()).ok()?;

    if let Some(ref event) = msg.event {
        match event.as_str() {
            "property-change" => match msg.name.as_deref() {
                Some("percent-pos") => {
                    if let Some(pct) = msg.data.as_ref().and_then(Value::as_f64) {
                        return Some(IpcSignal::PercentPos(pct));
                    }
                }
                Some("time-pos") => {
                    if let Some(t) = msg.data.as_ref().and_then(Value::as_f64) {
                        return Some(IpcSignal::TimePos(t));
                    }
                }
                Some("duration") => {
                    if let Some(d) = msg.data.as_ref().and_then(Value::as_f64) {
                        return Some(IpcSignal::Duration(d));
                    }
                }
                Some("pause") => {
                    if let Some(p) = msg.data.as_ref().and_then(Value::as_bool) {
                        return Some(IpcSignal::Paused(p));
                    }
                }
                Some("demuxer-cache-duration") => {
                    if let Some(d) = msg.data.as_ref().and_then(Value::as_f64) {
                        return Some(IpcSignal::CacheDuration(d));
                    }
                }
                Some("paused-for-cache") => {
                    if let Some(p) = msg.data.as_ref().and_then(Value::as_bool) {
                        return Some(IpcSignal::PausedForCache(p));
                    }
                }
                Some("eof-reached") => {
                    if let Some(eof) = msg.data.as_ref().and_then(Value::as_bool) {
                        return Some(IpcSignal::EofReached(eof));
                    }
                }
                _ => {}
            },
            "end-file" => {
                let reason = msg.reason.unwrap_or_else(|| "unknown".to_string());
                return Some(IpcSignal::EndFile { reason });
            }
            _ => {}
        }
        return Some(IpcSignal::Other);
    }

    if msg.error.as_deref() == Some("success") {
        match msg.request_id {
            Some(REQ_PERCENT_POS) => {
                if let Some(pct) = msg.data.as_ref().and_then(Value::as_f64) {
                    return Some(IpcSignal::PercentPos(pct));
                }
            }
            Some(REQ_TIME_POS) => {
                if let Some(t) = msg.data.as_ref().and_then(Value::as_f64) {
                    return Some(IpcSignal::TimePos(t));
                }
            }
            Some(REQ_DURATION) => {
                if let Some(d) = msg.data.as_ref().and_then(Value::as_f64) {
                    return Some(IpcSignal::Duration(d));
                }
            }
            Some(REQ_PAUSE) => {
                if let Some(p) = msg.data.as_ref().and_then(Value::as_bool) {
                    return Some(IpcSignal::Paused(p));
                }
            }
            Some(REQ_CACHE_DUR) => {
                if let Some(d) = msg.data.as_ref().and_then(Value::as_f64) {
                    return Some(IpcSignal::CacheDuration(d));
                }
            }
            Some(REQ_PAUSED_CACHE) => {
                if let Some(p) = msg.data.as_ref().and_then(Value::as_bool) {
                    return Some(IpcSignal::PausedForCache(p));
                }
            }
            _ => {}
        }
    }

    Some(IpcSignal::Other)
}

/// Formats seconds as `MM:SS` or `H:MM:SS`.
pub fn format_playback_time(seconds: f64) -> String {
    let total = if seconds.is_finite() && seconds > 0.0 {
        seconds.floor() as u64
    } else {
        0
    };
    let hours = total / 3600;
    let mins = (total % 3600) / 60;
    let secs = total % 60;
    if hours > 0 {
        format!("{hours}:{mins:02}:{secs:02}")
    } else {
        format!("{mins:02}:{secs:02}")
    }
}

/// Builds a 12-cell Unicode progress bar (`█` / `░`) for a percentage `[0..=100]`.
pub fn render_progress_bar(percent: f64) -> String {
    let clamped = if percent.is_finite() {
        percent.clamp(0.0, 100.0)
    } else {
        0.0
    };
    let filled = ((clamped / 100.0) * PROGRESS_BAR_CELLS as f64).round() as usize;
    let filled = if clamped > 0.0 && filled == 0 {
        1
    } else {
        filled.min(PROGRESS_BAR_CELLS)
    };
    let empty = PROGRESS_BAR_CELLS - filled;
    format!("{}{}", "█".repeat(filled), "░".repeat(empty))
}

/// Formats live proxy throughput in human-readable form (e.g. `2.8MB/s`, `640KB/s`).
pub fn format_throughput_bps(bps: u64) -> String {
    if bps >= 1_048_576 {
        format!("{:.1}MB/s", bps as f64 / 1_048_576.0)
    } else if bps >= 1024 {
        format!("{:.0}KB/s", bps as f64 / 1024.0)
    } else {
        format!("{bps}B/s")
    }
}

/// Clamps playback volume to `[0, 130]` after applying a signed delta.
pub fn apply_volume_delta(current: i64, delta: i64) -> i64 {
    current.saturating_add(delta).clamp(0, 130)
}

/// Clamps playback speed to `[0.50, 2.00]` rounded to two decimals.
pub fn apply_speed_delta(current: f64, delta: f64) -> f64 {
    let next = (current + delta).clamp(0.5, 2.0);
    (next * 100.0).round() / 100.0
}

/// Pure representation of an interactive live-remote hotkey action for clean unit testing.
#[derive(Debug, Clone, PartialEq)]
pub enum LiveRemoteAction {
    CyclePause,
    SeekRelative { secs: i64, osd: String },
    SeekAbsolute { secs: f64, osd: String },
    SetVolume { volume: i64 },
    SetMute { muted: bool },
    SetSpeed { speed: f64 },
    CycleFullscreen,
    ToggleAutoplay { autoplay: bool },
    SwitchQuality,
    NextEpisode,
    PrevEpisode,
    Quit,
}

/// Maps a terminal key event onto the expanded live `mpv` IPC remote deck.
pub fn map_hotkey_action(
    code: KeyCode,
    status: &PlaybackStatus,
    skip_intervals: &[SkipInterval],
) -> Option<LiveRemoteAction> {
    match code {
        KeyCode::Char(' ') => Some(LiveRemoteAction::CyclePause),
        KeyCode::Left => Some(LiveRemoteAction::SeekRelative {
            secs: -10,
            osd: "Seek -10s".to_string(),
        }),
        KeyCode::Right => Some(LiveRemoteAction::SeekRelative {
            secs: 10,
            osd: "Seek +10s".to_string(),
        }),
        KeyCode::Char('-') | KeyCode::Char('_') => Some(LiveRemoteAction::SetVolume {
            volume: apply_volume_delta(status.volume, -5),
        }),
        KeyCode::Char('+') | KeyCode::Char('=') => Some(LiveRemoteAction::SetVolume {
            volume: apply_volume_delta(status.volume, 5),
        }),
        KeyCode::Char('m') | KeyCode::Char('M') => Some(LiveRemoteAction::SetMute {
            muted: !status.muted,
        }),
        KeyCode::Char('[') => Some(LiveRemoteAction::SetSpeed {
            speed: apply_speed_delta(status.speed, -0.25),
        }),
        KeyCode::Char(']') => Some(LiveRemoteAction::SetSpeed {
            speed: apply_speed_delta(status.speed, 0.25),
        }),
        KeyCode::Char('f') | KeyCode::Char('F') => Some(LiveRemoteAction::CycleFullscreen),
        KeyCode::Char('a') | KeyCode::Char('A') => Some(LiveRemoteAction::ToggleAutoplay {
            autoplay: !status.autoplay,
        }),
        KeyCode::Char('i') | KeyCode::Char('I') => {
            match resolve_skip_intro_action(status.time_pos, skip_intervals, status.skip_intro_secs)
            {
                SkipTarget::ExactSeek { target_secs, label } => {
                    Some(LiveRemoteAction::SeekAbsolute {
                        secs: target_secs,
                        osd: label.to_string(),
                    })
                }
                SkipTarget::RelativeFallback { delta_secs } => {
                    Some(LiveRemoteAction::SeekRelative {
                        secs: delta_secs as i64,
                        osd: format!("Skipped Intro (+{delta_secs}s)"),
                    })
                }
            }
        }
        KeyCode::Char('v') | KeyCode::Char('V') => Some(LiveRemoteAction::SwitchQuality),
        KeyCode::Char('n') | KeyCode::Char('N') => Some(LiveRemoteAction::NextEpisode),
        KeyCode::Char('p') | KeyCode::Char('P') => Some(LiveRemoteAction::PrevEpisode),
        KeyCode::Char('q') | KeyCode::Char('Q') | KeyCode::Esc => Some(LiveRemoteAction::Quit),
        _ => None,
    }
}

/// Live terminal playback status bar state.
#[derive(Debug, Clone, PartialEq)]
pub struct PlaybackStatus {
    pub episode_num: u32,
    pub resolution: Resolution,
    pub time_pos: f64,
    pub duration: Option<f64>,
    pub percent_pos: f64,
    pub paused: bool,
    pub autoplay: bool,
    pub skip_intro_secs: u64,
    pub cache_duration_secs: Option<f64>,
    pub paused_for_cache: bool,
    pub turbo_workers: Option<usize>,
    pub proxy_bps: u64,
    pub volume: i64,
    pub muted: bool,
    pub speed: f64,
    pub op_detected: bool,
}

impl PlaybackStatus {
    pub fn effective_percent(&self) -> f64 {
        if self.percent_pos > 0.0 && self.percent_pos.is_finite() {
            return self.percent_pos.clamp(0.0, 100.0);
        }
        if let Some(dur) = self.duration {
            if dur > 0.0 && self.time_pos >= 0.0 {
                return ((self.time_pos / dur) * 100.0).clamp(0.0, 100.0);
            }
        }
        0.0
    }

    pub fn format_lines(&self) -> (String, String) {
        let icon = if self.paused { "⏸" } else { "▶" };
        let cur = format_playback_time(self.time_pos);
        let dur = self
            .duration
            .map(format_playback_time)
            .unwrap_or_else(|| "--:--".to_string());
        let pct = self.effective_percent();
        let pct_int = pct.round() as u32;
        let bar = render_progress_bar(pct);
        let ap = if self.autoplay { "ON" } else { "OFF" };

        let buf_part = if self.paused_for_cache {
            "\x1b[33mBuf: 0s (buffering...)\x1b[0m".to_string()
        } else {
            let secs = self.cache_duration_secs.unwrap_or(0.0).max(0.0).round() as u64;
            match (self.turbo_workers, self.proxy_bps) {
                (Some(w), bps) if bps > 0 => {
                    format!("Buf: {secs}s/120s ⚡{w}x {}", format_throughput_bps(bps))
                }
                (Some(w), _) => format!("Buf: {secs}s/120s ⚡{w}x"),
                (None, _) => format!("Buf: {secs}s/120s"),
            }
        };

        let mut extras = String::new();
        if self.muted {
            extras.push_str("  🔇");
        } else if self.volume != 100 {
            extras.push_str(&format!("  🔊{}%", self.volume));
        }
        if (self.speed - 1.0).abs() > 0.01 {
            extras.push_str(&format!("  {:.2}x", self.speed));
        }
        if self.op_detected {
            extras.push_str("  [OP detected]");
        }

        let status_line = format!(
            "{icon} Ep {:02} [{}] [{bar}] {cur} / {dur} ({pct_int}%)  {buf_part}  [Autoplay: {ap}]{extras}",
            self.episode_num, self.resolution
        );
        let hint_line = "  [Space] Pause  [←/→] ±10s  [i] Skip OP  [m] Mute  [-/+] Vol  [[/]] Speed  [f] Full  [a] Auto  [v] Qual  [q] Menu".to_string();
        (status_line, hint_line)
    }
}

pub const CINEMA_BAR_CELLS: usize = 24;

/// Classification of an individual cell in the chapter-aware progress bar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BarCellKind {
    PlayedOp,
    PlayedEd,
    PlayedNormal,
    UnplayedOp,
    UnplayedEd,
    UnplayedNormal,
}

/// Maps each progress bar cell `[0..cells)` to its chapter/played classification
/// using detected AniSkip `op` / `ed` intervals and video `duration`.
pub fn classify_progress_cells(
    percent: f64,
    duration: Option<f64>,
    cells: usize,
    intervals: &[SkipInterval],
) -> Vec<BarCellKind> {
    let clamped = if percent.is_finite() {
        percent.clamp(0.0, 100.0)
    } else {
        0.0
    };
    let filled = ((clamped / 100.0) * cells as f64).round() as usize;
    let filled = if clamped > 0.0 && filled == 0 {
        1
    } else {
        filled.min(cells)
    };

    let dur = duration.filter(|d| d.is_finite() && *d > 0.0);
    let mut out = Vec::with_capacity(cells);
    for i in 0..cells {
        let played = i < filled;
        let chapter_tag = dur.and_then(|total_secs| {
            let c_start = (i as f64 / cells as f64) * total_secs;
            let c_end = ((i + 1) as f64 / cells as f64) * total_secs;
            intervals.iter().find_map(|iv| {
                if c_end > iv.start_time && c_start < iv.end_time {
                    if iv.skip_type.is_op() {
                        Some(true) // OP
                    } else if iv.skip_type.is_ed() {
                        Some(false) // ED
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
        });

        let kind = match (played, chapter_tag) {
            (true, Some(true)) => BarCellKind::PlayedOp,
            (true, Some(false)) => BarCellKind::PlayedEd,
            (true, None) => BarCellKind::PlayedNormal,
            (false, Some(true)) => BarCellKind::UnplayedOp,
            (false, Some(false)) => BarCellKind::UnplayedEd,
            (false, None) => BarCellKind::UnplayedNormal,
        };
        out.push(kind);
    }
    out
}

/// Renders a chapter-aware progress bar highlighting OP (magenta) and ED (yellow)
/// intervals distinctly from normal episode cells.
pub fn render_chapter_progress_bar(
    percent: f64,
    duration: Option<f64>,
    cells: usize,
    intervals: &[SkipInterval],
) -> String {
    let kinds = classify_progress_cells(percent, duration, cells, intervals);
    let mut out = String::with_capacity(cells * 12);
    for k in kinds {
        match k {
            BarCellKind::PlayedOp => out.push_str("\x1b[35;1m█\x1b[0m"),
            BarCellKind::UnplayedOp => out.push_str("\x1b[35m▒\x1b[0m"),
            BarCellKind::PlayedEd => out.push_str("\x1b[33;1m█\x1b[0m"),
            BarCellKind::UnplayedEd => out.push_str("\x1b[33m▒\x1b[0m"),
            BarCellKind::PlayedNormal => out.push_str("\x1b[32m█\x1b[0m"),
            BarCellKind::UnplayedNormal => out.push_str("\x1b[2m░\x1b[0m"),
        }
    }
    out
}

/// Color-codes the 2-minute rolling buffer gauge:
/// - green (`\x1b[32m`) when `>= 30s`
/// - yellow (`\x1b[33m`) when `5..=29s` or `paused_for_cache`
/// - red (`\x1b[31m`) when `< 5s`
pub fn format_colored_buffer_gauge(
    cache_secs: Option<f64>,
    paused_for_cache: bool,
    turbo_workers: Option<usize>,
    proxy_bps: u64,
) -> String {
    if paused_for_cache {
        return "\x1b[33mBuf: 0s/120s (buffering...)\x1b[0m".to_string();
    }
    let secs = cache_secs.unwrap_or(0.0).max(0.0).round() as u64;
    let color = if secs >= 30 {
        "\x1b[32m"
    } else if secs >= 5 {
        "\x1b[33m"
    } else {
        "\x1b[31m"
    };
    let core = format!("{color}Buf: {secs}s/120s\x1b[0m");
    match (turbo_workers, proxy_bps) {
        (Some(w), bps) if bps > 0 => format!("{core} ⚡{w}x {}", format_throughput_bps(bps)),
        (Some(w), _) => format!("{core} ⚡{w}x"),
        (None, _) => core,
    }
}

/// Static Now-Playing Cinema Dashboard header drawn ONCE per episode change.
pub fn format_cinema_top_card_text(
    anime: &AnimeResult,
    episode_num: u32,
    total_episodes: Option<u32>,
    stream: &StreamSource,
    meta: Option<&AnimeMetadata>,
) -> Vec<String> {
    let ep_total = total_episodes
        .or_else(|| meta.and_then(|m| m.episodes))
        .map(|n| format!("{n:02}"))
        .unwrap_or_else(|| "?".to_string());

    let title_line = format!("{}", anime.title.bright_cyan().bold());
    let ep_line = format!(
        "Episode {:02} / {}   [{} · {} · {}]",
        episode_num,
        ep_total,
        stream.resolution,
        stream.server_name,
        anime.provider_display()
    );

    let mut lines = vec![title_line, ep_line];
    if let Some(m) = meta {
        let score = m
            .average_score
            .map(|s| format!("{s}%"))
            .unwrap_or_else(|| "N/A".to_string());
        let year = m
            .season_year
            .map(|y| y.to_string())
            .unwrap_or_else(|| "TBA".to_string());
        let genres = if m.genres.is_empty() {
            String::new()
        } else {
            format!(
                " · {}",
                m.genres
                    .iter()
                    .take(3)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        lines.push(format!("Score: {} · {year}{genres}", score.bright_green()));
    }
    lines
}

/// Renders the static top card ONCE per episode start/switch (never redrawn on ticks).
fn render_cinema_top_card(
    anime: &AnimeResult,
    episode_num: u32,
    total_episodes: Option<u32>,
    stream: &StreamSource,
) {
    if !io::stderr().is_terminal() {
        return;
    }
    clear_kitty_graphics();

    let cache_root = cache_root_dir();
    let meta = load_cached_metadata_for_title(&anime.title, &cache_root);
    let cover_path = meta
        .as_ref()
        .and_then(|m| get_existing_valid_cover(m, &cache_root));
    let info_lines =
        format_cinema_top_card_text(anime, episode_num, total_episodes, stream, meta.as_ref());

    let stderr = io::stderr();
    let mut lock = stderr.lock();

    let rendered_kitty = cover_path.as_ref().is_some_and(|p| {
        if is_kitty_capable() && has_kitten() {
            if let Some(icat_bytes) = capture_icat_unicode_placeholder(p, 16, 8) {
                let _ = lock.write_all(&icat_bytes);
                let _ = write!(lock, "\x1b[m\x1b[8A");
                for (i, line) in info_lines.iter().enumerate() {
                    let _ = write!(lock, "\x1b[19G{line}\r\n");
                    if i >= 7 {
                        break;
                    }
                }
                let remaining = 8usize.saturating_sub(info_lines.len());
                if remaining > 0 {
                    let _ = write!(lock, "\x1b[{remaining}B");
                }
                let _ = writeln!(
                    lock,
                    "\r{}",
                    "────────────────────────────────────────────────────────────".dimmed()
                );
                return true;
            }
        }
        false
    });

    if !rendered_kitty {
        let _ = writeln!(
            lock,
            "{}",
            "╭── Now Playing ────────────────────────────────────────────╮".bright_cyan()
        );
        for line in &info_lines {
            let _ = writeln!(lock, "│ {line}");
        }
        let _ = writeln!(
            lock,
            "{}",
            "╰───────────────────────────────────────────────────────────╯".bright_cyan()
        );
    }
    let _ = lock.flush();
}

impl PlaybackStatus {
    /// Formats the 3-line flicker-free dynamic footer for the Cinema Dashboard.
    pub fn format_cinema_footer(&self, intervals: &[SkipInterval]) -> [String; 3] {
        let icon = if self.paused { "⏸" } else { "▶" };
        let cur = format_playback_time(self.time_pos);
        let dur = self
            .duration
            .map(format_playback_time)
            .unwrap_or_else(|| "--:--".to_string());
        let pct = self.effective_percent();
        let pct_int = pct.round() as u32;
        let bar = render_chapter_progress_bar(pct, self.duration, CINEMA_BAR_CELLS, intervals);
        let op_tag = if self.op_detected {
            "  \x1b[35m[OP detected]\x1b[0m"
        } else {
            ""
        };

        let line1 = format!("{icon} [{bar}] {cur} / {dur} ({pct_int}%){op_tag}");

        let buf_gauge = format_colored_buffer_gauge(
            self.cache_duration_secs,
            self.paused_for_cache,
            self.turbo_workers,
            self.proxy_bps,
        );
        let ap = if self.autoplay { "ON" } else { "OFF" };
        let mut extras = String::new();
        if self.muted {
            extras.push_str("  🔇");
        } else if self.volume != 100 {
            extras.push_str(&format!("  🔊{}%", self.volume));
        }
        if (self.speed - 1.0).abs() > 0.01 {
            extras.push_str(&format!("  {:.2}x", self.speed));
        }
        let line2 = format!("  {buf_gauge}  [Autoplay: {ap}]{extras}");
        let line3 =
            "  [Space] [←/→] [i] OP [v] Qual [n/p] Ep [f] Full [a] Auto [q] Menu".to_string();

        [line1, line2, line3]
    }

    fn render_in_place(&self) {
        let (line1, line2) = self.format_lines();
        let stderr = io::stderr();
        let mut lock = stderr.lock();
        let _ = write!(lock, "\r\x1b[2K{line1}\r\n\x1b[2K{line2}\x1b[1A\r");
        let _ = lock.flush();
    }

    fn render_cinema_footer_in_place(&self, intervals: &[SkipInterval]) {
        let [l1, l2, l3] = self.format_cinema_footer(intervals);
        let stderr = io::stderr();
        let mut lock = stderr.lock();
        let _ = write!(
            lock,
            "\r\x1b[2K{l1}\r\n\r\x1b[2K{l2}\r\n\r\x1b[2K{l3}\x1b[2A\r"
        );
        let _ = lock.flush();
    }

    fn render_active(&self, cinema_mode: bool, intervals: &[SkipInterval]) {
        if cinema_mode {
            self.render_cinema_footer_in_place(intervals);
        } else {
            self.render_in_place();
        }
    }
}

fn clear_status_bar(cinema_mode: bool) {
    let stderr = io::stderr();
    let mut lock = stderr.lock();
    if cinema_mode {
        let _ = write!(lock, "\r\x1b[2K\r\n\x1b[2K\r\n\x1b[2K\x1b[2A\r");
        let _ = lock.flush();
        clear_kitty_graphics();
    } else {
        let _ = write!(lock, "\r\x1b[2K\r\n\x1b[2K\x1b[1A\r");
        let _ = lock.flush();
    }
}

/// Computes the next episode index for autoplay, skipping consecutive Jikan-flagged
/// `filler` or `recap` episodes when `skip_filler` is enabled.
pub fn next_canon_episode_index(
    episodes: &[Episode],
    current_idx: usize,
    guides: &HashMap<u32, EpisodeMeta>,
    skip_filler: bool,
) -> Option<usize> {
    let mut cand = current_idx + 1;
    if skip_filler {
        while cand < episodes.len() {
            let meta = guides.get(&episodes[cand].number);
            if meta.is_some_and(|m| m.filler || m.recap) {
                cand += 1;
            } else {
                break;
            }
        }
    }
    (cand < episodes.len()).then_some(cand)
}

/// Computes `position_secs` to persist: resets to `None` once episode reaches `>= 95%`.
pub fn persisted_position(time_pos: f64, percent_pos: f64) -> Option<u64> {
    if percent_pos >= 95.0 {
        return None;
    }
    if time_pos.is_finite() && time_pos > 1.0 {
        Some(time_pos.floor() as u64)
    } else {
        None
    }
}

/// Background pre-fetch state machine for zero-delay Episode N -> N+1 autoplay.
#[derive(Debug)]
pub enum PrefetchState {
    Idle,
    InFlight {
        episode_num: u32,
        handle: JoinHandle<Result<Vec<StreamSource>>>,
    },
    Ready {
        episode_num: u32,
        streams: Result<Vec<StreamSource>>,
    },
}

impl PrefetchState {
    /// Returns true when playback has reached `>= 85%`, autoplay is enabled,
    /// a next episode exists, and pre-fetch has not started yet.
    pub fn should_trigger(&self, percent_pos: f64, autoplay: bool, has_next: bool) -> bool {
        autoplay
            && has_next
            && percent_pos >= PREFETCH_TRIGGER_PERCENT
            && matches!(self, PrefetchState::Idle)
    }

    pub fn spawn_for_next(
        &mut self,
        next_ep: Episode,
        preferred_quality: Option<Resolution>,
        stalled_hosts: HashSet<String>,
        engine: Arc<ProviderEngine>,
        resolver: Arc<StreamResolver>,
    ) {
        let ep_num = next_ep.number;
        let handle = tokio::spawn(async move {
            let candidates = engine.get_streams(&next_ep).await?;
            resolver
                .resolve_ranked_verified_with_penalties(
                    ep_num,
                    candidates,
                    preferred_quality,
                    &stalled_hosts,
                )
                .await
        });
        *self = PrefetchState::InFlight {
            episode_num: ep_num,
            handle,
        };
    }

    /// Polls finished background handle without blocking playback monitoring.
    pub async fn poll_ready(&mut self) {
        let is_done = matches!(
            self,
            PrefetchState::InFlight { handle, .. } if handle.is_finished()
        );
        if is_done {
            let old = std::mem::replace(self, PrefetchState::Idle);
            if let PrefetchState::InFlight {
                episode_num,
                handle,
            } = old
            {
                let streams = match handle.await {
                    Ok(res) => res,
                    Err(e) => Err(SenkuError::MpvIpc(format!("pre-fetch task failed: {e}"))),
                };
                *self = PrefetchState::Ready {
                    episode_num,
                    streams,
                };
            }
        }
    }

    /// Consumes pre-fetched streams if ready/in-flight for `target_ep`, or resolves on demand.
    pub async fn take_or_resolve(
        self,
        next_ep: &Episode,
        preferred_quality: Option<Resolution>,
        stalled_hosts: &HashSet<String>,
        engine: &ProviderEngine,
        resolver: &StreamResolver,
    ) -> Result<Vec<StreamSource>> {
        match self {
            PrefetchState::Ready {
                episode_num,
                streams: Ok(list),
            } if episode_num == next_ep.number => {
                return Ok(list);
            }
            PrefetchState::InFlight {
                episode_num,
                handle,
            } if episode_num == next_ep.number => {
                if let Ok(Ok(list)) = handle.await {
                    return Ok(list);
                }
            }
            PrefetchState::InFlight { handle, .. } => {
                handle.abort();
            }
            _ => {}
        }

        let candidates = engine.get_streams(next_ep).await?;
        resolver
            .resolve_ranked_verified_with_penalties(
                next_ep.number,
                candidates,
                preferred_quality,
                stalled_hosts,
            )
            .await
    }
}

/// Builds mpv CLI arguments including aggressive cache/readahead tuning,
/// anti-stutter buffering cushion, reconnect flags, optional resume `--start=<secs>`,
/// Unix IPC socket, and optional Kitty VO flags.
pub fn build_mpv_args(
    stream: &StreamSource,
    title: &str,
    socket_path: &Path,
    use_kitty_vo: bool,
    start_secs: Option<u64>,
) -> Vec<String> {
    let mut args = vec![
        format!("--input-ipc-server={}", socket_path.display()),
        "--force-window=immediate".to_string(),
        "--keep-open=yes".to_string(),
        "--cache=yes".to_string(),
        "--cache-secs=120".to_string(),
        "--demuxer-readahead-secs=120".to_string(),
        "--demuxer-max-bytes=180MiB".to_string(),
        "--demuxer-max-back-bytes=12MiB".to_string(),
        "--network-timeout=15".to_string(),
        "--cache-pause-initial=yes".to_string(),
        "--cache-pause-wait=1.5".to_string(),
        "--stream-lavf-o=reconnect=1,reconnect_streamed=1,reconnect_delay_max=5".to_string(),
        format!("--user-agent={CHROME_USER_AGENT}"),
        format!("--force-media-title={title}"),
        format!("--title={title}"),
    ];

    if let Some(secs) = start_secs.filter(|&s| s > 10) {
        args.push(format!("--start={secs}"));
    }

    if let Some(ref referer) = stream.referer {
        args.push(format!("--referrer={referer}"));
        args.push(format!("--http-header-fields=Referer: {referer}"));
    }

    if use_kitty_vo {
        args.push("--vo=kitty".to_string());
        args.push("--vo-kitty-use-shm=yes".to_string());
    }

    args.push(stream.url.clone());
    args
}

/// Selects the best untried failover mirror index from `streams`, strictly
/// preferring streams on hosts NOT in `stalled_hosts` before ever trying
/// another resolution on a previously stalled host.
pub fn pick_failover_stream_index(
    streams: &[StreamSource],
    current_idx: usize,
    tried_indices: &HashSet<usize>,
    stalled_hosts: &HashSet<String>,
) -> Option<usize> {
    let is_untried = |i: usize| i != current_idx && !tried_indices.contains(&i);

    // Pass 1: First untried stream whose upstream host is NOT stalled.
    if let Some(idx) = (0..streams.len())
        .find(|&i| is_untried(i) && !stalled_hosts.contains(&stream_host_key(&streams[i])))
    {
        return Some(idx);
    }

    // Pass 2: Fall back to any remaining untried stream on a stalled host only if
    // no healthy alternative host exists.
    (0..streams.len()).find(|&i| is_untried(i))
}

/// Managed `mpv` player with PID-scoped Unix domain socket JSON-IPC,
/// live 2-line status bar, raw-mode terminal hotkeys, background Ep N+1 pre-fetch,
/// exact-second resume, and automatic stream failover.
pub struct MpvPlayer {
    socket_path: PathBuf,
    mpv_bin: String,
    kitty_requested: bool,
    verbose: bool,
    autoplay: bool,
    skip_intro_secs: u64,
    auto_skip_op: bool,
    auto_skip_ed: bool,
    no_fzf: bool,
    turbo_proxy: bool,
    proxy_threads: usize,
    cinema_dashboard: bool,
    skip_filler: bool,
    episode_guides: HashMap<u32, EpisodeMeta>,
}

impl MpvPlayer {
    pub fn new(kitty_requested: bool) -> Self {
        Self {
            socket_path: default_ipc_socket(),
            mpv_bin: "mpv".to_string(),
            kitty_requested,
            verbose: false,
            autoplay: true,
            skip_intro_secs: 85,
            auto_skip_op: false,
            auto_skip_ed: false,
            no_fzf: false,
            turbo_proxy: true,
            proxy_threads: DEFAULT_PROXY_THREADS,
            cinema_dashboard: true,
            skip_filler: false,
            episode_guides: HashMap::new(),
        }
    }

    pub fn with_mpv_bin(mut self, bin: impl Into<String>) -> Self {
        self.mpv_bin = bin.into();
        self
    }

    pub fn with_verbose(mut self, verbose: bool) -> Self {
        self.verbose = verbose;
        self
    }

    pub fn with_autoplay(mut self, autoplay: bool) -> Self {
        self.autoplay = autoplay;
        self
    }

    pub fn with_skip_intro_secs(mut self, secs: u64) -> Self {
        self.skip_intro_secs = secs;
        self
    }

    pub fn with_auto_skip_op(mut self, enabled: bool) -> Self {
        self.auto_skip_op = enabled;
        self
    }

    pub fn with_auto_skip_ed(mut self, enabled: bool) -> Self {
        self.auto_skip_ed = enabled;
        self
    }

    pub fn with_no_fzf(mut self, no_fzf: bool) -> Self {
        self.no_fzf = no_fzf;
        self
    }

    pub fn with_turbo_proxy(mut self, enabled: bool) -> Self {
        self.turbo_proxy = enabled;
        self
    }

    pub fn with_proxy_threads(mut self, threads: usize) -> Self {
        self.proxy_threads = threads.clamp(1, 16);
        self
    }

    pub fn with_cinema_dashboard(mut self, enabled: bool) -> Self {
        self.cinema_dashboard = enabled;
        self
    }

    pub fn with_skip_filler(mut self, enabled: bool) -> Self {
        self.skip_filler = enabled;
        self
    }

    pub fn with_episode_guides(mut self, guides: HashMap<u32, EpisodeMeta>) -> Self {
        self.episode_guides = guides;
        self
    }

    pub fn with_socket(socket_path: impl Into<PathBuf>, kitty_requested: bool) -> Self {
        Self {
            socket_path: socket_path.into(),
            mpv_bin: "mpv".to_string(),
            kitty_requested,
            verbose: false,
            autoplay: true,
            skip_intro_secs: 85,
            auto_skip_op: false,
            auto_skip_ed: false,
            no_fzf: false,
            turbo_proxy: true,
            proxy_threads: DEFAULT_PROXY_THREADS,
            cinema_dashboard: false,
            skip_filler: false,
            episode_guides: HashMap::new(),
        }
    }

    fn should_use_kitty_vo(&self) -> bool {
        self.kitty_requested && is_kitty_terminal()
    }

    fn ensure_mpv_installed(&self) -> Result<()> {
        let ok = std::process::Command::new(&self.mpv_bin)
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false);

        if !ok {
            return Err(SenkuError::MissingBinary {
                tool: self.mpv_bin.clone(),
                hint: "Install mpv (e.g. `sudo pacman -S mpv` on Arch Linux)".to_string(),
            });
        }
        Ok(())
    }

    /// Plays `episodes[start_idx]` and automatically advances to `N+1` whenever
    /// playback reaches `>= 99%`, terminates naturally with EOF (`0`), or the
    /// user presses `[n]`/`[p]` in the terminal.
    #[allow(clippy::too_many_arguments)]
    pub async fn play_loop(
        &self,
        anime: &AnimeResult,
        episodes: &[Episode],
        start_idx: usize,
        initial_streams: Vec<StreamSource>,
        initial_start_secs: Option<u64>,
        session_quality: &mut Option<Resolution>,
        http_client: Client,
        engine: Arc<ProviderEngine>,
        resolver: Arc<StreamResolver>,
        history: &HistoryStore,
    ) -> Result<PlaybackStop> {
        self.ensure_mpv_installed()?;

        if self.kitty_requested && !is_kitty_terminal() {
            log_warn("--kitty requested, but $TERM is not xterm-kitty; using standard mpv VO");
        }

        let _socket_guard = SocketGuard::new(&self.socket_path);
        let mut idx = start_idx;
        let mut episode_streams = initial_streams;
        let mut resume_anchor_secs = initial_start_secs.filter(|&s| s > 10);
        let mut pending_start_secs = resume_anchor_secs;
        let mut stream_idx = 0usize;
        let mut child: Option<Child> = None;
        let mut ipc_conn: Option<UnixStream> = None;
        let mut active_proxy: Option<ProxyHandle> = None;
        let mut stalled_hosts: HashSet<String> = HashSet::new();
        let mut turbo_blocked_hosts: HashSet<String> = HashSet::new();
        let mut tried_streams: HashSet<usize> = HashSet::new();
        let mut tried_providers: HashSet<ProviderId> = HashSet::new();
        tried_providers.insert(anime.provider);
        for s in &episode_streams {
            tried_providers.insert(s.provider);
        }
        let mut retried_direct_on_current = false;
        let mut live_autoplay = self.autoplay;
        let mut pending_osd_toast: Option<String> = None;

        loop {
            let ep = &episodes[idx];
            if stream_idx >= episode_streams.len() {
                if let Some((alt_provider, alt_streams)) = try_cross_provider_failover(
                    &anime.title,
                    ep.number,
                    &mut tried_providers,
                    &stalled_hosts,
                    *session_quality,
                    &engine,
                    &resolver,
                )
                .await
                {
                    episode_streams = alt_streams;
                    stream_idx = 0;
                    tried_streams.clear();
                    retried_direct_on_current = false;
                    pending_start_secs = resume_anchor_secs;
                    if let Some(ref mut sc) = ipc_conn {
                        let _ = send_show_text(
                            sc,
                            &format!("Switching provider to {}...", alt_provider.display_name()),
                            3000,
                        )
                        .await;
                    }
                    continue;
                }

                log_warn(&format!(
                    "All mirrors for Episode {:02} failed. Returning to episode menu...",
                    ep.number
                ));
                let _ = history.record_watch_at(anime, ep.number, resume_anchor_secs);
                if let Some(mut sc) = ipc_conn.take() {
                    let _ = send_ipc_command(&mut sc, json!({"command": ["quit"]})).await;
                }
                if let Some(mut c) = child.take() {
                    let _ = c.kill().await;
                    let _ = c.wait().await;
                }
                if let Some(p) = active_proxy.take() {
                    p.shutdown().await;
                }
                return Ok(PlaybackStop::UserStopped { episode_index: idx });
            }
            let current_stream = &episode_streams[stream_idx];
            let current_host = stream_host_key(current_stream);
            history.record_watch_at(anime, ep.number, pending_start_secs.or(resume_anchor_secs))?;

            // Shut down any previous stream proxy before launching/switching stream.
            if let Some(prev_proxy) = active_proxy.take() {
                prev_proxy.shutdown().await;
            }

            let mut mpv_stream = current_stream.clone();
            let mut turbo_workers = None;
            let mut proxy_stats = None;
            let allow_proxy = self.turbo_proxy
                && !turbo_blocked_hosts.contains(&current_host)
                && TurboStreamProxy::is_eligible(current_stream);
            if allow_proxy {
                match TurboStreamProxy::start(
                    http_client.clone(),
                    current_stream.clone(),
                    self.proxy_threads,
                )
                .await
                {
                    Ok(handle) => {
                        mpv_stream.url = handle.local_url().to_string();
                        mpv_stream.referer = None;
                        turbo_workers = Some(handle.workers());
                        proxy_stats = Some(handle.stats());
                        active_proxy = Some(handle);
                    }
                    Err(err) => {
                        if self.verbose {
                            log_warn(&format!("Turbo proxy fallback to direct URL ({err})"));
                        }
                    }
                }
            }

            let used_proxy = active_proxy.is_some();

            let title = format!(
                "{} — Ep {:02} [{}]",
                anime.title, ep.number, current_stream.resolution
            );
            let turbo_tag = turbo_workers
                .map(|w| format!(" · ⚡{w}x"))
                .unwrap_or_default();
            if self.verbose {
                log_info(&format!(
                    "▶ Streaming {title} ({}{turbo_tag}) [IPC: {}]",
                    current_stream.server_name,
                    self.socket_path.display()
                ));
            } else {
                log_info(&format!(
                    "▶ Streaming {title} ({}{turbo_tag})",
                    current_stream.server_name
                ));
            }

            if child.is_none() {
                let _ = tokio::fs::remove_file(&self.socket_path).await;
                let args = build_mpv_args(
                    &mpv_stream,
                    &title,
                    &self.socket_path,
                    self.should_use_kitty_vo(),
                    pending_start_secs,
                );
                let mut spawned = Command::new(&self.mpv_bin)
                    .args(&args)
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .kill_on_drop(true)
                    .spawn()?;

                match connect_ipc_with_retry(&self.socket_path).await {
                    Ok(conn) => {
                        child = Some(spawned);
                        ipc_conn = Some(conn);
                    }
                    Err(err) => {
                        let _ = spawned.kill().await;
                        let _ = spawned.wait().await;
                        stalled_hosts.insert(current_host.clone());
                        tried_streams.insert(stream_idx);
                        if let Some(next_idx) = pick_failover_stream_index(
                            &episode_streams,
                            stream_idx,
                            &tried_streams,
                            &stalled_hosts,
                        ) {
                            stream_idx = next_idx;
                            retried_direct_on_current = false;
                            log_warn(&format!(
                                "Mirror failed on launch ({err}); failing over to [{}]...",
                                episode_streams[stream_idx].server_name
                            ));
                            continue;
                        }
                        stream_idx = episode_streams.len();
                        continue;
                    }
                }
            } else if let Some(stream_conn) = ipc_conn.as_mut() {
                let osd = if let Some(note) = pending_osd_toast.take() {
                    format!(
                        "{note} · Playing Ep {:02} [{}]",
                        ep.number, current_stream.resolution
                    )
                } else {
                    format!(
                        "Playing Episode {:02} [{}]",
                        ep.number, current_stream.resolution
                    )
                };
                let _ = send_show_text(stream_conn, &osd, 3000).await;
                if let Err(err) = send_loadfile(
                    stream_conn,
                    &mpv_stream.url,
                    &title,
                    mpv_stream.referer.as_deref(),
                    pending_start_secs.map(|s| s as f64),
                )
                .await
                {
                    if let Some(mut c) = child.take() {
                        let _ = c.kill().await;
                        let _ = c.wait().await;
                    }
                    ipc_conn = None;
                    stalled_hosts.insert(current_host.clone());
                    tried_streams.insert(stream_idx);
                    if let Some(next_idx) = pick_failover_stream_index(
                        &episode_streams,
                        stream_idx,
                        &tried_streams,
                        &stalled_hosts,
                    ) {
                        stream_idx = next_idx;
                        retried_direct_on_current = false;
                        log_warn(&format!(
                            "IPC loadfile failed ({err}); failing over to [{}]...",
                            episode_streams[stream_idx].server_name
                        ));
                        continue;
                    }
                    stream_idx = episode_streams.len();
                    continue;
                }
            }

            // Consumed on initial launch/resume.
            pending_start_secs = None;

            let conn = ipc_conn
                .take()
                .ok_or_else(|| SenkuError::MpvIpc("missing active IPC socket".into()))?;
            let proc_child = child.as_mut().expect("child process present");

            let target_next_idx =
                next_canon_episode_index(episodes, idx, &self.episode_guides, self.skip_filler);
            let next_ep = target_next_idx.and_then(|i| episodes.get(i).cloned());
            let status_init = PlaybackStatus {
                episode_num: ep.number,
                resolution: current_stream.resolution,
                time_pos: 0.0,
                duration: None,
                percent_pos: 0.0,
                paused: false,
                autoplay: live_autoplay,
                skip_intro_secs: self.skip_intro_secs,
                cache_duration_secs: None,
                paused_for_cache: false,
                turbo_workers,
                proxy_bps: 0,
                volume: 100,
                muted: false,
                speed: 1.0,
                op_detected: false,
            };

            let has_backup_stream = pick_failover_stream_index(
                &episode_streams,
                stream_idx,
                &tried_streams,
                &stalled_hosts,
            )
            .is_some()
                || (used_proxy && !retried_direct_on_current);
            let mut prefetch = PrefetchState::Idle;
            if self.cinema_dashboard {
                render_cinema_top_card(
                    anime,
                    ep.number,
                    episodes.last().map(|e| e.number),
                    current_stream,
                );
            }
            let outcome = {
                let _cursor_guard = CursorGuard::hide_with_mode(self.cinema_dashboard);
                let _raw_guard = RawModeGuard::acquire();
                monitor_episode(
                    conn,
                    proc_child,
                    status_init,
                    anime,
                    idx > 0,
                    has_backup_stream,
                    next_ep.clone(),
                    *session_quality,
                    stalled_hosts.clone(),
                    proxy_stats,
                    http_client.clone(),
                    self.auto_skip_op,
                    self.auto_skip_ed,
                    self.cinema_dashboard,
                    &mut live_autoplay,
                    Arc::clone(&engine),
                    Arc::clone(&resolver),
                    &mut prefetch,
                    history,
                    _raw_guard.is_enabled(),
                )
                .await?
            };

            clear_status_bar(self.cinema_dashboard);

            match outcome {
                EpisodeOutcome::AdvanceNext { mut returned_conn } => {
                    let _ = history.record_watch_state(anime, ep.number, None, true);
                    let (Some(next_i), Some(next)) = (target_next_idx, next_ep) else {
                        log_info(&format!(
                            "Finished final episode ({:02}) of {}.",
                            ep.number, anime.title
                        ));
                        if let Some(mut c) = child.take() {
                            if let Some(mut sc) = returned_conn {
                                let _ =
                                    send_ipc_command(&mut sc, json!({"command": ["quit"]})).await;
                            }
                            reap_mpv_child(&mut c).await;
                        }
                        if let Some(p) = active_proxy.take() {
                            p.shutdown().await;
                        }
                        return Ok(PlaybackStop::CompletedSeries);
                    };

                    if next_i > idx + 1 {
                        let skip_msg = format!(
                            "Skipped Filler Ep {:02} → Ep {:02}",
                            episodes[idx + 1].number,
                            next.number
                        );
                        log_info(&skip_msg);
                        if let Some(ref mut sc) = returned_conn {
                            let _ = send_show_text(sc, &skip_msg, 3000).await;
                        }
                        pending_osd_toast = Some(skip_msg);
                    }

                    episode_streams = prefetch
                        .take_or_resolve(
                            &next,
                            *session_quality,
                            &stalled_hosts,
                            &engine,
                            &resolver,
                        )
                        .await?;
                    stream_idx = 0;
                    tried_streams.clear();
                    tried_providers.clear();
                    tried_providers.insert(next.provider);
                    for s in &episode_streams {
                        tried_providers.insert(s.provider);
                    }
                    retried_direct_on_current = false;
                    resume_anchor_secs = None;
                    idx = next_i;

                    if let Some(c) = child.as_mut() {
                        if let Ok(Some(_)) = c.try_wait() {
                            child = None;
                            ipc_conn = None;
                        } else {
                            ipc_conn = returned_conn;
                        }
                    }
                }
                EpisodeOutcome::JumpPrev { returned_conn } => {
                    if let PrefetchState::InFlight { handle, .. } = prefetch {
                        handle.abort();
                    }
                    if idx == 0 {
                        ipc_conn = returned_conn;
                        continue;
                    }
                    idx -= 1;
                    let prev_ep = &episodes[idx];
                    let candidates = engine.get_streams(prev_ep).await?;
                    episode_streams = resolver
                        .resolve_ranked_verified_with_penalties(
                            prev_ep.number,
                            candidates,
                            *session_quality,
                            &stalled_hosts,
                        )
                        .await?;
                    stream_idx = 0;
                    tried_streams.clear();
                    tried_providers.clear();
                    tried_providers.insert(prev_ep.provider);
                    retried_direct_on_current = false;
                    resume_anchor_secs = None;

                    if let Some(c) = child.as_mut() {
                        if let Ok(Some(_)) = c.try_wait() {
                            child = None;
                            ipc_conn = None;
                        } else {
                            ipc_conn = returned_conn;
                        }
                    }
                }
                EpisodeOutcome::SwitchQuality {
                    mut returned_conn,
                    resume_secs,
                } => {
                    if let PrefetchState::InFlight { handle, .. } = prefetch {
                        handle.abort();
                    }
                    if let Some(ref mut sc) = returned_conn {
                        let _ = send_ipc_command(
                            sc,
                            json!({ "command": ["set_property", "pause", true] }),
                        )
                        .await;
                    }
                    if resume_secs > 1.0 {
                        resume_anchor_secs = Some(resume_secs as u64);
                    }

                    let turbo_opt = self.turbo_proxy.then_some(self.proxy_threads);
                    match selector::select_stream_quality(&episode_streams, self.no_fzf, turbo_opt)
                    {
                        Ok(pick) => {
                            if let Some(chosen_res) = apply_quality_pick(&mut episode_streams, pick)
                            {
                                *session_quality = Some(chosen_res);
                            }
                            stream_idx = 0;
                            tried_streams.clear();
                            retried_direct_on_current = false;
                            pending_start_secs = Some(resume_secs.max(0.0) as u64);
                            if let Some(ref mut sc) = returned_conn {
                                let new_stream = &episode_streams[0];
                                let _ = send_show_text(
                                    sc,
                                    &format!(
                                        "Switching to [{}] ({})",
                                        new_stream.resolution, new_stream.server_name
                                    ),
                                    3000,
                                )
                                .await;
                            }
                            ipc_conn = returned_conn;
                        }
                        Err(SenkuError::SelectionCancelled) => {
                            if let Some(ref mut sc) = returned_conn {
                                let _ = send_ipc_command(
                                    sc,
                                    json!({ "command": ["set_property", "pause", false] }),
                                )
                                .await;
                            }
                            ipc_conn = returned_conn;
                        }
                        Err(e) => {
                            if let Some(p) = active_proxy.take() {
                                p.shutdown().await;
                            }
                            return Err(e);
                        }
                    }
                }
                EpisodeOutcome::StallFailover {
                    mut returned_conn,
                    resume_secs,
                } => {
                    if let PrefetchState::InFlight { handle, .. } = prefetch {
                        handle.abort();
                    }
                    stalled_hosts.insert(current_host.clone());
                    if used_proxy {
                        turbo_blocked_hosts.insert(current_host.clone());
                    }
                    if resume_secs > 1.0 {
                        resume_anchor_secs = Some(resume_secs as u64);
                    }
                    pending_start_secs = resume_anchor_secs;

                    if let Some(next_idx) = pick_failover_stream_index(
                        &episode_streams,
                        stream_idx,
                        &tried_streams,
                        &stalled_hosts,
                    ) {
                        tried_streams.insert(stream_idx);
                        stream_idx = next_idx;
                        retried_direct_on_current = false;
                        log_warn(&format!(
                            "Mirror stalled >=8s — auto-switching to [{}]...",
                            episode_streams[stream_idx].server_name
                        ));
                    } else if used_proxy && !retried_direct_on_current {
                        // Only this host/stream was available: retry directly bypassing turbo proxy.
                        retried_direct_on_current = true;
                        log_warn(&format!(
                            "Turbo mirror stalled >=8s — falling back to direct stream [{}]...",
                            episode_streams[stream_idx].server_name
                        ));
                    } else {
                        tried_streams.insert(stream_idx);
                        stream_idx = episode_streams.len();
                    }

                    if let Some(ref mut sc) = returned_conn {
                        let _ = send_show_text(
                            sc,
                            "Mirror stalled — switching to faster server...",
                            3000,
                        )
                        .await;
                    }
                    ipc_conn = returned_conn;
                }
                EpisodeOutcome::StreamError {
                    reason,
                    resume_secs,
                    mut returned_conn,
                } => {
                    if let PrefetchState::InFlight { handle, .. } = prefetch {
                        handle.abort();
                    }
                    stalled_hosts.insert(current_host.clone());
                    if used_proxy {
                        turbo_blocked_hosts.insert(current_host.clone());
                    }
                    if resume_secs > 1.0 {
                        resume_anchor_secs = Some(resume_secs as u64);
                    }
                    pending_start_secs = resume_anchor_secs;

                    // Keep active mpv process + IPC conn only if still running cleanly;
                    // otherwise reap and respawn cleanly on next mirror.
                    if let Some(c) = child.as_mut() {
                        match c.try_wait() {
                            Ok(None) => {
                                ipc_conn = returned_conn.take();
                            }
                            _ => {
                                child = None;
                                ipc_conn = None;
                            }
                        }
                    }

                    tried_streams.insert(stream_idx);
                    if let Some(next_idx) = pick_failover_stream_index(
                        &episode_streams,
                        stream_idx,
                        &tried_streams,
                        &stalled_hosts,
                    ) {
                        stream_idx = next_idx;
                        retried_direct_on_current = false;
                        log_warn(&format!(
                            "Stream error ({reason}); failing over to [{}]...",
                            episode_streams[stream_idx].server_name
                        ));
                    } else if used_proxy && !retried_direct_on_current {
                        retried_direct_on_current = true;
                        log_warn(&format!(
                            "Proxy stream error ({reason}); retrying direct [{}]...",
                            episode_streams[stream_idx].server_name
                        ));
                    } else {
                        log_warn(&format!(
                            "Stream error ({reason}); checking backup providers..."
                        ));
                        stream_idx = episode_streams.len();
                    }
                    continue;
                }
                EpisodeOutcome::UserQuit {
                    returned_conn,
                    saved_pos,
                } => {
                    if let PrefetchState::InFlight { handle, .. } = prefetch {
                        handle.abort();
                    }
                    let _ = history.record_watch_at(anime, ep.number, saved_pos);
                    if let Some(mut sc) = returned_conn {
                        let _ = send_ipc_command(&mut sc, json!({"command": ["quit"]})).await;
                    }
                    if let Some(mut c) = child.take() {
                        reap_mpv_child(&mut c).await;
                    }
                    if let Some(p) = active_proxy.take() {
                        p.shutdown().await;
                    }
                    return Ok(PlaybackStop::UserStopped { episode_index: idx });
                }
                EpisodeOutcome::Interrupted { saved_pos } => {
                    if let PrefetchState::InFlight { handle, .. } = prefetch {
                        handle.abort();
                    }
                    let _ = history.record_watch_at(anime, ep.number, saved_pos);
                    if let Some(mut c) = child.take() {
                        let _ = c.kill().await;
                        let _ = c.wait().await;
                    }
                    if let Some(p) = active_proxy.take() {
                        p.shutdown().await;
                    }
                    return Ok(PlaybackStop::Interrupted);
                }
            }
        }
    }
}

enum EpisodeOutcome {
    AdvanceNext {
        returned_conn: Option<UnixStream>,
    },
    JumpPrev {
        returned_conn: Option<UnixStream>,
    },
    SwitchQuality {
        returned_conn: Option<UnixStream>,
        resume_secs: f64,
    },
    StallFailover {
        returned_conn: Option<UnixStream>,
        resume_secs: f64,
    },
    StreamError {
        reason: String,
        resume_secs: f64,
        returned_conn: Option<UnixStream>,
    },
    UserQuit {
        returned_conn: Option<UnixStream>,
        saved_pos: Option<u64>,
    },
    Interrupted {
        saved_pos: Option<u64>,
    },
}

/// Attempts cross-provider failover (`OkAnime` <-> `Animerco`) for `episode_num`
/// when all mirrors on the current provider fail or stall.
async fn try_cross_provider_failover(
    anime_title: &str,
    episode_num: u32,
    tried_providers: &mut HashSet<ProviderId>,
    stalled_hosts: &HashSet<String>,
    preferred_quality: Option<Resolution>,
    engine: &ProviderEngine,
    resolver: &StreamResolver,
) -> Option<(ProviderId, Vec<StreamSource>)> {
    while let Some((alt_anime, alt_ep)) = engine
        .find_alternate_provider_episode(
            anime_title,
            episode_num,
            tried_providers,
            Duration::from_secs(4),
        )
        .await
    {
        let prov = alt_anime.provider;
        tried_providers.insert(prov);
        log_info(&format!(
            "Switching provider to {}...",
            prov.display_name().bright_cyan()
        ));

        if let Ok(candidates) = engine.get_streams(&alt_ep).await {
            if let Ok(verified) = resolver
                .resolve_ranked_verified_with_penalties(
                    episode_num,
                    candidates,
                    preferred_quality,
                    stalled_hosts,
                )
                .await
            {
                if !verified.is_empty() {
                    return Some((prov, verified));
                }
            }
        }
    }
    None
}

/// Waits briefly for `mpv` to exit cleanly after IPC `quit`, force-killing and
/// reaping the child process if it stalls so zero zombies or orphans ever remain.
async fn reap_mpv_child(child: &mut Child) {
    if tokio::time::timeout(Duration::from_millis(1500), child.wait())
        .await
        .is_err()
    {
        let _ = child.kill().await;
        let _ = child.wait().await;
    }
}

async fn connect_ipc_with_retry(socket_path: &Path) -> Result<UnixStream> {
    for _ in 0..40 {
        if let Ok(stream) = UnixStream::connect(socket_path).await {
            return Ok(stream);
        }
        tokio::time::sleep(Duration::from_millis(75)).await;
    }
    Err(SenkuError::MpvIpc(format!(
        "timed out waiting for mpv to create {}",
        socket_path.display()
    )))
}

async fn send_ipc_command(conn: &mut UnixStream, payload: Value) -> Result<()> {
    let mut line = serde_json::to_string(&payload)?;
    line.push('\n');
    conn.write_all(line.as_bytes()).await?;
    conn.flush().await?;
    Ok(())
}

async fn send_writer_command<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    payload: Value,
) -> Result<()> {
    let mut line = serde_json::to_string(&payload)?;
    line.push('\n');
    writer.write_all(line.as_bytes()).await?;
    writer.flush().await?;
    Ok(())
}

pub async fn send_show_text(conn: &mut UnixStream, text: &str, duration_ms: u64) -> Result<()> {
    send_ipc_command(conn, json!({ "command": ["show-text", text, duration_ms] })).await
}

pub async fn send_loadfile(
    conn: &mut UnixStream,
    url: &str,
    title: &str,
    referer: Option<&str>,
    start_secs: Option<f64>,
) -> Result<()> {
    send_ipc_command(
        conn,
        json!({ "command": ["set_property", "user-agent", CHROME_USER_AGENT] }),
    )
    .await?;

    let is_local_proxy = url.starts_with("http://127.0.0.1");
    match referer.filter(|r| !r.is_empty() && !is_local_proxy) {
        Some(ref_url) => {
            send_ipc_command(
                conn,
                json!({ "command": ["set_property", "referrer", ref_url] }),
            )
            .await?;
            send_ipc_command(
                conn,
                json!({
                    "command": ["set_property", "http-header-fields", [format!("Referer: {ref_url}")]]
                }),
            )
            .await?;
        }
        None => {
            let _ = send_ipc_command(conn, json!({ "command": ["set_property", "referrer", ""] }))
                .await;
            let _ = send_ipc_command(
                conn,
                json!({ "command": ["set_property", "http-header-fields", []] }),
            )
            .await;
        }
    }

    if let Some(secs) = start_secs.filter(|&s| s > 1.0) {
        send_ipc_command(
            conn,
            json!({ "command": ["set_property", "start", format!("{secs:.1}")] }),
        )
        .await?;
    } else {
        let _ = send_ipc_command(
            conn,
            json!({ "command": ["set_property", "start", "none"] }),
        )
        .await;
    }
    send_ipc_command(conn, json!({ "command": ["loadfile", url, "replace"] })).await?;
    send_ipc_command(
        conn,
        json!({ "command": ["set_property", "force-media-title", title] }),
    )
    .await?;
    send_ipc_command(conn, json!({ "command": ["set_property", "title", title] })).await?;
    send_ipc_command(conn, json!({ "command": ["set_property", "pause", false] })).await?;
    let _ = send_ipc_command(
        conn,
        json!({ "command": ["set_property", "start", "none"] }),
    )
    .await;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn monitor_episode(
    mut conn: UnixStream,
    child: &mut Child,
    mut status: PlaybackStatus,
    anime: &AnimeResult,
    has_prev: bool,
    has_backup_stream: bool,
    next_ep: Option<Episode>,
    preferred_quality: Option<Resolution>,
    stalled_hosts: HashSet<String>,
    proxy_stats: Option<Arc<ProxyStats>>,
    http_client: Client,
    auto_skip_op: bool,
    auto_skip_ed: bool,
    cinema_dashboard: bool,
    live_autoplay: &mut bool,
    engine: Arc<ProviderEngine>,
    resolver: Arc<StreamResolver>,
    prefetch: &mut PrefetchState,
    history: &HistoryStore,
    raw_hotkeys_enabled: bool,
) -> Result<EpisodeOutcome> {
    send_ipc_command(
        &mut conn,
        json!({ "command": ["observe_property", 1, "percent-pos"] }),
    )
    .await?;
    send_ipc_command(
        &mut conn,
        json!({ "command": ["observe_property", 2, "eof-reached"] }),
    )
    .await?;
    send_ipc_command(
        &mut conn,
        json!({ "command": ["observe_property", 3, "time-pos"] }),
    )
    .await?;
    send_ipc_command(
        &mut conn,
        json!({ "command": ["observe_property", 4, "duration"] }),
    )
    .await?;
    send_ipc_command(
        &mut conn,
        json!({ "command": ["observe_property", 5, "pause"] }),
    )
    .await?;
    send_ipc_command(
        &mut conn,
        json!({ "command": ["observe_property", 6, "demuxer-cache-duration"] }),
    )
    .await?;
    send_ipc_command(
        &mut conn,
        json!({ "command": ["observe_property", 7, "paused-for-cache"] }),
    )
    .await?;

    let mut watchdog = StallWatchdog::new(STALL_FAILOVER_THRESHOLD);
    let mut skip_intervals: Vec<SkipInterval> = Vec::new();
    let mut auto_skipped_op = false;
    let mut auto_skipped_ed = false;

    let title_for_skip = anime.title.clone();
    let ep_for_skip = status.episode_num;
    let mut aniskip_task = tokio::spawn(async move {
        fetch_episode_skips(&http_client, &title_for_skip, ep_for_skip).await
    });
    let mut aniskip_done = false;
    let mut marked_watched = false;

    let (reader_half, mut writer_half) = conn.into_split();
    let mut lines = BufReader::new(reader_half).lines();
    let mut poll_interval = tokio::time::interval(Duration::from_millis(500));
    let mut save_interval = tokio::time::interval(Duration::from_secs(5));
    // Skip immediate first tick at t=0.
    save_interval.tick().await;

    let mut key_stream = if raw_hotkeys_enabled {
        Some(EventStream::new())
    } else {
        None
    };

    status.render_active(cinema_dashboard, &skip_intervals);

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                aniskip_task.abort();
                let saved_pos = persisted_position(status.time_pos, status.percent_pos);
                return Ok(EpisodeOutcome::Interrupted { saved_pos });
            }
            joined = &mut aniskip_task, if !aniskip_done => {
                aniskip_done = true;
                if let Ok(intervals) = joined {
                    if !intervals.is_empty() {
                        let chapters = build_chapter_list(&intervals);
                        if !chapters.is_empty() {
                            let _ = send_writer_command(
                                &mut writer_half,
                                json!({
                                    "command": ["set_property", "chapter-list", chapters_to_mpv_json(&chapters)]
                                }),
                            )
                            .await;
                        }
                        status.op_detected = intervals.iter().any(|i| i.skip_type.is_op());
                        skip_intervals = intervals;
                        status.render_active(cinema_dashboard, &skip_intervals);
                    }
                }
            }
            _ = save_interval.tick() => {
                let saved_pos = persisted_position(status.time_pos, status.percent_pos);
                let is_done = marked_watched || status.percent_pos >= 90.0;
                if is_done {
                    marked_watched = true;
                }
                let _ = history.record_watch_state(anime, status.episode_num, saved_pos, is_done);
            }
            _ = poll_interval.tick() => {
                prefetch.poll_ready().await;
                if let Some(ref ps) = proxy_stats {
                    status.turbo_workers = Some(ps.active_workers());
                    status.proxy_bps = ps.current_throughput_bps();
                }
                if has_backup_stream
                    && watchdog.update(!status.paused && status.paused_for_cache, Instant::now())
                {
                    let reassembled = lines.into_inner().into_inner().reunite(writer_half).ok();
                    return Ok(EpisodeOutcome::StallFailover {
                        returned_conn: reassembled,
                        resume_secs: status.time_pos,
                    });
                }
                for (prop, req_id) in [
                    ("percent-pos", REQ_PERCENT_POS),
                    ("time-pos", REQ_TIME_POS),
                    ("duration", REQ_DURATION),
                    ("pause", REQ_PAUSE),
                    ("demuxer-cache-duration", REQ_CACHE_DUR),
                    ("paused-for-cache", REQ_PAUSED_CACHE),
                ] {
                    if send_writer_command(
                        &mut writer_half,
                        json!({ "command": ["get_property", prop], "request_id": req_id }),
                    ).await.is_err() {
                        break;
                    }
                }
                status.render_active(cinema_dashboard, &skip_intervals);
            }
            maybe_key = async {
                match key_stream.as_mut() {
                    Some(s) => s.next().await,
                    None => std::future::pending().await,
                }
            } => {
                if let Some(Ok(Event::Key(KeyEvent { code, modifiers, .. }))) = maybe_key {
                    if modifiers.contains(KeyModifiers::CONTROL) && matches!(code, KeyCode::Char('c') | KeyCode::Char('C')) {
                        let saved_pos = persisted_position(status.time_pos, status.percent_pos);
                        return Ok(EpisodeOutcome::Interrupted { saved_pos });
                    }
                    match map_hotkey_action(code, &status, &skip_intervals) {
                        Some(LiveRemoteAction::CyclePause) => {
                            let _ = send_writer_command(
                                &mut writer_half,
                                json!({ "command": ["cycle", "pause"] }),
                            ).await;
                        }
                        Some(LiveRemoteAction::SeekRelative { secs, osd }) => {
                            let _ = send_writer_command(
                                &mut writer_half,
                                json!({ "command": ["seek", secs, "relative"] }),
                            ).await;
                            let _ = send_writer_command(
                                &mut writer_half,
                                json!({ "command": ["show-text", osd, 2000] }),
                            ).await;
                        }
                        Some(LiveRemoteAction::SeekAbsolute { secs, osd }) => {
                            let _ = send_writer_command(
                                &mut writer_half,
                                json!({ "command": ["seek", secs, "absolute"] }),
                            ).await;
                            let _ = send_writer_command(
                                &mut writer_half,
                                json!({ "command": ["show-text", osd, 2000] }),
                            ).await;
                        }
                        Some(LiveRemoteAction::SetVolume { volume }) => {
                            status.volume = volume;
                            let _ = send_writer_command(
                                &mut writer_half,
                                json!({ "command": ["set_property", "volume", volume] }),
                            ).await;
                            let _ = send_writer_command(
                                &mut writer_half,
                                json!({ "command": ["show-text", format!("Volume: {volume}%"), 1500] }),
                            ).await;
                            status.render_active(cinema_dashboard, &skip_intervals);
                        }
                        Some(LiveRemoteAction::SetMute { muted }) => {
                            status.muted = muted;
                            let _ = send_writer_command(
                                &mut writer_half,
                                json!({ "command": ["set_property", "mute", muted] }),
                            ).await;
                            let label = if muted { "Muted" } else { "Unmuted" };
                            let _ = send_writer_command(
                                &mut writer_half,
                                json!({ "command": ["show-text", label, 1500] }),
                            ).await;
                            status.render_active(cinema_dashboard, &skip_intervals);
                        }
                        Some(LiveRemoteAction::SetSpeed { speed }) => {
                            status.speed = speed;
                            let _ = send_writer_command(
                                &mut writer_half,
                                json!({ "command": ["set_property", "speed", speed] }),
                            ).await;
                            let _ = send_writer_command(
                                &mut writer_half,
                                json!({ "command": ["show-text", format!("Speed: {speed:.2}x"), 1500] }),
                            ).await;
                            status.render_active(cinema_dashboard, &skip_intervals);
                        }
                        Some(LiveRemoteAction::CycleFullscreen) => {
                            let _ = send_writer_command(
                                &mut writer_half,
                                json!({ "command": ["cycle", "fullscreen"] }),
                            ).await;
                        }
                        Some(LiveRemoteAction::ToggleAutoplay { autoplay }) => {
                            status.autoplay = autoplay;
                            *live_autoplay = autoplay;
                            let label = if autoplay { "Autoplay: ON" } else { "Autoplay: OFF" };
                            let _ = send_writer_command(
                                &mut writer_half,
                                json!({ "command": ["show-text", label, 1800] }),
                            ).await;
                            status.render_active(cinema_dashboard, &skip_intervals);
                        }
                        Some(LiveRemoteAction::SwitchQuality) => {
                            aniskip_task.abort();
                            let reassembled = lines.into_inner().into_inner().reunite(writer_half)
                                .map_err(|e| SenkuError::MpvIpc(e.to_string()))?;
                            return Ok(EpisodeOutcome::SwitchQuality {
                                returned_conn: Some(reassembled),
                                resume_secs: status.time_pos,
                            });
                        }
                        Some(LiveRemoteAction::NextEpisode) => {
                            if next_ep.is_some() {
                                aniskip_task.abort();
                                let reassembled = lines.into_inner().into_inner().reunite(writer_half)
                                    .map_err(|e| SenkuError::MpvIpc(e.to_string()))?;
                                return Ok(EpisodeOutcome::AdvanceNext {
                                    returned_conn: Some(reassembled),
                                });
                            } else {
                                let _ = send_writer_command(
                                    &mut writer_half,
                                    json!({ "command": ["show-text", "Already at final episode", 2000] }),
                                ).await;
                            }
                        }
                        Some(LiveRemoteAction::PrevEpisode) => {
                            if has_prev {
                                aniskip_task.abort();
                                let reassembled = lines.into_inner().into_inner().reunite(writer_half)
                                    .map_err(|e| SenkuError::MpvIpc(e.to_string()))?;
                                return Ok(EpisodeOutcome::JumpPrev {
                                    returned_conn: Some(reassembled),
                                });
                            } else {
                                let _ = send_writer_command(
                                    &mut writer_half,
                                    json!({ "command": ["show-text", "Already at first episode", 2000] }),
                                ).await;
                            }
                        }
                        Some(LiveRemoteAction::Quit) => {
                            aniskip_task.abort();
                            let saved_pos = persisted_position(status.time_pos, status.percent_pos);
                            let reassembled = lines.into_inner().into_inner().reunite(writer_half)
                                .map_err(|e| SenkuError::MpvIpc(e.to_string()))?;
                            return Ok(EpisodeOutcome::UserQuit {
                                returned_conn: Some(reassembled),
                                saved_pos,
                            });
                        }
                        None => {}
                    }
                }
            }
            maybe_line = lines.next_line() => {
                match maybe_line {
                    Ok(Some(line)) => {
                        if let Some(sig) = parse_ipc_line(&line) {
                            match sig {
                                IpcSignal::PercentPos(pct) => {
                                    status.percent_pos = pct;
                                    if pct >= 90.0 && !marked_watched {
                                        marked_watched = true;
                                        let saved = persisted_position(status.time_pos, pct);
                                        let _ = history.record_watch_state(
                                            anime,
                                            status.episode_num,
                                            saved,
                                            true,
                                        );
                                    }
                                    if prefetch.should_trigger(pct, status.autoplay, next_ep.is_some()) {
                                        if let Some(ref nxt) = next_ep {
                                            prefetch.spawn_for_next(
                                                nxt.clone(),
                                                preferred_quality,
                                                stalled_hosts.clone(),
                                                Arc::clone(&engine),
                                                Arc::clone(&resolver),
                                            );
                                        }
                                    }
                                    if pct >= 99.0 {
                                        let reassembled = lines.into_inner().into_inner().reunite(writer_half)
                                            .map_err(|e| SenkuError::MpvIpc(e.to_string()))?;
                                        return Ok(EpisodeOutcome::AdvanceNext {
                                            returned_conn: Some(reassembled),
                                        });
                                    }
                                }
                                IpcSignal::TimePos(t) => {
                                    status.time_pos = t;
                                    if auto_skip_op && !auto_skipped_op {
                                        if let Some(op) = skip_intervals.iter().find(|i| i.skip_type.is_op()) {
                                            if t >= op.start_time && t < op.end_time - 1.0 {
                                                auto_skipped_op = true;
                                                let _ = send_writer_command(
                                                    &mut writer_half,
                                                    json!({ "command": ["seek", op.end_time, "absolute"] }),
                                                ).await;
                                                let _ = send_writer_command(
                                                    &mut writer_half,
                                                    json!({ "command": ["show-text", "Auto-skipped Opening (exact)", 2000] }),
                                                ).await;
                                            }
                                        }
                                    }
                                    if auto_skip_ed && !auto_skipped_ed {
                                        if let Some(ed) = skip_intervals.iter().find(|i| i.skip_type.is_ed()) {
                                            if t >= ed.start_time && t < ed.end_time {
                                                auto_skipped_ed = true;
                                                if next_ep.is_some() && status.autoplay {
                                                    let _ = send_writer_command(
                                                        &mut writer_half,
                                                        json!({ "command": ["show-text", "Auto-skipping Ending — next episode...", 2000] }),
                                                    ).await;
                                                    aniskip_task.abort();
                                                    let reassembled = lines.into_inner().into_inner().reunite(writer_half)
                                                        .map_err(|e| SenkuError::MpvIpc(e.to_string()))?;
                                                    return Ok(EpisodeOutcome::AdvanceNext {
                                                        returned_conn: Some(reassembled),
                                                    });
                                                } else if ed.end_time - t > 1.0 {
                                                    let _ = send_writer_command(
                                                        &mut writer_half,
                                                        json!({ "command": ["seek", ed.end_time, "absolute"] }),
                                                    ).await;
                                                    let _ = send_writer_command(
                                                        &mut writer_half,
                                                        json!({ "command": ["show-text", "Auto-skipped Ending", 2000] }),
                                                    ).await;
                                                }
                                            }
                                        }
                                    }
                                }
                                IpcSignal::Duration(d) => {
                                    status.duration = Some(d);
                                }
                                IpcSignal::Paused(p) => {
                                    status.paused = p;
                                    status.render_active(cinema_dashboard, &skip_intervals);
                                }
                                IpcSignal::CacheDuration(d) => {
                                    status.cache_duration_secs = Some(d);
                                }
                                IpcSignal::PausedForCache(p) => {
                                    status.paused_for_cache = p;
                                    status.render_active(cinema_dashboard, &skip_intervals);
                                    if has_backup_stream
                                        && watchdog.update(!status.paused && p, Instant::now())
                                    {
                                        let reassembled =
                                            lines.into_inner().into_inner().reunite(writer_half).ok();
                                        return Ok(EpisodeOutcome::StallFailover {
                                            returned_conn: reassembled,
                                            resume_secs: status.time_pos,
                                        });
                                    }
                                }
                                IpcSignal::EofReached(true) => {
                                    let reassembled = lines.into_inner().into_inner().reunite(writer_half)
                                        .map_err(|e| SenkuError::MpvIpc(e.to_string()))?;
                                    return Ok(EpisodeOutcome::AdvanceNext {
                                        returned_conn: Some(reassembled),
                                    });
                                }
                                IpcSignal::EndFile { reason } => {
                                    if reason == "eof" || status.percent_pos >= 99.0 {
                                        let reassembled = lines.into_inner().into_inner().reunite(writer_half)
                                            .map_err(|e| SenkuError::MpvIpc(e.to_string()))?;
                                        return Ok(EpisodeOutcome::AdvanceNext {
                                            returned_conn: Some(reassembled),
                                        });
                                    } else if reason == "error" {
                                        let reassembled =
                                            lines.into_inner().into_inner().reunite(writer_half).ok();
                                        return Ok(EpisodeOutcome::StreamError {
                                            reason: "mpv end-file error".into(),
                                            resume_secs: status.time_pos,
                                            returned_conn: reassembled,
                                        });
                                    } else if reason == "quit" {
                                        let saved_pos = persisted_position(status.time_pos, status.percent_pos);
                                        return Ok(EpisodeOutcome::UserQuit {
                                            returned_conn: None,
                                            saved_pos,
                                        });
                                    }
                                }
                                IpcSignal::EofReached(false) | IpcSignal::Other => {}
                            }
                        }
                    }
                    Ok(None) | Err(_) => {
                        let proc_status = child.wait().await?;
                        if proc_status.success() && status.percent_pos >= 99.0 {
                            return Ok(EpisodeOutcome::AdvanceNext { returned_conn: None });
                        }
                        if !proc_status.success() {
                            return Ok(EpisodeOutcome::StreamError {
                                reason: format!(
                                    "mpv exited with status {}",
                                    proc_status.code().unwrap_or(-1)
                                ),
                                resume_secs: status.time_pos,
                                returned_conn: None,
                            });
                        }
                        let saved_pos = persisted_position(status.time_pos, status.percent_pos);
                        return Ok(EpisodeOutcome::UserQuit {
                            returned_conn: None,
                            saved_pos,
                        });
                    }
                }
            }
            status_res = child.wait() => {
                let proc_status = status_res?;
                if proc_status.success() && status.percent_pos >= 99.0 {
                    return Ok(EpisodeOutcome::AdvanceNext { returned_conn: None });
                }
                if !proc_status.success() {
                    return Ok(EpisodeOutcome::StreamError {
                        reason: format!(
                            "mpv exited with status {}",
                            proc_status.code().unwrap_or(-1)
                        ),
                        resume_secs: status.time_pos,
                        returned_conn: None,
                    });
                }
                let saved_pos = persisted_position(status.time_pos, status.percent_pos);
                return Ok(EpisodeOutcome::UserQuit {
                    returned_conn: None,
                    saved_pos,
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::ProviderId;
    use crate::providers::{build_http_client, AnimeProvider};
    use async_trait::async_trait;
    use tempfile::tempdir;
    use tokio::io::AsyncReadExt;
    use tokio::net::UnixListener;

    #[test]
    fn parses_mpv_json_ipc_events_and_formats_progress_bar_status() {
        let pct_line = r#"{"event":"property-change","id":1,"name":"percent-pos","data":99.3}"#;
        assert_eq!(parse_ipc_line(pct_line), Some(IpcSignal::PercentPos(99.3)));

        let time_line = r#"{"request_id":101,"error":"success","data":57.0}"#;
        assert_eq!(parse_ipc_line(time_line), Some(IpcSignal::TimePos(57.0)));

        let dur_line = r#"{"request_id":102,"error":"success","data":1429.0}"#;
        assert_eq!(parse_ipc_line(dur_line), Some(IpcSignal::Duration(1429.0)));

        let pause_line = r#"{"event":"property-change","id":5,"name":"pause","data":true}"#;
        assert_eq!(parse_ipc_line(pause_line), Some(IpcSignal::Paused(true)));

        let cache_line = r#"{"request_id":104,"error":"success","data":48.2}"#;
        assert_eq!(
            parse_ipc_line(cache_line),
            Some(IpcSignal::CacheDuration(48.2))
        );

        let stalled_line =
            r#"{"event":"property-change","id":7,"name":"paused-for-cache","data":true}"#;
        assert_eq!(
            parse_ipc_line(stalled_line),
            Some(IpcSignal::PausedForCache(true))
        );

        let status = PlaybackStatus {
            episode_num: 7,
            resolution: Resolution::Hd720,
            time_pos: 57.0,
            duration: Some(1429.0),
            percent_pos: 4.0,
            paused: false,
            autoplay: true,
            skip_intro_secs: 85,
            cache_duration_secs: Some(48.2),
            paused_for_cache: false,
            turbo_workers: Some(3),
            proxy_bps: 2_936_012, // ~2.8MB/s
            volume: 100,
            muted: false,
            speed: 1.0,
            op_detected: false,
        };
        let (line1, line2) = status.format_lines();
        assert_eq!(
            line1,
            "▶ Ep 07 [720p] [█░░░░░░░░░░░] 00:57 / 23:49 (4%)  Buf: 48s/120s ⚡3x 2.8MB/s  [Autoplay: ON]"
        );
        assert!(
            line2.contains("[Space] Pause")
                && line2.contains("[←/→] ±10s")
                && line2.contains("[f] Full")
        );

        let mut mod_status = status.clone();
        mod_status.muted = true;
        mod_status.speed = 1.25;
        mod_status.op_detected = true;
        let (mod_line1, _) = mod_status.format_lines();
        assert!(mod_line1.contains("🔇"));
        assert!(mod_line1.contains("1.25x"));
        assert!(mod_line1.contains("[OP detected]"));

        let mut buffering_status = status.clone();
        buffering_status.paused_for_cache = true;
        let (buf_line1, _) = buffering_status.format_lines();
        assert_eq!(
            buf_line1,
            "▶ Ep 07 [720p] [█░░░░░░░░░░░] 00:57 / 23:49 (4%)  \x1b[33mBuf: 0s (buffering...)\x1b[0m  [Autoplay: ON]"
        );

        assert_eq!(render_progress_bar(0.0), "░░░░░░░░░░░░");
        assert_eq!(render_progress_bar(50.0), "██████░░░░░░");
        assert_eq!(render_progress_bar(100.0), "████████████");

        assert_eq!(persisted_position(142.8, 12.0), Some(142));
        assert_eq!(persisted_position(1380.0, 95.0), None);
        assert_eq!(persisted_position(0.5, 0.1), None);
    }

    #[test]
    fn maps_expanded_live_hotkeys_with_volume_and_speed_clamping() {
        let mut status = PlaybackStatus {
            episode_num: 1,
            resolution: Resolution::Fhd1080,
            time_pos: 40.0,
            duration: Some(1400.0),
            percent_pos: 3.0,
            paused: false,
            autoplay: true,
            skip_intro_secs: 85,
            cache_duration_secs: Some(10.0),
            paused_for_cache: false,
            turbo_workers: None,
            proxy_bps: 0,
            volume: 100,
            muted: false,
            speed: 1.0,
            op_detected: false,
        };

        assert_eq!(
            map_hotkey_action(KeyCode::Left, &status, &[]),
            Some(LiveRemoteAction::SeekRelative {
                secs: -10,
                osd: "Seek -10s".into(),
            })
        );
        assert_eq!(
            map_hotkey_action(KeyCode::Right, &status, &[]),
            Some(LiveRemoteAction::SeekRelative {
                secs: 10,
                osd: "Seek +10s".into(),
            })
        );
        assert_eq!(
            map_hotkey_action(KeyCode::Char('f'), &status, &[]),
            Some(LiveRemoteAction::CycleFullscreen)
        );
        assert_eq!(
            map_hotkey_action(KeyCode::Char('a'), &status, &[]),
            Some(LiveRemoteAction::ToggleAutoplay { autoplay: false })
        );
        assert_eq!(
            map_hotkey_action(KeyCode::Char('m'), &status, &[]),
            Some(LiveRemoteAction::SetMute { muted: true })
        );

        // Volume clamps within 0..=130.
        status.volume = 128;
        assert_eq!(
            map_hotkey_action(KeyCode::Char('+'), &status, &[]),
            Some(LiveRemoteAction::SetVolume { volume: 130 })
        );
        assert_eq!(
            map_hotkey_action(KeyCode::Char('='), &status, &[]),
            Some(LiveRemoteAction::SetVolume { volume: 130 })
        );
        status.volume = 2;
        assert_eq!(
            map_hotkey_action(KeyCode::Char('-'), &status, &[]),
            Some(LiveRemoteAction::SetVolume { volume: 0 })
        );

        // Speed clamps within 0.50..=2.00.
        status.speed = 1.90;
        assert_eq!(
            map_hotkey_action(KeyCode::Char(']'), &status, &[]),
            Some(LiveRemoteAction::SetSpeed { speed: 2.0 })
        );
        status.speed = 0.60;
        assert_eq!(
            map_hotkey_action(KeyCode::Char('['), &status, &[]),
            Some(LiveRemoteAction::SetSpeed { speed: 0.5 })
        );
    }

    #[test]
    fn stall_watchdog_triggers_at_8s_and_resets_on_recovery() {
        let mut watchdog = StallWatchdog::new(STALL_FAILOVER_THRESHOLD);
        let t0 = Instant::now();
        assert!(!watchdog.update(false, t0));
        assert!(!watchdog.update(true, t0));
        assert!(!watchdog.update(true, t0 + Duration::from_secs(7)));
        // Recovery before threshold resets the timer.
        assert!(!watchdog.update(
            false,
            t0 + Duration::from_secs(7) + Duration::from_millis(500)
        ));
        assert!(!watchdog.update(true, t0 + Duration::from_secs(8)));
        assert!(!watchdog.update(true, t0 + Duration::from_secs(15)));
        assert!(watchdog.update(true, t0 + Duration::from_secs(16)));
    }

    #[test]
    fn builds_mpv_args_with_cache_readahead_start_and_kitty_flags() {
        let stream = StreamSource {
            url: "https://cdn.example.com/ep1.mp4".into(),
            resolution: Resolution::Fhd1080,
            provider: ProviderId::Anime3rb,
            server_name: "cdn".into(),
            referer: Some("https://anime3rb.com".into()),
            content_length: None,
            verified: true,
            throughput_kbps: None,
            supports_ranges: false,
        };

        let sock = default_ipc_socket();
        let args = build_mpv_args(&stream, "Steins;Gate E01", &sock, true, Some(185));
        assert!(args.contains(&format!("--input-ipc-server={}", sock.display())));
        assert!(args.contains(&"--cache=yes".to_string()));
        assert!(args.contains(&"--cache-secs=120".to_string()));
        assert!(args.contains(&"--demuxer-readahead-secs=120".to_string()));
        assert!(args.contains(&"--demuxer-max-bytes=180MiB".to_string()));
        assert!(args.contains(&"--demuxer-max-back-bytes=12MiB".to_string()));
        assert!(args.contains(&"--network-timeout=15".to_string()));
        assert!(args.contains(&"--cache-pause-initial=yes".to_string()));
        assert!(args.contains(&"--cache-pause-wait=1.5".to_string()));
        assert!(args.contains(
            &"--stream-lavf-o=reconnect=1,reconnect_streamed=1,reconnect_delay_max=5".to_string()
        ));
        assert!(args.contains(&"--start=185".to_string()));
        assert!(args.contains(&"--vo=kitty".to_string()));
        assert!(args.contains(&"--vo-kitty-use-shm=yes".to_string()));
        assert!(args.contains(&"--referrer=https://anime3rb.com".to_string()));

        // Short <= 10s timestamps should NOT emit --start.
        let args_no_start = build_mpv_args(&stream, "Steins;Gate E01", &sock, false, Some(8));
        assert!(!args_no_start.iter().any(|a| a.starts_with("--start=")));
    }

    #[test]
    fn host_aware_failover_skips_stalled_host_for_healthy_alternative() {
        let streams = vec![
            StreamSource {
                url: "https://a4.mp4upload.com:282/d/ep1-1080p.mp4".into(),
                resolution: Resolution::Fhd1080,
                provider: ProviderId::OkAnime,
                server_name: "mp4upload FHD".into(),
                referer: None,
                content_length: Some(250_000_000),
                verified: true,
                throughput_kbps: Some(900),
                supports_ranges: true,
            },
            StreamSource {
                url: "https://s9.mp4upload.com:282/d/ep1-720p.mp4".into(),
                resolution: Resolution::Hd720,
                provider: ProviderId::OkAnime,
                server_name: "mp4upload HD".into(),
                referer: None,
                content_length: Some(150_000_000),
                verified: true,
                throughput_kbps: Some(800),
                supports_ranges: true,
            },
            StreamSource {
                url: "https://cdn.vidcache.net/hls/ep1-720p.m3u8".into(),
                resolution: Resolution::Hd720,
                provider: ProviderId::OkAnime,
                server_name: "vidcache HD".into(),
                referer: None,
                content_length: None,
                verified: true,
                throughput_kbps: Some(2400),
                supports_ranges: false,
            },
        ];

        let mut tried = HashSet::new();
        let mut stalled = HashSet::new();
        // mp4upload FHD stalls -> mark host stalled & index 0 tried.
        tried.insert(0);
        stalled.insert(stream_host_key(&streams[0]));
        assert_eq!(stream_host_key(&streams[0]), "mp4upload.com");
        assert_eq!(stream_host_key(&streams[1]), "mp4upload.com");

        // Must jump directly to index 2 (vidcache), skipping index 1 (HD mp4upload.com).
        let picked = pick_failover_stream_index(&streams, 0, &tried, &stalled);
        assert_eq!(picked, Some(2));

        // Only if vidcache is also tried should Pass 2 fall back to HD mp4upload.
        tried.insert(2);
        let fallback_same_host = pick_failover_stream_index(&streams, 2, &tried, &stalled);
        assert_eq!(fallback_same_host, Some(1));
    }

    #[test]
    fn pid_scoped_socket_guard_cleans_up_on_drop() {
        let dir = tempdir().unwrap();
        let pid_sock = dir
            .path()
            .join(format!("senku-anime-cli-{}.sock", std::process::id()));
        assert!(default_ipc_socket()
            .to_string_lossy()
            .contains(&std::process::id().to_string()));

        std::fs::write(&pid_sock, b"").unwrap();
        assert!(pid_sock.exists());
        {
            let guard = SocketGuard::new(&pid_sock);
            assert_eq!(guard.path(), pid_sock.as_path());
        }
        assert!(
            !pid_sock.exists(),
            "SocketGuard must delete socket file on drop"
        );
    }

    #[tokio::test]
    async fn prefetch_state_triggers_at_85_percent_and_resolves_zero_delay() {
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = [0u8; 1024];
                    let _ = sock.read(&mut buf).await;
                    let resp = "HTTP/1.1 200 OK\r\nContent-Type: video/mp4\r\nContent-Length: 2048\r\nConnection: close\r\n\r\n";
                    let _ = sock.write_all(resp.as_bytes()).await;
                });
            }
        });
        let media_url = format!("http://{addr}/ep2.mp4");

        struct MockNextProvider {
            media_url: String,
        }

        #[async_trait]
        impl AnimeProvider for MockNextProvider {
            fn id(&self) -> ProviderId {
                ProviderId::Animerco
            }
            fn base_url(&self) -> &str {
                "https://example.invalid"
            }
            async fn search(&self, _q: &str) -> Result<Vec<AnimeResult>> {
                Ok(vec![])
            }
            async fn get_episodes(&self, _a: &AnimeResult) -> Result<Vec<Episode>> {
                Ok(vec![])
            }
            async fn get_streams(&self, _ep: &Episode) -> Result<Vec<StreamSource>> {
                Ok(vec![StreamSource {
                    url: self.media_url.clone(),
                    resolution: Resolution::Unknown,
                    provider: ProviderId::Animerco,
                    server_name: "Michi FHD".into(),
                    referer: None,
                    content_length: None,
                    verified: false,
                    throughput_kbps: None,
                    supports_ranges: false,
                }])
            }
        }

        let client = build_http_client(Duration::from_secs(3)).unwrap();
        let engine = Arc::new(ProviderEngine::with_providers(vec![Arc::new(
            MockNextProvider {
                media_url: media_url.clone(),
            },
        )]));
        let resolver = Arc::new(StreamResolver::new(client));

        let mut prefetch = PrefetchState::Idle;
        assert!(!prefetch.should_trigger(84.9, true, true));
        assert!(!prefetch.should_trigger(90.0, false, true));
        assert!(!prefetch.should_trigger(90.0, true, false));
        assert!(prefetch.should_trigger(85.0, true, true));

        let next_ep = Episode {
            number: 2,
            title: Some("Ep 2".into()),
            url: "https://example.invalid/ep/2".into(),
            provider: ProviderId::Animerco,
            sources: Vec::new(),
        };
        prefetch.spawn_for_next(
            next_ep.clone(),
            None,
            HashSet::new(),
            Arc::clone(&engine),
            Arc::clone(&resolver),
        );
        assert!(matches!(
            prefetch,
            PrefetchState::InFlight { episode_num: 2, .. }
        ));
        assert!(!prefetch.should_trigger(92.0, true, true));

        tokio::time::sleep(Duration::from_millis(50)).await;
        prefetch.poll_ready().await;
        assert!(matches!(
            prefetch,
            PrefetchState::Ready { episode_num: 2, .. }
        ));

        let resolved = prefetch
            .take_or_resolve(&next_ep, None, &HashSet::new(), &engine, &resolver)
            .await
            .unwrap();
        assert_eq!(resolved[0].url, media_url);
        assert_eq!(resolved[0].resolution, Resolution::Fhd1080);
    }

    #[tokio::test]
    async fn ipc_socket_triggers_autoplay_on_99_percent_and_sends_osd_plus_loadfile() {
        let dir = tempdir().unwrap();
        let sock_path = dir.path().join("test-mpv.sock");
        let listener = UnixListener::bind(&sock_path).unwrap();

        let server_task = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            sock.write_all(
                br#"{"event":"property-change","id":1,"name":"percent-pos","data":99.2}
"#,
            )
            .await
            .unwrap();

            let mut acc = String::new();
            let mut buf = [0u8; 2048];
            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            while tokio::time::Instant::now() < deadline {
                match tokio::time::timeout(Duration::from_millis(200), sock.read(&mut buf)).await {
                    Ok(Ok(0)) | Err(_) => break,
                    Ok(Ok(n)) => {
                        acc.push_str(&String::from_utf8_lossy(&buf[..n]));
                        if acc.contains("loadfile") {
                            break;
                        }
                    }
                    Ok(Err(_)) => break,
                }
            }
            acc
        });

        let client = build_http_client(Duration::from_secs(3)).unwrap();
        let engine = Arc::new(ProviderEngine::with_providers(vec![]));
        let resolver = Arc::new(StreamResolver::new(client.clone()));
        let history = HistoryStore::with_path(dir.path().join("history.json"));
        let mut prefetch = PrefetchState::Idle;

        let anime = AnimeResult {
            slug: "steins-gate".into(),
            title: "Steins;Gate".into(),
            alt_title: None,
            url: "https://example.invalid/steins-gate".into(),
            provider: ProviderId::Animerco,
            year: None,
            episode_count: None,
            latency_ms: 10,
            sources: Vec::new(),
        };

        let conn = connect_ipc_with_retry(&sock_path).await.unwrap();
        let mut dummy_child = Command::new("sleep").arg("2").spawn().unwrap();
        let status = PlaybackStatus {
            episode_num: 1,
            resolution: Resolution::Fhd1080,
            time_pos: 0.0,
            duration: None,
            percent_pos: 0.0,
            paused: false,
            autoplay: true,
            skip_intro_secs: 85,
            cache_duration_secs: None,
            paused_for_cache: false,
            turbo_workers: None,
            proxy_bps: 0,
            volume: 100,
            muted: false,
            speed: 1.0,
            op_detected: false,
        };
        let mut live_ap = true;
        let outcome = monitor_episode(
            conn,
            &mut dummy_child,
            status,
            &anime,
            false,
            false,
            None,
            None,
            HashSet::new(),
            None,
            client.clone(),
            false,
            false,
            false,
            &mut live_ap,
            engine,
            resolver,
            &mut prefetch,
            &history,
            false,
        )
        .await
        .unwrap();
        let _ = dummy_child.kill().await;

        match outcome {
            EpisodeOutcome::AdvanceNext { returned_conn } => {
                let mut stream = returned_conn.unwrap();
                send_show_text(&mut stream, "Playing Episode 02 [1080p]", 3000)
                    .await
                    .unwrap();
                send_loadfile(
                    &mut stream,
                    "https://cdn.example.com/ep2.mp4",
                    "Ep 2",
                    Some("https://ww3.okanime.xyz/"),
                    None,
                )
                .await
                .unwrap();
            }
            _ => panic!("expected AdvanceNext at >= 99%"),
        }

        let received = server_task.await.unwrap();
        assert!(received.contains("observe_property"));
        assert!(received.contains("show-text"));
        assert!(received.contains("Playing Episode 02 [1080p]"));
        assert!(received.contains("loadfile"));
    }

    #[tokio::test]
    async fn send_loadfile_sets_and_clears_referrer_headers_before_loadfile() {
        let dir = tempdir().unwrap();
        let sock_path = dir.path().join("loadfile-headers.sock");
        let listener = UnixListener::bind(&sock_path).unwrap();

        let server_task = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut acc = String::new();
            let mut buf = [0u8; 4096];
            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            while tokio::time::Instant::now() < deadline {
                match tokio::time::timeout(Duration::from_millis(120), sock.read(&mut buf)).await {
                    Ok(Ok(0)) | Err(_) => break,
                    Ok(Ok(n)) => {
                        acc.push_str(&String::from_utf8_lossy(&buf[..n]));
                        if acc.matches("loadfile").count() >= 2 {
                            break;
                        }
                    }
                    Ok(Err(_)) => break,
                }
            }
            acc
        });

        let mut conn = connect_ipc_with_retry(&sock_path).await.unwrap();
        // 1. Direct upstream URL -> must set referrer, user-agent, and http-header-fields BEFORE loadfile.
        send_loadfile(
            &mut conn,
            "https://s1.mp4upload.com/d/video.mp4",
            "DBS — Ep 06 [1080p]",
            Some("https://www.mp4upload.com/embed-abc.html"),
            Some(51.0),
        )
        .await
        .unwrap();

        // 2. Local proxy URL -> must clear referrer and http-header-fields BEFORE loadfile.
        send_loadfile(
            &mut conn,
            "http://127.0.0.1:45123/stream",
            "DBS — Ep 06 [720p]",
            Some("https://should-be-cleared.invalid"),
            Some(51.0),
        )
        .await
        .unwrap();

        drop(conn);
        let received = server_task.await.unwrap();
        let lines: Vec<&str> = received.lines().collect();

        let first_load_idx = lines
            .iter()
            .position(|l| l.contains(r#""loadfile""#))
            .expect("first loadfile");
        let ref_set_idx = lines
            .iter()
            .position(|l| {
                l.contains(r#""referrer""#)
                    && l.contains("https://www.mp4upload.com/embed-abc.html")
            })
            .expect("referrer set before direct loadfile");
        let hdr_set_idx = lines
            .iter()
            .position(|l| {
                l.contains(r#""http-header-fields""#)
                    && l.contains("Referer: https://www.mp4upload.com/embed-abc.html")
            })
            .expect("http-header-fields set before direct loadfile");
        assert!(ref_set_idx < first_load_idx);
        assert!(hdr_set_idx < first_load_idx);
        assert!(lines.iter().any(|l| l.contains(r#""user-agent""#)));

        let second_load_idx = lines
            .iter()
            .rposition(|l| l.contains(r#""loadfile""#))
            .expect("second loadfile");
        let clear_ref_idx = lines[first_load_idx..second_load_idx]
            .iter()
            .position(|l| l.contains(r#""referrer","""#))
            .expect("referrer cleared before proxy loadfile");
        let clear_hdr_idx = lines[first_load_idx..second_load_idx]
            .iter()
            .position(|l| l.contains(r#""http-header-fields",[]"#))
            .expect("http-header-fields cleared before proxy loadfile");
        assert!(clear_ref_idx < second_load_idx);
        assert!(clear_hdr_idx < second_load_idx);
    }

    #[tokio::test]
    async fn play_loop_recovers_from_end_file_error_via_cross_provider_failover_and_never_crashes()
    {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempdir().unwrap();
        let mock_mpv = dir.path().join("mock-mpv.py");
        let sock_path = dir.path().join("play-loop-test.sock");
        let log_path = dir.path().join("mock-events.log");

        let script = r#"#!/usr/bin/env python3
import json, os, socket, sys, time

if "--version" in sys.argv:
    print("mpv 0.41.0-mock")
    sys.exit(0)

ipc_path = None
init_url = None
for arg in sys.argv[1:]:
    if arg.startswith("--input-ipc-server="):
        ipc_path = arg.split("=", 1)[1]
    elif not arg.startswith("--"):
        init_url = arg

log_file = os.environ.get("MOCK_LOG_FILE", "")
def log(msg):
    if log_file:
        with open(log_file, "a") as f:
            f.write(msg + "\n")

log(f"LAUNCH:{init_url}")
if os.path.exists(ipc_path):
    os.unlink(ipc_path)
srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
srv.bind(ipc_path)
srv.listen(1)
conn, _ = srv.accept()
conn.settimeout(0.05)

def send(obj):
    conn.sendall((json.dumps(obj) + "\n").encode())

# Emit end-file error for primary mirror after 51s time-pos, then complete on backup URL.
send({"event": "property-change", "id": 3, "name": "time-pos", "data": 51.0})
time.sleep(0.05)
send({"event": "end-file", "reason": "error"})

buf = b""
current_url = init_url
while True:
    try:
        chunk = conn.recv(4096)
        if not chunk:
            break
        buf += chunk
        while b"\n" in buf:
            line, buf = buf.split(b"\n", 1)
            if not line.strip():
                continue
            cmd = json.loads(line.decode())
            c = cmd.get("command", [])
            if c and c[0] == "loadfile":
                current_url = c[1]
                log(f"LOADFILE:{current_url}")
                if "always-fail" in current_url:
                    send({"event": "end-file", "reason": "error"})
                elif "backup-ok" in current_url:
                    send({"event": "property-change", "id": 1, "name": "percent-pos", "data": 99.5})
            elif c and c[0] == "quit":
                srv.close()
                sys.exit(0)
    except socket.timeout:
        continue
"#;
        std::fs::write(&mock_mpv, script).unwrap();
        let mut perms = std::fs::metadata(&mock_mpv).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&mock_mpv, perms).unwrap();

        // Mock HTTP server for Animerco backup stream verification.
        let http_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = http_listener.accept().await {
                tokio::spawn(async move {
                    let mut b = [0u8; 1024];
                    let _ = sock.read(&mut b).await;
                    let req = String::from_utf8_lossy(&b);
                    let resp = if req.contains("backup-ok") {
                        "HTTP/1.1 200 OK\r\nContent-Type: video/mp4\r\nContent-Length: 1048576\r\nConnection: close\r\n\r\n"
                    } else {
                        "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    };
                    let _ = sock.write_all(resp.as_bytes()).await;
                });
            }
        });
        let backup_url = format!("http://{http_addr}/backup-ok.mp4");

        struct FailPrimaryProvider;
        struct HealthyBackupProvider {
            url: String,
        }

        #[async_trait]
        impl AnimeProvider for FailPrimaryProvider {
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
        impl AnimeProvider for HealthyBackupProvider {
            fn id(&self) -> ProviderId {
                ProviderId::Animerco
            }
            fn base_url(&self) -> &str {
                "https://animerco.invalid"
            }
            async fn search(&self, _q: &str) -> Result<Vec<AnimeResult>> {
                Ok(vec![AnimeResult {
                    slug: "dbs-beerus".into(),
                    title: "Dragon Ball Super: Beerus".into(),
                    alt_title: None,
                    url: "https://animerco.invalid/animes/dbs-beerus".into(),
                    provider: ProviderId::Animerco,
                    year: None,
                    episode_count: None,
                    latency_ms: 5,
                    sources: Vec::new(),
                }])
            }
            async fn get_episodes(&self, _a: &AnimeResult) -> Result<Vec<Episode>> {
                Ok(vec![Episode {
                    number: 6,
                    title: Some("Ep 6".into()),
                    url: "https://animerco.invalid/episodes/6".into(),
                    provider: ProviderId::Animerco,
                    sources: Vec::new(),
                }])
            }
            async fn get_streams(&self, _ep: &Episode) -> Result<Vec<StreamSource>> {
                Ok(vec![StreamSource {
                    url: self.url.clone(),
                    resolution: Resolution::Fhd1080,
                    provider: ProviderId::Animerco,
                    server_name: "vidcache FHD".into(),
                    referer: Some("https://eva.animerco.org/".into()),
                    content_length: None,
                    verified: true,
                    throughput_kbps: Some(2500),
                    supports_ranges: false,
                }])
            }
        }

        // Case 1: Primary fails with end-file error -> cross-provider failover to Animerco completes cleanly!
        std::env::set_var("MOCK_LOG_FILE", &log_path);
        let client = build_http_client(Duration::from_secs(3)).unwrap();
        let engine = Arc::new(ProviderEngine::with_providers(vec![
            Arc::new(FailPrimaryProvider),
            Arc::new(HealthyBackupProvider {
                url: backup_url.clone(),
            }),
        ]));
        let resolver = Arc::new(StreamResolver::new(client.clone()));
        let history = HistoryStore::with_path(dir.path().join("hist.json"));
        let player = MpvPlayer::with_socket(&sock_path, false)
            .with_mpv_bin(mock_mpv.to_string_lossy().into_owned())
            .with_turbo_proxy(false);

        let anime = AnimeResult {
            slug: "dbs-beerus".into(),
            title: "Dragon Ball Super: Beerus".into(),
            alt_title: None,
            url: "https://okanime.invalid/anime/dbs-beerus".into(),
            provider: ProviderId::OkAnime,
            year: None,
            episode_count: None,
            latency_ms: 10,
            sources: Vec::new(),
        };
        let episodes = vec![Episode {
            number: 6,
            title: Some("Ep 6".into()),
            url: "https://okanime.invalid/episode/6".into(),
            provider: ProviderId::OkAnime,
            sources: Vec::new(),
        }];
        let initial_streams = vec![StreamSource {
            url: "https://a4.mp4upload.com:282/d/always-fail.mp4".into(),
            resolution: Resolution::Fhd1080,
            provider: ProviderId::OkAnime,
            server_name: "mp4upload FHD".into(),
            referer: Some("https://www.mp4upload.com/".into()),
            content_length: Some(100_000_000),
            verified: true,
            throughput_kbps: Some(300),
            supports_ranges: true,
        }];
        let mut session_q = None;

        let stop = player
            .play_loop(
                &anime,
                &episodes,
                0,
                initial_streams.clone(),
                None,
                &mut session_q,
                client.clone(),
                engine,
                resolver.clone(),
                &history,
            )
            .await
            .expect("cross-provider failover should succeed instead of crashing");
        assert_eq!(stop, PlaybackStop::CompletedSeries);

        // Case 2: All mirrors & providers fail -> must return UserStopped instead of Err.
        let sock_path_2 = dir.path().join("play-loop-exhausted.sock");
        let empty_engine = Arc::new(ProviderEngine::with_providers(vec![Arc::new(
            FailPrimaryProvider,
        )]));
        let player2 = MpvPlayer::with_socket(&sock_path_2, false)
            .with_mpv_bin(mock_mpv.to_string_lossy().into_owned())
            .with_turbo_proxy(false);

        let stop2 = player2
            .play_loop(
                &anime,
                &episodes,
                0,
                initial_streams,
                None,
                &mut session_q,
                client,
                empty_engine,
                resolver,
                &history,
            )
            .await
            .expect("exhausted mirrors must return cleanly to post-playback menu");
        assert_eq!(stop2, PlaybackStop::UserStopped { episode_index: 0 });
    }

    #[test]
    fn chapter_aware_progress_bar_marks_op_ed_cells_and_buffer_thresholds() {
        let intervals = vec![
            SkipInterval {
                skip_type: crate::metadata::SkipType::Op,
                start_time: 100.0, // ~10%..20% of 1000s -> cells 2..4
                end_time: 200.0,
                episode_length: 1000.0,
            },
            SkipInterval {
                skip_type: crate::metadata::SkipType::Ed,
                start_time: 850.0, // ~85%..95% of 1000s -> cells 20..22
                end_time: 950.0,
                episode_length: 1000.0,
            },
        ];

        // At 50% (cells 0..12 played), OP cells are PlayedOp and ED cells are UnplayedEd.
        let cells = classify_progress_cells(50.0, Some(1000.0), CINEMA_BAR_CELLS, &intervals);
        assert_eq!(cells.len(), CINEMA_BAR_CELLS);
        assert!(cells.contains(&BarCellKind::PlayedOp));
        assert!(cells.contains(&BarCellKind::UnplayedEd));
        assert!(cells.contains(&BarCellKind::PlayedNormal));
        assert!(cells.contains(&BarCellKind::UnplayedNormal));

        let rendered =
            render_chapter_progress_bar(50.0, Some(1000.0), CINEMA_BAR_CELLS, &intervals);
        assert!(rendered.contains("\x1b[35;1m█\x1b[0m")); // Played OP
        assert!(rendered.contains("\x1b[33m▒\x1b[0m")); // Unplayed ED
        assert!(rendered.contains("\x1b[32m█\x1b[0m")); // Played normal

        // Buffer gauge color thresholds: green >= 30s, yellow 5..29s, red < 5s
        let green = format_colored_buffer_gauge(Some(45.0), false, Some(4), 12_000_000);
        assert!(green.starts_with("\x1b[32m"));
        assert!(green.contains("Buf: 45s/120s"));
        assert!(green.contains("⚡4x"));

        let yellow = format_colored_buffer_gauge(Some(12.0), false, None, 0);
        assert!(yellow.starts_with("\x1b[33m"));
        assert!(yellow.contains("Buf: 12s/120s"));

        let red = format_colored_buffer_gauge(Some(2.0), false, None, 0);
        assert!(red.starts_with("\x1b[31m"));
        assert!(red.contains("Buf: 2s/120s"));

        let buffering = format_colored_buffer_gauge(Some(0.0), true, Some(4), 0);
        assert!(buffering.starts_with("\x1b[33m"));
        assert!(buffering.contains("buffering..."));
    }

    #[test]
    fn next_canon_episode_index_skips_filler_and_recap_episodes() {
        let episodes: Vec<Episode> = [130, 131, 132, 133]
            .into_iter()
            .map(|n| Episode {
                number: n,
                title: None,
                url: format!("https://example.invalid/ep/{n}"),
                provider: ProviderId::OkAnime,
                sources: Vec::new(),
            })
            .collect();

        let mut guides = HashMap::new();
        guides.insert(
            131,
            EpisodeMeta {
                number: 131,
                title: Some("Filler 1".into()),
                filler: true,
                recap: false,
            },
        );
        guides.insert(
            132,
            EpisodeMeta {
                number: 132,
                title: Some("Recap 1".into()),
                filler: false,
                recap: true,
            },
        );
        guides.insert(
            133,
            EpisodeMeta {
                number: 133,
                title: Some("Canon Return".into()),
                filler: false,
                recap: false,
            },
        );

        // With skip_filler = false -> lands on Ep 131 (index 1).
        assert_eq!(
            next_canon_episode_index(&episodes, 0, &guides, false),
            Some(1)
        );
        // With skip_filler = true -> skips consecutive 131 (filler) + 132 (recap) -> Ep 133 (index 3)!
        assert_eq!(
            next_canon_episode_index(&episodes, 0, &guides, true),
            Some(3)
        );
        // End of series returns None.
        assert_eq!(next_canon_episode_index(&episodes, 3, &guides, true), None);
    }
}
