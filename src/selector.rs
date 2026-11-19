use crate::error::{Result, SenkuError};
use crate::history::{format_history_row_label, NewEpisodeAlert};
use crate::metadata::EpisodeMeta;
use crate::models::{AnimeResult, Episode, Resolution, StreamSource, WatchHistoryEntry};
use crate::preview::clear_kitty_graphics;
use crate::proxy::TurboStreamProxy;
use owo_colors::OwoColorize;
use std::collections::HashMap;
use std::io::{self, BufRead, Write};
use std::path::Path;
use std::process::{Command, Stdio};

/// Top-level zero-arg interactive launcher menu choices.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LauncherAction {
    SearchAnime,
    ContinueWatching,
    DownloadEpisodes,
    WatchHistory,
    Quit,
}

impl LauncherAction {
    pub const MENU_LABELS: [&'static str; 5] = [
        "Search Anime",
        "Continue Watching",
        "Download Episode(s)",
        "Watch History",
        "Quit",
    ];

    pub fn from_index(idx: usize) -> Option<Self> {
        match idx {
            0 => Some(Self::SearchAnime),
            1 => Some(Self::ContinueWatching),
            2 => Some(Self::DownloadEpisodes),
            3 => Some(Self::WatchHistory),
            4 => Some(Self::Quit),
            _ => None,
        }
    }
}

/// Post-playback `ani-cli`-style interactive actions after manual `mpv` quit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostPlaybackAction {
    NextEpisode,
    PreviousEpisode,
    ReplayCurrent,
    SelectEpisode,
    DownloadCurrent,
    SearchAnother,
    Quit,
}

impl PostPlaybackAction {
    pub fn parse_input(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "n" | "next" | "1" => Some(Self::NextEpisode),
            "p" | "prev" | "previous" | "2" => Some(Self::PreviousEpisode),
            "r" | "replay" | "3" => Some(Self::ReplayCurrent),
            "s" | "select" | "4" => Some(Self::SelectEpisode),
            "d" | "download" | "5" => Some(Self::DownloadCurrent),
            "a" | "another" | "search" | "6" => Some(Self::SearchAnother),
            "q" | "quit" | "exit" | "7" => Some(Self::Quit),
            _ => None,
        }
    }
}

/// Checks whether `fzf` is available in `$PATH`.
pub fn has_fzf() -> bool {
    Command::new("fzf")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Generic interactive menu selector using `fzf` when available, or TTY fallback.
pub fn select_menu_item(items: &[&str], prompt: &str, no_fzf: bool) -> Result<usize> {
    if items.is_empty() {
        return Err(SenkuError::SelectionCancelled);
    }
    let lines: Vec<String> = items
        .iter()
        .enumerate()
        .map(|(i, label)| format!("{:>3} | {label}", i + 1))
        .collect();

    if !no_fzf && has_fzf() {
        pick_with_fzf(&lines, prompt)
    } else {
        pick_with_tty(&lines, prompt.trim_end_matches(" > "))
    }
}

/// Interactive zero-arg launcher menu.
pub fn select_launcher_action(no_fzf: bool) -> Result<LauncherAction> {
    let idx = select_menu_item(&LauncherAction::MENU_LABELS, "Portal > ", no_fzf)?;
    LauncherAction::from_index(idx).ok_or(SenkuError::SelectionCancelled)
}

/// Interactive history entry picker (shows last episode & timestamp).
pub fn select_history_entry(entries: &[WatchHistoryEntry], no_fzf: bool) -> Result<usize> {
    select_history_entry_with_alerts(entries, &HashMap::new(), no_fzf, false)
}

/// Interactive history entry picker with optional AniList cover/metadata `fzf` preview.
pub fn select_history_entry_with_preview(
    entries: &[WatchHistoryEntry],
    no_fzf: bool,
    enable_preview: bool,
) -> Result<usize> {
    select_history_entry_with_alerts(entries, &HashMap::new(), no_fzf, enable_preview)
}

/// Interactive history entry picker highlighting any newly released episodes.
pub fn select_history_entry_with_alerts(
    entries: &[WatchHistoryEntry],
    alerts: &HashMap<String, NewEpisodeAlert>,
    no_fzf: bool,
    enable_preview: bool,
) -> Result<usize> {
    if entries.is_empty() {
        return Err(SenkuError::SelectionCancelled);
    }
    let lines: Vec<String> = entries
        .iter()
        .enumerate()
        .map(|(i, item)| format_history_row_label(i, item, alerts.get(&item.anime_slug)))
        .collect();

    if !no_fzf && has_fzf() {
        pick_with_fzf_opts(&lines, "Resume > ", enable_preview)
    } else {
        pick_with_tty(&lines, "Select anime from history")
    }
}

/// Fast interactive prompt after the user manually quits `mpv`.
pub fn prompt_post_playback(
    has_next: bool,
    has_prev: bool,
    no_fzf: bool,
) -> Result<PostPlaybackAction> {
    if !no_fzf && has_fzf() {
        let mut labels: Vec<(&str, PostPlaybackAction)> = Vec::new();
        if has_next {
            labels.push(("[n] Next episode", PostPlaybackAction::NextEpisode));
        }
        if has_prev {
            labels.push(("[p] Previous episode", PostPlaybackAction::PreviousEpisode));
        }
        labels.push((
            "[r] Replay current episode",
            PostPlaybackAction::ReplayCurrent,
        ));
        labels.push((
            "[s] Select another episode",
            PostPlaybackAction::SelectEpisode,
        ));
        labels.push((
            "[d] Download current episode",
            PostPlaybackAction::DownloadCurrent,
        ));
        labels.push((
            "[a] Search another anime",
            PostPlaybackAction::SearchAnother,
        ));
        labels.push(("[q] Quit", PostPlaybackAction::Quit));

        let str_labels: Vec<&str> = labels.iter().map(|(l, _)| *l).collect();
        return match select_menu_item(&str_labels, "Action > ", false) {
            Ok(idx) => Ok(labels[idx].1),
            Err(SenkuError::SelectionCancelled) => Ok(PostPlaybackAction::Quit),
            Err(e) => Err(e),
        };
    }

    let stderr = io::stderr();
    let mut err_lock = stderr.lock();
    writeln!(err_lock)?;
    if has_next {
        writeln!(err_lock, "  {} Next episode", "[n]".bright_green().bold())?;
    }
    if has_prev {
        writeln!(
            err_lock,
            "  {} Previous episode",
            "[p]".bright_cyan().bold()
        )?;
    }
    writeln!(
        err_lock,
        "  {} Replay current episode",
        "[r]".bright_cyan().bold()
    )?;
    writeln!(
        err_lock,
        "  {} Select another episode",
        "[s]".bright_cyan().bold()
    )?;
    writeln!(
        err_lock,
        "  {} Download current episode",
        "[d]".bright_cyan().bold()
    )?;
    writeln!(
        err_lock,
        "  {} Search another anime",
        "[a]".bright_cyan().bold()
    )?;
    writeln!(err_lock, "  {} Quit", "[q]".red().bold())?;

    loop {
        write!(err_lock, "\n{} ", "Control >".bright_cyan().bold())?;
        err_lock.flush()?;

        let mut input = String::new();
        let bytes = io::stdin().lock().read_line(&mut input)?;
        if bytes == 0 {
            return Ok(PostPlaybackAction::Quit);
        }
        if let Some(action) = PostPlaybackAction::parse_input(&input) {
            if action == PostPlaybackAction::NextEpisode && !has_next {
                writeln!(err_lock, "{}", "No next episode available.".yellow())?;
                continue;
            }
            if action == PostPlaybackAction::PreviousEpisode && !has_prev {
                writeln!(err_lock, "{}", "Already at the first episode.".yellow())?;
                continue;
            }
            return Ok(action);
        }
        writeln!(
            err_lock,
            "{}",
            "Invalid option. Enter n, p, r, s, d, a, or q.".yellow()
        )?;
    }
}

/// Result of the interactive quality/server picker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QualityPick {
    AutoBest,
    Index(usize),
}

/// Returns true when the interactive quality prompt should be bypassed
/// (explicit `-q <RES>`, `--best` / `-b`, or `auto_select_best = true` in config.toml).
pub fn should_skip_quality_prompt(
    cli_quality: Option<Resolution>,
    cli_best: bool,
    cfg_auto_best: bool,
) -> bool {
    cli_quality.is_some() || cli_best || cfg_auto_best
}

/// Formats speed & turbo metadata for a stream line in the quality selector.
pub fn format_stream_metrics(stream: &StreamSource, turbo_workers: Option<usize>) -> String {
    let mut parts = Vec::new();
    if let Some(kbps) = stream.throughput_kbps {
        parts.push(format!("{:.1} MB/s", kbps as f64 / 1024.0));
    } else if let Some(bytes) = stream.content_length.filter(|&b| b > 0) {
        parts.push(format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0)));
    }
    if let Some(workers) = turbo_workers.filter(|&w| w > 1 && TurboStreamProxy::is_eligible(stream))
    {
        parts.push(format!("Turbo {workers}x"));
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!("  ({})", parts.join(" · "))
    }
}

/// Formats aligned unified pool lines for the quality/server picker:
/// `[1080p]  <server>  [MRC/OKA]  <speed>  <HLS|⚡Nx|MP4>`
pub fn format_quality_picker_lines(
    streams: &[StreamSource],
    turbo_workers: Option<usize>,
) -> Vec<String> {
    if streams.is_empty() {
        return Vec::new();
    }

    let kind_for = |s: &StreamSource| -> String {
        if s.url.to_ascii_lowercase().contains(".m3u8") {
            "HLS".to_string()
        } else if let Some(w) = turbo_workers.filter(|&w| w > 1 && TurboStreamProxy::is_eligible(s))
        {
            format!("⚡{w}x")
        } else {
            "MP4".to_string()
        }
    };
    let speed_for = |s: &StreamSource| -> String {
        if !s.verified {
            "[Unverified]".to_string()
        } else if let Some(kbps) = s.throughput_kbps {
            if kbps >= 1024 {
                format!("{:.1} MB/s", kbps as f64 / 1024.0)
            } else {
                format!("{kbps} KB/s")
            }
        } else if let Some(bytes) = s.content_length.filter(|&b| b > 0) {
            format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
        } else {
            "—".to_string()
        }
    };

    let res_w = streams
        .iter()
        .map(|s| format!("[{}]", s.resolution).len())
        .max()
        .unwrap_or(7)
        .max(7);
    let srv_w = streams
        .iter()
        .map(|s| s.server_name.chars().count())
        .max()
        .unwrap_or(10)
        .clamp(8, 24);
    let spd_w = streams
        .iter()
        .map(|s| speed_for(s).len())
        .max()
        .unwrap_or(6)
        .max(4);

    let best = &streams[0];
    let unverified_note = if best.verified { "" } else { " [Unverified]" };
    let mut lines = Vec::with_capacity(streams.len() + 1);
    lines.push(format!(
        "  1 | ★ Auto (Best Available — {} {} [{}] {}{}{})",
        best.resolution,
        best.server_name,
        best.provider.short_tag(),
        kind_for(best),
        unverified_note,
        format_stream_metrics(best, turbo_workers)
    ));

    for (i, stream) in streams.iter().enumerate() {
        let res_col = format!("[{}]", stream.resolution);
        let srv_col: String = stream.server_name.chars().take(srv_w).collect();
        let prov_col = format!("[{}]", stream.provider.short_tag());
        let spd_col = speed_for(stream);
        let kind_col = kind_for(stream);
        lines.push(format!(
            "{:>3} | {:<res_w$}  {:<srv_w$}  {:<5}  {:<spd_w$}  {}",
            i + 2,
            res_col,
            srv_col,
            prov_col,
            spd_col,
            kind_col
        ));
    }

    lines
}

/// Interactive quality & server picker listing `★ Auto (Best Available)` followed
/// by every verified stream sorted highest-first across all pooled providers.
/// Never silently skips when only 1 stream is present — call-site `should_skip_quality_prompt`
/// controls whether prompting is bypassed (`-q`, `-b`, or `auto_select_best = true`).
pub fn select_stream_quality(
    streams: &[StreamSource],
    no_fzf: bool,
    turbo_workers: Option<usize>,
) -> Result<QualityPick> {
    if streams.is_empty() {
        return Err(SenkuError::SelectionCancelled);
    }

    let lines = format_quality_picker_lines(streams, turbo_workers);

    let picked = if !no_fzf && has_fzf() {
        pick_with_fzf(&lines, "Quality > ")?
    } else {
        pick_with_tty(&lines, "Select quality/server")?
    };

    if picked == 0 {
        Ok(QualityPick::AutoBest)
    } else {
        Ok(QualityPick::Index(picked - 1))
    }
}

/// Promotes the user-selected stream to index `0` while keeping backup mirrors
/// behind it for failover, returning the chosen resolution for sticky sessions.
pub fn apply_quality_pick(
    streams: &mut Vec<StreamSource>,
    pick: QualityPick,
) -> Option<Resolution> {
    match pick {
        QualityPick::AutoBest => None,
        QualityPick::Index(idx) if idx < streams.len() => {
            let chosen = streams.remove(idx);
            let res = chosen.resolution;
            streams.insert(0, chosen);
            Some(res)
        }
        QualityPick::Index(_) => None,
    }
}

/// Interactive anime picker using `fzf` when available, or built-in terminal selector.
pub fn select_anime(results: &[AnimeResult], no_fzf: bool) -> Result<usize> {
    select_anime_with_preview(results, no_fzf, false)
}

/// Interactive anime picker with optional AniList cover/metadata `fzf` preview.
pub fn select_anime_with_preview(
    results: &[AnimeResult],
    no_fzf: bool,
    enable_preview: bool,
) -> Result<usize> {
    if results.is_empty() {
        return Err(SenkuError::SelectionCancelled);
    }
    if results.len() == 1 {
        return Ok(0);
    }

    let lines: Vec<String> = results
        .iter()
        .enumerate()
        .map(|(i, item)| {
            let year = item
                .year
                .as_deref()
                .map(|y| format!(" ({y})"))
                .unwrap_or_default();
            format!(
                "{:>3} | [{}] {}{} ({}ms)",
                i + 1,
                item.provider_badge(),
                item.title,
                year,
                item.latency_ms
            )
        })
        .collect();

    if !no_fzf && has_fzf() {
        pick_with_fzf_opts(&lines, "Anime > ", enable_preview)
    } else {
        pick_with_tty(&lines, "Select anime number")
    }
}

/// Context powering rich Smart Episode Navigator rows and resume cursor placement.
#[derive(Debug, Clone, Default)]
pub struct EpisodePickerContext<'a> {
    pub watched_episodes: &'a [u32],
    pub in_progress_episode: Option<u32>,
    pub in_progress_secs: Option<u64>,
    pub guides: Option<&'a HashMap<u32, EpisodeMeta>>,
}

/// Computes the 0-based episode index to pre-highlight (`last_episode` if `position_secs > 10`,
/// else `last_episode + 1` or `max(watched) + 1`, falling back to `0`).
pub fn logical_resume_episode_index(episodes: &[Episode], ctx: &EpisodePickerContext<'_>) -> usize {
    if episodes.is_empty() {
        return 0;
    }
    let target_num = if let Some(last_ep) = ctx.in_progress_episode {
        if ctx.in_progress_secs.is_some_and(|s| s > 10) {
            last_ep
        } else {
            last_ep + 1
        }
    } else if let Some(&max_w) = ctx.watched_episodes.iter().max() {
        max_w + 1
    } else {
        return 0;
    };

    episodes
        .iter()
        .position(|e| e.number == target_num)
        .or_else(|| {
            ctx.in_progress_episode
                .and_then(|last| episodes.iter().position(|e| e.number == last))
        })
        .unwrap_or(0)
}

/// Formats a rich Smart Episode Navigator row:
/// `129 | ✓ Ep 129  Formidable Enemy and Formidable Ally`
/// `130 | ▶ Ep 130  Magic to Destroy  [04:12]`
/// `132 |   Ep 132  Flash and Start  [FILLER]`
pub fn format_episode_row_label(
    display_idx: usize,
    ep: &Episode,
    ctx: &EpisodePickerContext<'_>,
) -> String {
    let is_in_progress =
        ctx.in_progress_episode == Some(ep.number) && ctx.in_progress_secs.is_some_and(|s| s > 10);
    let is_watched = !is_in_progress && ctx.watched_episodes.contains(&ep.number);

    let icon = if is_in_progress {
        "▶"
    } else if is_watched {
        "✓"
    } else {
        " "
    };

    let guide = ctx.guides.and_then(|g| g.get(&ep.number));
    let ep_title = guide
        .and_then(|g| g.title.as_deref())
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .or_else(|| ep.title.as_deref().map(str::trim).filter(|t| !t.is_empty()));

    let title_part = ep_title.map(|t| format!("  {t}")).unwrap_or_default();

    let mut badges = Vec::new();
    if is_in_progress {
        if let Some(secs) = ctx.in_progress_secs {
            badges.push(format!("[{:02}:{:02}]", secs / 60, secs % 60));
        }
    }
    if guide.is_some_and(|g| g.filler) {
        badges.push("\x1b[2;33m[FILLER]\x1b[0m".to_string());
    }
    if guide.is_some_and(|g| g.recap) {
        badges.push("\x1b[2;33m[RECAP]\x1b[0m".to_string());
    }
    let badge_part = if badges.is_empty() {
        String::new()
    } else {
        format!("  {}", badges.join(" "))
    };

    format!(
        "{:>3} | {icon} Ep {:02}{title_part}{badge_part}",
        display_idx + 1,
        ep.number
    )
}

/// Interactive single-episode picker when `-e` wasn't passed on the CLI.
pub fn select_episode(episodes: &[Episode], no_fzf: bool) -> Result<usize> {
    select_episode_with_context(episodes, no_fzf, &EpisodePickerContext::default())
}

/// Interactive Smart Episode Navigator picker showing watch-state icons, resume offsets,
/// Jikan titles, `[FILLER]`/`[RECAP]` badges, and auto-positioning the cursor on the next episode.
pub fn select_episode_with_context(
    episodes: &[Episode],
    no_fzf: bool,
    ctx: &EpisodePickerContext<'_>,
) -> Result<usize> {
    if episodes.is_empty() {
        return Err(SenkuError::SelectionCancelled);
    }
    if episodes.len() == 1 {
        return Ok(0);
    }

    let lines: Vec<String> = episodes
        .iter()
        .enumerate()
        .map(|(i, ep)| format_episode_row_label(i, ep, ctx))
        .collect();
    let default_idx = logical_resume_episode_index(episodes, ctx);

    if !no_fzf && has_fzf() {
        pick_with_fzf_opts_at(&lines, "Episode > ", false, Some(default_idx + 1))
    } else {
        pick_with_tty_default(&lines, "Select episode number", Some(default_idx))
    }
}

fn pick_with_fzf(lines: &[String], prompt: &str) -> Result<usize> {
    pick_with_fzf_opts_at(lines, prompt, false, None)
}

fn pick_with_fzf_opts(lines: &[String], prompt: &str, enable_preview: bool) -> Result<usize> {
    pick_with_fzf_opts_at(lines, prompt, enable_preview, None)
}

fn pick_with_fzf_opts_at(
    lines: &[String],
    prompt: &str,
    enable_preview: bool,
    initial_pos: Option<usize>,
) -> Result<usize> {
    struct KittyClearGuard(bool);
    impl Drop for KittyClearGuard {
        fn drop(&mut self) {
            if self.0 {
                clear_kitty_graphics();
            }
        }
    }
    let _clear_guard = KittyClearGuard(enable_preview);

    let mut cmd = Command::new("fzf");
    cmd.args([
        if enable_preview {
            "--height=70%"
        } else {
            "--height=45%"
        },
        "--layout=reverse",
        "--border=rounded",
        "--ansi",
        "--no-multi",
        "--prompt",
        prompt,
        "--header",
        "senku-anime-cli :: Wubba Lubba Pick (ESC to abort)",
    ]);

    if let Some(pos) = initial_pos.filter(|&p| p > 1 && p <= lines.len()) {
        cmd.args(["--bind", &format!("load:pos({pos})")]);
    }

    if enable_preview {
        if let Ok(exe) = std::env::current_exe() {
            let preview_cmd = build_fzf_preview_cmd(&exe);
            cmd.args([
                "--preview",
                &preview_cmd,
                "--preview-window",
                "right:45%:wrap",
            ]);
        }
    }

    let mut child = cmd.stdin(Stdio::piped()).stdout(Stdio::piped()).spawn()?;

    if let Some(mut stdin) = child.stdin.take() {
        for line in lines {
            if let Err(e) = writeln!(stdin, "{line}") {
                if e.kind() == io::ErrorKind::BrokenPipe {
                    break;
                }
                return Err(e.into());
            }
        }
    }

    let output = child.wait_with_output()?;
    if !output.status.success() {
        return Err(SenkuError::SelectionCancelled);
    }

    let selected = String::from_utf8_lossy(&output.stdout);
    let idx_str = selected
        .trim()
        .split('|')
        .next()
        .map(str::trim)
        .ok_or(SenkuError::SelectionCancelled)?;

    let one_based: usize = idx_str
        .parse()
        .map_err(|_| SenkuError::SelectionCancelled)?;
    one_based
        .checked_sub(1)
        .filter(|&i| i < lines.len())
        .ok_or(SenkuError::SelectionCancelled)
}

/// Builds the `fzf --preview` command string safely single-quoting the binary path
/// while leaving `{}` unquoted so `fzf` performs its own shell-safe escaping for titles
/// containing quotes (`'`, `"`), `$`, or backticks.
pub fn build_fzf_preview_cmd(exe: &Path) -> String {
    let escaped_exe = exe.to_string_lossy().replace('\'', "'\"'\"'");
    format!("'{escaped_exe}' --preview {{}}")
}

fn pick_with_tty(lines: &[String], prompt: &str) -> Result<usize> {
    pick_with_tty_default(lines, prompt, None)
}

fn pick_with_tty_default(
    lines: &[String],
    prompt: &str,
    default_idx: Option<usize>,
) -> Result<usize> {
    let stderr = io::stderr();
    let mut err_lock = stderr.lock();
    for line in lines {
        writeln!(err_lock, "  {}", line.bright_white())?;
    }
    if let Some(def) = default_idx.filter(|&i| i < lines.len()) {
        write!(
            err_lock,
            "\n{} [1-{}, default {}]: ",
            prompt.bright_cyan().bold(),
            lines.len(),
            def + 1
        )?;
    } else {
        write!(
            err_lock,
            "\n{} [1-{}]: ",
            prompt.bright_cyan().bold(),
            lines.len()
        )?;
    }
    err_lock.flush()?;

    let stdin = io::stdin();
    let mut input = String::new();
    stdin.lock().read_line(&mut input)?;
    let trimmed = input.trim();
    if trimmed.is_empty() {
        if let Some(def) = default_idx.filter(|&i| i < lines.len()) {
            return Ok(def);
        }
        return Err(SenkuError::SelectionCancelled);
    }
    if trimmed.eq_ignore_ascii_case("q") {
        return Err(SenkuError::SelectionCancelled);
    }

    let one_based: usize = trimmed
        .parse()
        .map_err(|_| SenkuError::SelectionCancelled)?;
    one_based
        .checked_sub(1)
        .filter(|&i| i < lines.len())
        .ok_or(SenkuError::SelectionCancelled)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::ProviderId;

    #[test]
    fn maps_zero_arg_launcher_indices_correctly() {
        assert_eq!(
            LauncherAction::from_index(0),
            Some(LauncherAction::SearchAnime)
        );
        assert_eq!(
            LauncherAction::from_index(1),
            Some(LauncherAction::ContinueWatching)
        );
        assert_eq!(
            LauncherAction::from_index(2),
            Some(LauncherAction::DownloadEpisodes)
        );
        assert_eq!(
            LauncherAction::from_index(3),
            Some(LauncherAction::WatchHistory)
        );
        assert_eq!(LauncherAction::from_index(4), Some(LauncherAction::Quit));
        assert_eq!(LauncherAction::from_index(5), None);
    }

    #[test]
    fn parses_post_playback_shortcuts_case_insensitively() {
        assert_eq!(
            PostPlaybackAction::parse_input("N"),
            Some(PostPlaybackAction::NextEpisode)
        );
        assert_eq!(
            PostPlaybackAction::parse_input("p"),
            Some(PostPlaybackAction::PreviousEpisode)
        );
        assert_eq!(
            PostPlaybackAction::parse_input("r"),
            Some(PostPlaybackAction::ReplayCurrent)
        );
        assert_eq!(
            PostPlaybackAction::parse_input("s"),
            Some(PostPlaybackAction::SelectEpisode)
        );
        assert_eq!(
            PostPlaybackAction::parse_input("d"),
            Some(PostPlaybackAction::DownloadCurrent)
        );
        assert_eq!(
            PostPlaybackAction::parse_input("a"),
            Some(PostPlaybackAction::SearchAnother)
        );
        assert_eq!(
            PostPlaybackAction::parse_input("q"),
            Some(PostPlaybackAction::Quit)
        );
        assert_eq!(PostPlaybackAction::parse_input("invalid"), None);
    }

    #[test]
    fn skips_quality_prompt_when_q_or_best_or_config_auto_best_is_set() {
        assert!(!should_skip_quality_prompt(None, false, false));
        assert!(should_skip_quality_prompt(
            Some(Resolution::Hd720),
            false,
            false
        ));
        assert!(should_skip_quality_prompt(None, true, false));
        assert!(should_skip_quality_prompt(None, false, true));
    }

    #[test]
    fn formats_aligned_unified_quality_picker_even_for_single_stream() {
        let single = vec![StreamSource {
            url: "https://cdn.vidcache.net/ep6-1080p.m3u8".into(),
            resolution: Resolution::Fhd1080,
            provider: ProviderId::Animerco,
            server_name: "vidcache FHD".into(),
            referer: None,
            content_length: None,
            verified: true,
            throughput_kbps: Some(3480),
            supports_ranges: false,
        }];
        let single_lines = format_quality_picker_lines(&single, Some(4));
        assert_eq!(single_lines.len(), 2);
        assert!(single_lines[0].contains("★ Auto"));
        assert!(single_lines[1].contains("[1080p]"));
        assert!(single_lines[1].contains("[MRC]"));
        assert!(single_lines[1].contains("HLS"));

        let multi = vec![
            single[0].clone(),
            StreamSource {
                url: "https://cdn.yourupload.com/ep6-720p.mp4".into(),
                resolution: Resolution::Hd720,
                provider: ProviderId::OkAnime,
                server_name: "yourupload HD".into(),
                referer: None,
                content_length: Some(150 * 1024 * 1024),
                verified: true,
                throughput_kbps: Some(820),
                supports_ranges: true,
            },
            StreamSource {
                url: "https://s1.mp4upload.com:282/d/ep6-480p.mp4".into(),
                resolution: Resolution::Sd480,
                provider: ProviderId::OkAnime,
                server_name: "mp4upload SD".into(),
                referer: None,
                content_length: Some(80 * 1024 * 1024),
                verified: true,
                throughput_kbps: Some(400),
                supports_ranges: true,
            },
        ];
        let lines = format_quality_picker_lines(&multi, Some(4));
        assert_eq!(lines.len(), 4);
        assert!(lines[1].contains("[MRC]") && lines[1].contains("HLS"));
        assert!(lines[2].contains("[OKA]") && lines[2].contains("⚡4x"));
        assert!(lines[3].contains("[OKA]") && lines[3].contains("MP4"));
        // Columns align `[MRC]` and `[OKA]` at exact same column offset across rows.
        assert_eq!(lines[1].find("[MRC]"), lines[2].find("[OKA]"));
        assert_eq!(lines[2].find("[OKA]"), lines[3].find("[OKA]"));

        // Unverified stream carries [Unverified] tag in both Auto-Best and row lines.
        let unverified = vec![StreamSource {
            verified: false,
            throughput_kbps: None,
            ..multi[0].clone()
        }];
        let u_lines = format_quality_picker_lines(&unverified, None);
        assert!(u_lines[0].contains("[Unverified]"));
        assert!(u_lines[1].contains("[Unverified]"));
    }

    #[test]
    fn formats_rich_episode_rows_and_computes_logical_resume_cursor() {
        let episodes = vec![
            Episode {
                number: 129,
                title: None,
                url: "https://example.invalid/ep/129".into(),
                provider: ProviderId::OkAnime,
                sources: Vec::new(),
            },
            Episode {
                number: 130,
                title: None,
                url: "https://example.invalid/ep/130".into(),
                provider: ProviderId::OkAnime,
                sources: Vec::new(),
            },
            Episode {
                number: 132,
                title: None,
                url: "https://example.invalid/ep/132".into(),
                provider: ProviderId::OkAnime,
                sources: Vec::new(),
            },
        ];
        let mut guides = HashMap::new();
        guides.insert(
            129,
            EpisodeMeta {
                number: 129,
                title: Some("Formidable Enemy and Formidable Ally".into()),
                filler: false,
                recap: false,
            },
        );
        guides.insert(
            130,
            EpisodeMeta {
                number: 130,
                title: Some("Magic to Destroy".into()),
                filler: false,
                recap: false,
            },
        );
        guides.insert(
            132,
            EpisodeMeta {
                number: 132,
                title: Some("Flash and Start".into()),
                filler: true,
                recap: false,
            },
        );

        let watched = [129];
        let ctx_in_progress = EpisodePickerContext {
            watched_episodes: &watched,
            in_progress_episode: Some(130),
            in_progress_secs: Some(252),
            guides: Some(&guides),
        };
        assert_eq!(logical_resume_episode_index(&episodes, &ctx_in_progress), 1);

        let r129 = format_episode_row_label(0, &episodes[0], &ctx_in_progress);
        assert!(r129.contains("✓ Ep 129  Formidable Enemy and Formidable Ally"));

        let r130 = format_episode_row_label(1, &episodes[1], &ctx_in_progress);
        assert!(r130.contains("▶ Ep 130  Magic to Destroy  [04:12]"));

        let r132 = format_episode_row_label(2, &episodes[2], &ctx_in_progress);
        assert!(r132.contains("  Ep 132  Flash and Start"));
        assert!(r132.contains("[FILLER]"));

        // Recap badge check
        let ep_recap = Episode {
            number: 133,
            title: None,
            url: "https://example.invalid/ep/133".into(),
            provider: ProviderId::OkAnime,
            sources: Vec::new(),
        };
        guides.insert(
            133,
            EpisodeMeta {
                number: 133,
                title: Some("Briefing".into()),
                filler: false,
                recap: true,
            },
        );
        let ctx_recap = EpisodePickerContext {
            watched_episodes: &watched,
            in_progress_episode: Some(130),
            in_progress_secs: Some(252),
            guides: Some(&guides),
        };
        let r133 = format_episode_row_label(3, &ep_recap, &ctx_recap);
        assert!(r133.contains("  Ep 133  Briefing"));
        assert!(r133.contains("[RECAP]"));

        // When last episode finished (position <= 10), cursor advances to 130 (129 + 1).
        let ctx_finished = EpisodePickerContext {
            watched_episodes: &watched,
            in_progress_episode: Some(129),
            in_progress_secs: Some(4),
            guides: Some(&guides),
        };
        assert_eq!(logical_resume_episode_index(&episodes, &ctx_finished), 1);
    }

    #[test]
    fn builds_shell_safe_fzf_preview_cmd_without_double_quoting_placeholder() {
        let cmd = build_fzf_preview_cmd(Path::new("/opt/my bin/senku's/sac"));
        assert_eq!(cmd, "'/opt/my bin/senku'\"'\"'s/sac' --preview {}");
        assert!(!cmd.contains("\"{}\""));
    }
}
