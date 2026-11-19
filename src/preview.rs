use crate::cli::is_kitty_terminal;
use crate::metadata::{
    cache_root_dir, cache_slug, clean_metadata_query, fetch_preview_metadata, AnimeMetadata,
    PREVIEW_DEBOUNCE,
};
use owo_colors::OwoColorize;
use reqwest::Client;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

/// Kitty Graphics Protocol escape sequence to delete all visible/off-screen placements.
pub const KITTY_GRAPHICS_CLEAR_SEQ: &str = "\x1b_Ga=d,d=A\x1b\\";

/// Extracts a clean anime search query from an `fzf` formatted row or raw title.
pub fn extract_preview_query(raw: &str) -> String {
    clean_metadata_query(raw)
}

/// Extracts provider badge (`OkAnime`, `Animerco`, etc.) from an `fzf` formatted row.
pub fn extract_provider_badge(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    let after_pipe = trimmed
        .split_once('|')
        .map(|(_, r)| r.trim())
        .unwrap_or(trimmed);
    if let Some(rest) = after_pipe.strip_prefix('[') {
        if let Some(close) = rest.find(']') {
            let badge = rest[..close].trim();
            if !badge.is_empty() {
                return Some(badge.to_string());
            }
        }
    }
    None
}

/// Renders a clean minimal card when both AniList and Jikan are rate-limited or offline.
pub fn format_minimal_fallback_card(query: &str, provider: Option<&str>) -> String {
    let mut out = format!("{}\n", query.bright_cyan().bold());
    if let Some(prov) = provider {
        out.push_str(&format!("Provider: [{}]\n", prov.bright_green()));
    }
    out
}

/// Checks whether the `kitten` CLI is installed (part of Kitty terminal).
pub fn has_kitten() -> bool {
    Command::new("kitten")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Formats episode count cleanly (`"24"`, `"Ep 14+"` from `nextAiringEpisode`, `"Ongoing"`, or `"TBA"`).
pub fn format_episode_display(meta: &AnimeMetadata) -> String {
    if let Some(ep_count) = meta.episodes {
        return ep_count.to_string();
    }
    if let Some(next_ep) = meta.next_airing_episode.filter(|&n| n > 1) {
        return format!("Ep {}+", next_ep - 1);
    }
    if meta.status.as_deref().is_some_and(|s| {
        s.eq_ignore_ascii_case("RELEASING") || s.to_ascii_lowercase().contains("airing")
    }) {
        return "Ongoing".to_string();
    }
    "TBA".to_string()
}

/// Validates image header magic bytes for JPEG (`FF D8 FF`), PNG (`89 50 4E 47`),
/// or WebP (`RIFF....WEBP`) so corrupt HTML/error bodies are never cached or passed to `kitten`.
pub fn is_valid_image_bytes(bytes: &[u8]) -> bool {
    if bytes.len() >= 3 && bytes[0..3] == [0xFF, 0xD8, 0xFF] {
        return true;
    }
    if bytes.len() >= 4 && bytes[0..4] == [0x89, 0x50, 0x4E, 0x47] {
        return true;
    }
    if bytes.len() >= 12 && &bytes[0..4] == b"RIFF" && &bytes[8..12] == b"WEBP" {
        return true;
    }
    false
}

/// Formats structured, wrapped metadata output for the `fzf` preview pane.
pub fn format_metadata_card(meta: &AnimeMetadata, width: usize) -> String {
    let wrap_width = width.clamp(28, 72);
    let mut out = String::new();

    let display_title = meta
        .title_english
        .as_deref()
        .filter(|e| !e.eq_ignore_ascii_case(&meta.title_romaji))
        .map(|e| format!("{} ({})", meta.title_romaji, e))
        .unwrap_or_else(|| meta.title_romaji.clone());

    out.push_str(&format!("{}\n", display_title.bright_cyan().bold()));
    out.push_str(&format!("{}\n", "─".repeat(wrap_width.min(48)).dimmed()));

    let score = meta
        .average_score
        .map(|s| format!("{s}%"))
        .unwrap_or_else(|| "N/A".to_string());
    let year = meta
        .season_year
        .map(|y| y.to_string())
        .unwrap_or_else(|| "TBA".to_string());
    let eps = format_episode_display(meta);
    let status = meta.status.as_deref().unwrap_or("UNKNOWN");

    out.push_str(&format!(
        "★ Score: {}   Year: {}   Eps: {}\n",
        score.bright_green().bold(),
        year,
        eps
    ));
    out.push_str(&format!("Status: {status}\n"));
    if !meta.genres.is_empty() {
        out.push_str(&format!(
            "Genres: {}\n",
            meta.genres.join(", ").bright_yellow()
        ));
    }
    if !meta.description.is_empty() {
        out.push('\n');
        out.push_str(&wrap_text(&meta.description, wrap_width));
        out.push('\n');
    }

    out
}

fn wrap_text(text: &str, max_width: usize) -> String {
    let mut lines = Vec::new();
    let mut current = String::new();
    for word in text.split_whitespace() {
        if current.is_empty() {
            current.push_str(word);
        } else if current.chars().count() + 1 + word.chars().count() <= max_width {
            current.push(' ');
            current.push_str(word);
        } else {
            lines.push(current);
            current = word.to_string();
        }
        if lines.len() >= 10 {
            break;
        }
    }
    if !current.is_empty() && lines.len() < 11 {
        lines.push(current);
    }
    lines.join("\n")
}

/// Resolves the deterministic local cover JPEG/PNG cache path for `meta`.
pub fn cached_cover_path(meta: &AnimeMetadata, cache_root: &Path) -> Option<PathBuf> {
    let id = meta.id_mal.unwrap_or(0);
    let file_name = if id > 0 {
        format!("{id}.jpg")
    } else {
        format!("{}.jpg", cache_slug(&meta.title_romaji))
    };
    Some(cache_root.join("covers").join(file_name))
}

/// Returns an existing valid cached cover path immediately (0ms), without network I/O.
pub fn get_existing_valid_cover(meta: &AnimeMetadata, cache_root: &Path) -> Option<PathBuf> {
    let cover_path = cached_cover_path(meta, cache_root)?;
    if !cover_path.exists() {
        return None;
    }
    if let Ok(existing) = fs::read(&cover_path) {
        if is_valid_image_bytes(&existing) {
            return Some(cover_path);
        }
    }
    let _ = fs::remove_file(&cover_path);
    None
}

pub async fn ensure_cached_cover(
    client: &Client,
    meta: &AnimeMetadata,
    cache_root: &Path,
) -> Option<PathBuf> {
    if let Some(existing) = get_existing_valid_cover(meta, cache_root) {
        return Some(existing);
    }
    let cover_url = meta.cover_image_url.as_deref()?;
    let cover_path = cached_cover_path(meta, cache_root)?;

    let resp = client
        .get(cover_url)
        .timeout(Duration::from_millis(2500))
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let bytes = resp.bytes().await.ok()?;
    if !is_valid_image_bytes(&bytes) {
        return None;
    }
    crate::metadata::atomic_write_bytes(&cover_path, &bytes).ok()?;
    Some(cover_path)
}

/// Returns true when Kitty graphics protocol is active (`TERM=xterm-kitty` or `KITTY_WINDOW_ID`).
pub fn is_kitty_capable() -> bool {
    is_kitty_terminal()
        || std::env::var("KITTY_WINDOW_ID")
            .map(|v| !v.trim().is_empty())
            .unwrap_or(false)
}

/// Clears all active Kitty graphics placements from the terminal screen so no ghost
/// `fzf` preview images linger after selection or cancellation.
pub fn clear_kitty_graphics() {
    if !is_kitty_capable() {
        return;
    }
    let stderr = io::stderr();
    let mut lock = stderr.lock();
    let _ = lock.write_all(KITTY_GRAPHICS_CLEAR_SEQ.as_bytes());
    let _ = lock.flush();
}

/// Computes responsive `(img_cols, img_rows)` dimensions from `FZF_PREVIEW_COLUMNS`
/// and `FZF_PREVIEW_LINES` (defaulting cleanly to `40x24`), leaving space below for metadata.
pub fn compute_preview_dimensions(
    preview_cols: Option<u16>,
    preview_lines: Option<u16>,
) -> (u16, u16) {
    let cols = preview_cols.unwrap_or(40);
    let lines = preview_lines.unwrap_or(24);
    let img_cols = cols.saturating_sub(2).clamp(16, 36);
    let img_rows = (lines / 2).clamp(8, 18);
    (img_cols, img_rows)
}

/// Builds `kitten icat` arguments using `--unicode-placeholder` so placements stay
/// confined inside `fzf`'s preview PTY text buffer instead of floating over the terminal.
pub fn build_icat_args(cover_path: &Path, img_cols: u16, img_rows: u16) -> Vec<String> {
    vec![
        "icat".to_string(),
        "--transfer-mode=memory".to_string(),
        "--unicode-placeholder".to_string(),
        "--stdin=no".to_string(),
        "--align=left".to_string(),
        "--place".to_string(),
        format!("{img_cols}x{img_rows}@0x0"),
        cover_path.to_string_lossy().into_owned(),
    ]
}

/// Captures `kitten icat --unicode-placeholder` bytes cleanly without touching `/dev/tty`.
pub fn capture_icat_unicode_placeholder(
    cover_path: &Path,
    img_cols: u16,
    img_rows: u16,
) -> Option<Vec<u8>> {
    if !is_kitty_capable() || !has_kitten() || !cover_path.exists() {
        return None;
    }
    let args = build_icat_args(cover_path, img_cols, img_rows);
    let output = Command::new("kitten")
        .args(&args)
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .ok()?;
    if output.status.success() && !output.stdout.is_empty() {
        Some(output.stdout)
    } else {
        None
    }
}

/// Renders a poster image via Kitty Graphics Protocol (`kitten icat --unicode-placeholder`)
/// capturing stdout cleanly rather than inheriting `/dev/tty`. Returns `true` if written cleanly.
pub fn render_cover_kitty(cover_path: &Path) -> bool {
    let preview_cols = std::env::var("FZF_PREVIEW_COLUMNS")
        .ok()
        .and_then(|v| v.parse::<u16>().ok());
    let preview_lines = std::env::var("FZF_PREVIEW_LINES")
        .ok()
        .and_then(|v| v.parse::<u16>().ok());
    let (img_cols, img_rows) = compute_preview_dimensions(preview_cols, preview_lines);
    let Some(icat_bytes) = capture_icat_unicode_placeholder(cover_path, img_cols, img_rows) else {
        return false;
    };

    let stdout = io::stdout();
    let mut out = stdout.lock();
    if out.write_all(&icat_bytes).is_err() {
        return false;
    }
    if out.write_all(b"\x1b[m\n").is_err() {
        return false;
    }
    out.flush().is_ok()
}

/// Entrypoint for hidden `--preview <QUERY>` invoked by `fzf` (or manually).
/// Uses zero-429 120ms debounce on cache misses and ignores broken pipes silently.
pub async fn run_preview(raw_arg: &str, client: &Client) {
    let query = extract_preview_query(raw_arg);
    if query.is_empty() {
        return;
    }
    let provider = extract_provider_badge(raw_arg);

    let cols = std::env::var("FZF_PREVIEW_COLUMNS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(40);

    let Some(meta) = fetch_preview_metadata(client, raw_arg, PREVIEW_DEBOUNCE).await else {
        let fallback = format_minimal_fallback_card(&query, provider.as_deref());
        let mut out = io::stdout().lock();
        let _ = out.write_all(fallback.as_bytes());
        let _ = out.flush();
        return;
    };

    if is_kitty_capable() && has_kitten() {
        if let Some(cover_path) = ensure_cached_cover(client, &meta, &cache_root_dir()).await {
            let _ = render_cover_kitty(&cover_path);
        }
    }

    let card = format_metadata_card(&meta, cols);
    let mut out = io::stdout().lock();
    let _ = out.write_all(card.as_bytes());
    let _ = out.flush();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_clean_query_from_fzf_rows_and_formats_card() {
        assert_eq!(
            extract_preview_query("  1 | [OkAnime] Dragon Ball Super: Beerus (2026) (142ms)"),
            "Dragon Ball Super: Beerus"
        );
        assert_eq!(
            extract_preview_query(
                "  2 | [Animerco] Steins;Gate — Ep 06 @ 03:12 (2026-01-01 12:00 UTC)"
            ),
            "Steins;Gate"
        );
        assert_eq!(extract_preview_query("One Piece"), "One Piece");

        let meta = AnimeMetadata {
            id_mal: Some(16498),
            title_romaji: "Shingeki no Kyojin".into(),
            title_english: Some("Attack on Titan".into()),
            average_score: Some(88),
            episodes: Some(25),
            next_airing_episode: None,
            status: Some("FINISHED".into()),
            season_year: Some(2013),
            genres: vec!["Action".into(), "Drama".into()],
            description: "Humanity fights against giant humanoid Titans.".into(),
            cover_image_url: None,
            cached_at: 0,
        };
        let card = format_metadata_card(&meta, 50);
        assert!(card.contains("Shingeki no Kyojin (Attack on Titan)"));
        assert!(card.contains("88%"));
        assert!(card.contains("2013"));
        assert!(card.contains("25"));
        assert!(card.contains("Action, Drama"));
        assert!(card.contains("Humanity fights"));

        let same_case = AnimeMetadata {
            title_romaji: "ONE PIECE".into(),
            title_english: Some("One Piece".into()),
            ..meta.clone()
        };
        let same_card = format_metadata_card(&same_case, 50);
        assert!(same_card.contains("ONE PIECE"));
        assert!(!same_card.contains("ONE PIECE (One Piece)"));

        // Airing with nextAiringEpisode = 15 renders "Ep 14+" instead of "?"
        let airing = AnimeMetadata {
            episodes: None,
            next_airing_episode: Some(15),
            status: Some("RELEASING".into()),
            ..meta.clone()
        };
        let airing_card = format_metadata_card(&airing, 50);
        assert!(airing_card.contains("Ep 14+"));
        assert!(!airing_card.contains("Eps: ?"));

        let ongoing = AnimeMetadata {
            episodes: None,
            next_airing_episode: None,
            status: Some("RELEASING".into()),
            ..meta
        };
        assert_eq!(format_episode_display(&ongoing), "Ongoing");

        // Cover image magic-byte validation
        assert!(is_valid_image_bytes(&[0xFF, 0xD8, 0xFF, 0xE0]));
        assert!(is_valid_image_bytes(&[0x89, 0x50, 0x4E, 0x47, 0x0D]));
        assert!(is_valid_image_bytes(b"RIFF\x00\x00\x00\x00WEBP"));
        assert!(!is_valid_image_bytes(b"<html>403 Forbidden</html>"));
    }

    #[test]
    fn builds_unicode_placeholder_icat_args_and_responsive_dimensions() {
        assert_eq!(KITTY_GRAPHICS_CLEAR_SEQ, "\x1b_Ga=d,d=A\x1b\\");

        // Fallback defaults (40x24 -> 36x12)
        assert_eq!(compute_preview_dimensions(None, None), (36, 12));
        // Small preview pane clamps safely at lower bounds (16x8)
        assert_eq!(compute_preview_dimensions(Some(12), Some(10)), (16, 8));
        // Large preview pane clamps safely at upper bounds (36x18)
        assert_eq!(compute_preview_dimensions(Some(120), Some(60)), (36, 18));

        let path = Path::new("/tmp/covers/9253.jpg");
        let args = build_icat_args(path, 34, 12);
        assert_eq!(
            args,
            vec![
                "icat",
                "--transfer-mode=memory",
                "--unicode-placeholder",
                "--stdin=no",
                "--align=left",
                "--place",
                "34x12@0x0",
                "/tmp/covers/9253.jpg",
            ]
        );
        assert!(!args.contains(&"--clear".to_string()));
    }

    #[test]
    fn extracts_provider_badge_and_formats_minimal_fallback_card() {
        assert_eq!(
            extract_provider_badge("  1 | [OkAnime] Frieren (2023) (95ms)"),
            Some("OkAnime".to_string())
        );
        assert_eq!(
            extract_provider_badge("  2 | [MRC] One Piece — Ep 1000"),
            Some("MRC".to_string())
        );
        assert_eq!(extract_provider_badge("Steins;Gate"), None);

        let card = format_minimal_fallback_card("Frieren", Some("OkAnime"));
        assert!(card.contains("Frieren"));
        assert!(card.contains("OkAnime"));
        assert!(!card.contains("429"));
        assert!(!card.contains("error"));
    }
}
