use crate::models::{ProviderId, Resolution};
use clap::{ArgAction, CommandFactory, Parser, ValueEnum};
use clap_complete::{generate, Shell};
use owo_colors::OwoColorize;
use std::io::Write;
use std::path::PathBuf;

/// Clean Rick Sanchez / Senku scientific ASCII art header.
pub const RICK_SENKU_ASCII: &str = r#"
        .---.        .---.
       /     \  __  /     \     senku-anime-cli (sac) v1.8.0
      / /     \(  )/     \ \    ───────────────────────────────────────────────
     //////   ' \/ `   \\\\\\   "Listen Morty—*burp*—I turned an anime scraper
    //// / // :    : \\ \ \\\\   into a zero-bloat Rust portal gun. 10 billion
   // /   /  /`    '\  \   \ \\  percent pure Arabic subs. Zero telemetry."
  //          //..\\          \\
         ====UU====UU====
             '//||\\`
               ''``
"#;

pub const VERSION_BANNER: &str = concat!(
    "1.8.0\n",
    "        .---.        .---.\n",
    "       /     \\  __  /     \\     senku-anime-cli (sac) 1.8.0\n",
    "      / /     \\(  )/     \\ \\    Rust + Tokio + mpv JSON-IPC + Chunked HTTP\n",
    "     //////   ' \\/ `   \\\\\\\\\\\\   License: GPL-3.0-only | Zero telemetry.\n",
    "    //// / // :    : \\\\ \\ \\\\\\\\  \"Wubba lubba dub-dub. Get excited.\"\n",
    "   // /   /  /`    '\\  \\   \\ \\\\\n",
    "  //          //..\\\\          \\\\\n",
    "         ====UU====UU====\n",
    "             '//||\\\\`\n",
    "               ''``",
);

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ProviderChoice {
    All,
    Animerco,
    Okanime,
    Anime3rb,
    Animeslayer,
    Animeify,
}

impl ProviderChoice {
    pub fn to_filter(self) -> Option<ProviderId> {
        match self {
            ProviderChoice::All => None,
            ProviderChoice::Animerco => Some(ProviderId::Animerco),
            ProviderChoice::Okanime => Some(ProviderId::OkAnime),
            ProviderChoice::Anime3rb => Some(ProviderId::Anime3rb),
            ProviderChoice::Animeslayer => Some(ProviderId::AnimeSlayer),
            ProviderChoice::Animeify => Some(ProviderId::Animeify),
        }
    }
}

/// Lightning-fast, zero-bloat Arabic-subbed anime CLI streamer & downloader.
#[derive(Debug, Parser)]
#[command(
    name = "senku-anime-cli",
    bin_name = "senku-anime-cli",
    version = VERSION_BANNER,
    about = "Portal-grade Arabic-subbed anime streamer & multi-threaded downloader.",
    before_help = RICK_SENKU_ASCII,
    after_help = "EXAMPLES:\n  \
        senku-anime-cli \"Steins;Gate\"             Search, race Arabic providers & stream highest quality\n  \
        sac -c                                    Instantly resume your last watched series & episode\n  \
        sac \"Attack on Titan\" -e 1-12 -d          Batch chunk-download episodes 1..12 with live MB/s & ETA\n  \
        sac \"One Piece\" -q 720p --kitty           Force 720p & pass Kitty video output flags to mpv\n\n\
        PHILOSOPHY:\n  \
        Zero telemetry. Zero bloat. Pure Rust + GPL-3.0.",
    arg_required_else_help = false
)]
pub struct Cli {
    /// Anime title or search query (optional when using -c / --history)
    #[arg(value_name = "QUERY")]
    pub query: Option<String>,

    /// Episode number or range (e.g. 5, 1-12, 1,3,5-8)
    #[arg(short = 'e', long = "episode", value_name = "RANGE")]
    pub episode: Option<String>,

    /// Preferred resolution (skips interactive quality prompt, e.g. 1080p, 720p, 480p)
    #[arg(short = 'q', long = "quality", value_name = "RES")]
    pub quality: Option<Resolution>,

    /// Automatically pick the highest available resolution without prompting
    #[arg(short = 'b', long = "best", action = ArgAction::SetTrue)]
    pub best: bool,

    /// Download selected episode(s) instead of streaming in mpv
    #[arg(short = 'd', long = "download", action = ArgAction::SetTrue)]
    pub download: bool,

    /// Resolve and print the verified stream URL to stdout without launching mpv
    #[arg(short = 'u', long = "url", action = ArgAction::SetTrue)]
    pub url_only: bool,

    /// Output directory for downloaded episodes (defaults to config or '.')
    #[arg(short = 'o', long = "output", value_name = "DIR")]
    pub output: Option<PathBuf>,

    /// Continue watching the most recent anime from ~/.local/share/senku-anime-cli/history.json
    #[arg(short = 'c', long = "continue", action = ArgAction::SetTrue)]
    pub continue_watch: bool,

    /// Target a specific Arabic provider or race all providers concurrently
    #[arg(short = 'p', long = "provider", value_enum, default_value_t = ProviderChoice::All)]
    pub provider: ProviderChoice,

    /// Pass Kitty graphics/video output flags (--vo=kitty) to mpv (auto-detected when TERM=xterm-kitty if requested)
    #[arg(long = "kitty", env = "SENKU_KITTY", action = ArgAction::SetTrue)]
    pub kitty: bool,

    /// Disable automatic next-episode playback
    #[arg(long = "no-autoplay", action = ArgAction::SetTrue)]
    pub no_autoplay: bool,

    /// Seconds to jump forward on [i] Skip Intro hotkey (default: 85)
    #[arg(long = "skip-intro", value_name = "SECS")]
    pub skip_intro_secs: Option<u64>,

    /// Disable fzf and use the built-in Rust terminal selector
    #[arg(long = "no-fzf", action = ArgAction::SetTrue)]
    pub no_fzf: bool,

    /// Number of concurrent HTTP Range threads per download (default: 8)
    #[arg(short = 't', long = "threads", value_name = "N")]
    pub threads: Option<usize>,

    /// Disable the local multi-threaded HTTP Range turbo stream proxy
    #[arg(long = "no-proxy", action = ArgAction::SetTrue)]
    pub no_proxy: bool,

    /// Number of concurrent HTTP Range workers in the local turbo stream proxy (default: 3)
    #[arg(long = "proxy-threads", value_name = "N")]
    pub proxy_threads: Option<usize>,

    /// Provider search race deadline in milliseconds (default: 4000)
    #[arg(long = "timeout-ms", value_name = "MS")]
    pub timeout_ms: Option<u64>,

    /// Show watch history and exit
    #[arg(long = "history", action = ArgAction::SetTrue)]
    pub show_history: bool,

    /// Clear local watch history and exit
    #[arg(long = "clear-history", action = ArgAction::SetTrue)]
    pub clear_history: bool,

    /// Suppress the Rick/Senku ASCII banner
    #[arg(long = "no-banner", action = ArgAction::SetTrue)]
    pub no_banner: bool,

    /// Print per-provider warnings and debug diagnostics during races
    #[arg(short = 'v', long = "verbose", action = ArgAction::SetTrue)]
    pub verbose: bool,

    /// Automatically skip detected OP and ED intervals via AniSkip chapters
    #[arg(long = "auto-skip", action = ArgAction::SetTrue)]
    pub auto_skip: bool,

    /// Automatically skip Jikan-flagged filler and recap episodes during autoplay
    #[arg(long = "skip-filler", action = ArgAction::SetTrue)]
    pub skip_filler: bool,

    /// Disable interactive AniList cover-art / metadata previews inside fzf
    #[arg(long = "no-preview", action = ArgAction::SetTrue)]
    pub no_preview: bool,

    /// Hidden internal fzf preview renderer for AniList metadata + Kitty cover art
    #[arg(long = "preview", value_name = "QUERY", hide = true)]
    pub preview: Option<String>,

    /// Use compact 2-line playback bar instead of the Now-Playing Cinema Dashboard
    #[arg(long = "minimal", action = ArgAction::SetTrue)]
    pub minimal: bool,

    /// Check recent watch history for newly aired/released episodes and exit
    #[arg(long = "updates", action = ArgAction::SetTrue)]
    pub updates: bool,

    /// Run system, cache, config, and provider diagnostics and exit
    #[arg(long = "doctor", action = ArgAction::SetTrue)]
    pub doctor: bool,

    /// Print shell completion scripts for senku-anime-cli and sac to stdout
    #[arg(long = "completions", value_enum, value_name = "SHELL")]
    pub completions: Option<Shell>,

    /// Include Cloudflare-gated providers (anime3rb, animeslayer, animeify) in -p all races
    #[arg(long = "include-blocked", action = ArgAction::SetTrue)]
    pub include_blocked: bool,
}

impl Cli {
    /// Returns true when invoked with no query and no direct action flags
    /// (triggers the zero-arg interactive launcher menu).
    pub fn is_zero_arg_launcher(&self) -> bool {
        self.query.as_deref().is_none_or(|q| q.trim().is_empty())
            && !self.continue_watch
            && !self.show_history
            && !self.clear_history
            && !self.download
            && !self.url_only
            && !self.updates
            && !self.doctor
            && self.completions.is_none()
            && self.preview.is_none()
    }
}

/// Stdout writer wrapper that swallows `ErrorKind::BrokenPipe` (`EPIPE`) silently.
struct BrokenPipeIgnoringWriter<W>(W);

impl<W: Write> Write for BrokenPipeIgnoringWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self.0.write(buf) {
            Ok(n) => Ok(n),
            Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => Ok(buf.len()),
            Err(e) => Err(e),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self.0.flush() {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => Ok(()),
            Err(e) => Err(e),
        }
    }
}

/// Writes a line to `out`, silently ignoring `BrokenPipe` (`EPIPE`).
pub fn writeln_ignore_broken_pipe<W: Write>(out: &mut W, text: &str) -> std::io::Result<()> {
    match writeln!(out, "{text}").and_then(|()| out.flush()) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => Ok(()),
        Err(e) => Err(e),
    }
}

/// Generates shell completion scripts for both `senku-anime-cli` and `sac`.
pub fn write_completions<W: Write>(shell: Shell, out: &mut W) {
    let mut safe_out = BrokenPipeIgnoringWriter(out);
    let mut main_cmd = Cli::command();
    generate(shell, &mut main_cmd, "senku-anime-cli", &mut safe_out);
    let _ = writeln!(safe_out);
    let mut alias_cmd = Cli::command();
    generate(shell, &mut alias_cmd, "sac", &mut safe_out);
    let _ = safe_out.flush();
}

/// Returns completion scripts for both `senku-anime-cli` and `sac` as a `String`.
pub fn render_completions_to_string(shell: Shell) -> String {
    let mut buf = Vec::new();
    write_completions(shell, &mut buf);
    String::from_utf8_lossy(&buf).into_owned()
}

/// Returns true when running inside Kitty terminal (`$TERM == "xterm-kitty"`).
pub fn is_kitty_terminal() -> bool {
    std::env::var("TERM")
        .map(|v| v.trim() == "xterm-kitty")
        .unwrap_or(false)
}

/// Prints the high-tech scientific header to stderr so stdout stays pipe-clean.
pub fn print_banner(suppress: bool) {
    if suppress {
        return;
    }
    let kitty_badge = if is_kitty_terminal() {
        "[TERM: xterm-kitty detected]".bright_cyan().to_string()
    } else {
        "[TERM: standard]".dimmed().to_string()
    };

    eprintln!("{}", RICK_SENKU_ASCII.bright_green());
    eprintln!(
        "  {} {}",
        ":: Portal Engine Online — 10,000,000,000% Scientific Precision ::"
            .bright_cyan()
            .bold(),
        kitty_badge
    );
    eprintln!();
}

pub fn log_info(msg: &str) {
    eprintln!("{} {}", "[SCI]".bright_green().bold(), msg);
}

pub fn log_warn(msg: &str) {
    eprintln!("{} {}", "[WARN]".yellow().bold(), msg.yellow());
}

pub fn log_err(msg: &str) {
    eprintln!("{} {}", "[ERR]".red().bold(), msg.red());
}
