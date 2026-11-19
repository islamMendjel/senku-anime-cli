use crate::cli::Cli;
use crate::models::Resolution;
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

pub const DEFAULT_THREADS: usize = 8;
pub const DEFAULT_PROXY_THREADS: usize = 3;
pub const DEFAULT_TIMEOUT_MS: u64 = 4000;
pub const DEFAULT_SKIP_INTRO_SECS: u64 = 85;

/// Persistent user configuration loaded from
/// `$XDG_CONFIG_HOME/senku-anime-cli/config.toml` (`~/.config/senku-anime-cli/config.toml`).
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct AppConfig {
    pub default_quality: Option<String>,
    pub auto_select_best: Option<bool>,
    pub autoplay: Option<bool>,
    pub kitty: Option<bool>,
    pub download_dir: Option<PathBuf>,
    pub threads: Option<usize>,
    pub turbo_proxy: Option<bool>,
    pub proxy_threads: Option<usize>,
    pub timeout_ms: Option<u64>,
    pub skip_intro_secs: Option<u64>,
    pub auto_skip_op: Option<bool>,
    pub auto_skip_ed: Option<bool>,
    pub preview: Option<bool>,
    pub cinema_dashboard: Option<bool>,
    pub skip_filler: Option<bool>,
}

/// Merged runtime settings where explicit CLI flags always override `config.toml`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectiveSettings {
    pub quality: Option<Resolution>,
    pub auto_select_best: bool,
    pub autoplay: bool,
    pub kitty: bool,
    pub download_dir: PathBuf,
    pub threads: usize,
    pub turbo_proxy: bool,
    pub proxy_threads: usize,
    pub timeout_ms: u64,
    pub skip_intro_secs: u64,
    pub auto_skip_op: bool,
    pub auto_skip_ed: bool,
    pub preview: bool,
    pub cinema_dashboard: bool,
    pub skip_filler: bool,
}

impl AppConfig {
    pub fn default_path() -> Option<PathBuf> {
        dirs::config_dir()
            .or_else(|| dirs::home_dir().map(|h| h.join(".config")))
            .map(|dir| dir.join("senku-anime-cli").join("config.toml"))
    }

    /// Loads config from `$XDG_CONFIG_HOME/senku-anime-cli/config.toml`.
    /// Missing config files fall back silently to defaults.
    pub fn load_default() -> Self {
        match Self::default_path() {
            Some(path) => Self::load_from_path(&path),
            None => Self::default(),
        }
    }

    pub fn load_from_path(path: &Path) -> Self {
        if !path.exists() {
            return Self::default();
        }
        let Ok(raw) = fs::read_to_string(path) else {
            return Self::default();
        };
        if raw.trim().is_empty() {
            return Self::default();
        }
        match toml::from_str::<AppConfig>(&raw) {
            Ok(cfg) => cfg,
            Err(err) => {
                eprintln!(
                    "[WARN] Ignoring invalid config at {} ({err}); using defaults",
                    path.display()
                );
                Self::default()
            }
        }
    }

    pub fn resolve_settings(&self, cli: &Cli) -> EffectiveSettings {
        let cfg_quality = self
            .default_quality
            .as_deref()
            .and_then(|s| Resolution::from_str(s).ok());

        let download_dir = cli
            .output
            .clone()
            .or_else(|| self.download_dir.clone())
            .map(expand_tilde)
            .unwrap_or_else(|| PathBuf::from("."));

        EffectiveSettings {
            quality: cli.quality.or(cfg_quality),
            auto_select_best: cli.best
                || cli.quality.is_some()
                || self.auto_select_best.unwrap_or(false),
            autoplay: if cli.no_autoplay {
                false
            } else {
                self.autoplay.unwrap_or(true)
            },
            kitty: cli.kitty || self.kitty.unwrap_or(false),
            download_dir,
            threads: cli
                .threads
                .or(self.threads)
                .unwrap_or(DEFAULT_THREADS)
                .clamp(1, 32),
            turbo_proxy: !cli.no_proxy && self.turbo_proxy.unwrap_or(true),
            proxy_threads: cli
                .proxy_threads
                .or(self.proxy_threads)
                .unwrap_or(DEFAULT_PROXY_THREADS)
                .clamp(1, 16),
            timeout_ms: cli
                .timeout_ms
                .or(self.timeout_ms)
                .unwrap_or(DEFAULT_TIMEOUT_MS),
            skip_intro_secs: cli
                .skip_intro_secs
                .or(self.skip_intro_secs)
                .unwrap_or(DEFAULT_SKIP_INTRO_SECS),
            auto_skip_op: cli.auto_skip || self.auto_skip_op.unwrap_or(false),
            auto_skip_ed: cli.auto_skip || self.auto_skip_ed.unwrap_or(false),
            preview: !cli.no_preview && self.preview.unwrap_or(true),
            cinema_dashboard: !cli.minimal && self.cinema_dashboard.unwrap_or(true),
            skip_filler: cli.skip_filler || self.skip_filler.unwrap_or(false),
        }
    }
}

fn expand_tilde(path: PathBuf) -> PathBuf {
    let s = path.to_string_lossy();
    if let Some(rest) = s.strip_prefix("~/") {
        if let Some(home) = dirs::home_dir() {
            return home.join(rest);
        }
    } else if s == "~" {
        if let Some(home) = dirs::home_dir() {
            return home;
        }
    }
    path
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use tempfile::tempdir;

    #[test]
    fn missing_config_falls_back_silently_to_defaults() {
        let dir = tempdir().unwrap();
        let missing = dir.path().join("nonexistent-config.toml");
        let cfg = AppConfig::load_from_path(&missing);
        assert_eq!(cfg, AppConfig::default());

        let cli = Cli::parse_from(["senku-anime-cli", "One Piece"]);
        let eff = cfg.resolve_settings(&cli);
        assert_eq!(eff.quality, None);
        assert!(eff.autoplay);
        assert!(!eff.kitty);
        assert_eq!(eff.download_dir, PathBuf::from("."));
        assert_eq!(eff.threads, 8);
        assert_eq!(eff.timeout_ms, 4000);
        assert_eq!(eff.skip_intro_secs, 85);
    }

    #[test]
    fn parses_config_toml_and_lets_cli_flags_override() {
        let dir = tempdir().unwrap();
        let cfg_path = dir.path().join("config.toml");
        let toml_body = r#"
default_quality = "720p"
auto_select_best = true
autoplay = false
kitty = true
download_dir = "/tmp/anime-dl"
threads = 12
timeout_ms = 2500
skip_intro_secs = 90
turbo_proxy = false
proxy_threads = 10
cinema_dashboard = false
skip_filler = true
"#;

        fs::write(&cfg_path, toml_body).unwrap();

        let cfg = AppConfig::load_from_path(&cfg_path);
        assert_eq!(cfg.default_quality.as_deref(), Some("720p"));
        assert_eq!(cfg.auto_select_best, Some(true));
        assert_eq!(cfg.autoplay, Some(false));
        assert_eq!(cfg.kitty, Some(true));
        assert_eq!(cfg.threads, Some(12));
        assert_eq!(cfg.timeout_ms, Some(2500));
        assert_eq!(cfg.skip_intro_secs, Some(90));
        assert_eq!(cfg.turbo_proxy, Some(false));
        assert_eq!(cfg.proxy_threads, Some(10));
        assert_eq!(cfg.cinema_dashboard, Some(false));
        assert_eq!(cfg.skip_filler, Some(true));

        // 1. Without CLI overrides -> config values win.
        let cli_plain = Cli::parse_from(["senku-anime-cli", "One Piece"]);
        let eff_plain = cfg.resolve_settings(&cli_plain);
        assert_eq!(eff_plain.quality, Some(Resolution::Hd720));
        assert!(eff_plain.auto_select_best);
        assert!(!eff_plain.autoplay);
        assert!(eff_plain.kitty);
        assert_eq!(eff_plain.download_dir, PathBuf::from("/tmp/anime-dl"));
        assert_eq!(eff_plain.threads, 12);
        assert_eq!(eff_plain.timeout_ms, 2500);
        assert_eq!(eff_plain.skip_intro_secs, 90);
        assert!(!eff_plain.turbo_proxy);
        assert_eq!(eff_plain.proxy_threads, 10);
        assert!(!eff_plain.cinema_dashboard);
        assert!(eff_plain.skip_filler);

        // 2. With explicit CLI overrides -> CLI flags always win over config.toml.
        let cli_override = Cli::parse_from([
            "senku-anime-cli",
            "One Piece",
            "-q",
            "1080p",
            "-o",
            "/var/tmp/out",
            "-t",
            "4",
            "--timeout-ms",
            "1500",
            "--skip-intro",
            "75",
            "--proxy-threads",
            "8",
        ]);
        let eff_override = cfg.resolve_settings(&cli_override);
        assert_eq!(eff_override.quality, Some(Resolution::Fhd1080));
        assert_eq!(eff_override.download_dir, PathBuf::from("/var/tmp/out"));
        assert_eq!(eff_override.threads, 4);
        assert_eq!(eff_override.timeout_ms, 1500);
        assert_eq!(eff_override.skip_intro_secs, 75);
        assert_eq!(eff_override.proxy_threads, 8);

        let cli_no_proxy =
            Cli::parse_from(["senku-anime-cli", "--no-proxy", "--minimal", "One Piece"]);
        let eff_no_proxy = AppConfig::default().resolve_settings(&cli_no_proxy);
        assert!(!eff_no_proxy.turbo_proxy);
        assert!(!eff_no_proxy.cinema_dashboard);
        assert_eq!(eff_no_proxy.proxy_threads, DEFAULT_PROXY_THREADS);
        assert!(
            AppConfig::default()
                .resolve_settings(&cli_plain)
                .cinema_dashboard
        );
    }
}
