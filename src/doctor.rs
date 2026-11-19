use crate::cli::is_kitty_terminal;
use crate::config::AppConfig;
use crate::error::Result;
use crate::history::HistoryStore;
use crate::metadata::{cache_root_dir, ANILIST_GRAPHQL_URL, ANISKIP_API_BASE};
use crate::providers::ProviderEngine;
use owo_colors::OwoColorize;
use reqwest::Client;
use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCheck {
    pub name: String,
    pub available: bool,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderPing {
    pub name: String,
    pub ok: bool,
    pub status_or_err: String,
    pub latency_ms: u64,
}

#[derive(Debug, Clone)]
pub struct DoctorReport {
    pub mpv: ToolCheck,
    pub fzf: ToolCheck,
    pub kitty: ToolCheck,
    pub config_status: String,
    pub history_status: String,
    pub cache_status: String,
    pub pings: Vec<ProviderPing>,
}

fn check_tool_version(bin: &str, flag: &str) -> ToolCheck {
    let out = Command::new(bin).arg(flag).stdin(Stdio::null()).output();
    match out {
        Ok(o) if o.status.success() => {
            let txt = String::from_utf8_lossy(&o.stdout);
            let first = txt.lines().next().unwrap_or("installed").trim().to_string();
            ToolCheck {
                name: bin.to_string(),
                available: true,
                detail: first,
            }
        }
        _ => ToolCheck {
            name: bin.to_string(),
            available: false,
            detail: "not found in $PATH".to_string(),
        },
    }
}

pub fn directory_stats(dir: &Path) -> (usize, u64) {
    if !dir.exists() {
        return (0, 0);
    }
    let mut files = 0usize;
    let mut bytes = 0u64;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(cur) = stack.pop() {
        let Ok(entries) = fs::read_dir(cur) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if let Ok(meta) = entry.metadata() {
                files += 1;
                bytes = bytes.saturating_add(meta.len());
            }
        }
    }
    (files, bytes)
}

pub fn format_human_bytes(bytes: u64) -> String {
    if bytes >= 1024 * 1024 {
        format!("{:.2} MiB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.1} KiB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}

/// Formats the complete `--doctor` health table cleanly for terminal display.
pub fn format_doctor_report(report: &DoctorReport) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "{}\n",
        ":: System & Tooling Diagnostics ::".bright_cyan().bold()
    ));

    for tool in [&report.mpv, &report.fzf, &report.kitty] {
        let badge = if tool.available {
            "[OK]".bright_green().bold().to_string()
        } else {
            "[MISS]".yellow().bold().to_string()
        };
        out.push_str(&format!(
            "  {badge:<14} {:<10} {}\n",
            tool.name, tool.detail
        ));
    }

    out.push_str(&format!(
        "\n{}\n",
        ":: Config, History & Cache ::".bright_cyan().bold()
    ));
    out.push_str(&format!("  config.toml : {}\n", report.config_status));
    out.push_str(&format!("  history.json: {}\n", report.history_status));
    out.push_str(&format!("  cache dir   : {}\n", report.cache_status));

    out.push_str(&format!(
        "\n{}\n",
        ":: Provider & API Latency ::".bright_cyan().bold()
    ));
    for ping in &report.pings {
        let badge = if ping.ok {
            "[UP]".bright_green().bold().to_string()
        } else {
            "[DOWN]".red().bold().to_string()
        };
        out.push_str(&format!(
            "  {badge:<14} {:<14} {:>4}ms  ({})\n",
            ping.name, ping.latency_ms, ping.status_or_err
        ));
    }

    out
}

/// Runs diagnostics across local tools, config, history, cache, and live HTTP endpoints.
pub async fn run_doctor(
    client: &Client,
    engine: &ProviderEngine,
    history: &HistoryStore,
) -> Result<()> {
    let mpv = check_tool_version("mpv", "--version");
    let fzf = check_tool_version("fzf", "--version");

    let kitten_check = check_tool_version("kitten", "--version");
    let kitty_term = is_kitty_terminal();
    let kitty = ToolCheck {
        name: "kitty".to_string(),
        available: kitty_term && kitten_check.available,
        detail: match (kitty_term, kitten_check.available) {
            (true, true) => format!("TERM=xterm-kitty + {}", kitten_check.detail),
            (true, false) => "TERM=xterm-kitty (install `kitten` for image previews)".to_string(),
            (false, true) => format!(
                "kitten installed, standard terminal ({})",
                kitten_check.detail
            ),
            (false, false) => "standard terminal (text-only fzf previews)".to_string(),
        },
    };

    let config_status = match AppConfig::default_path() {
        Some(path) if path.exists() => match fs::read_to_string(&path) {
            Ok(raw) => match toml::from_str::<AppConfig>(&raw) {
                Ok(_) => format!("loaded ({})", path.display()),
                Err(err) => format!("invalid TOML at {} ({err})", path.display()),
            },
            Err(err) => format!("unreadable at {} ({err})", path.display()),
        },
        Some(path) => format!("not found ({}; using built-in defaults)", path.display()),
        None => "no XDG config path resolved (using defaults)".to_string(),
    };

    let history_count = history.load_all().map(|v| v.len()).unwrap_or(0);
    let history_status = format!("{history_count} entries ({})", history.path().display());

    let cache_dir = cache_root_dir();
    let (file_count, total_bytes) = directory_stats(&cache_dir);
    let cache_status = format!(
        "{file_count} files, {} ({})",
        format_human_bytes(total_bytes),
        cache_dir.display()
    );

    let mut targets: Vec<(String, String)> = engine
        .active_provider_endpoints()
        .into_iter()
        .map(|(id, url)| (id.display_name().to_string(), url))
        .collect();
    targets.push(("AniList API".to_string(), ANILIST_GRAPHQL_URL.to_string()));
    targets.push((
        "AniSkip API".to_string(),
        format!("{}/9253/1", ANISKIP_API_BASE),
    ));

    let mut set = JoinSet::new();
    for (idx, (name, url)) in targets.into_iter().enumerate() {
        let c = client.clone();
        set.spawn(async move {
            let started = Instant::now();
            let res = c.get(&url).timeout(Duration::from_secs(3)).send().await;
            let latency_ms = started.elapsed().as_millis() as u64;
            let ping = match res {
                Ok(resp) => {
                    let status = resp.status();
                    ProviderPing {
                        name,
                        ok: status.as_u16() < 500,
                        status_or_err: format!("HTTP {status}"),
                        latency_ms,
                    }
                }
                Err(err) => ProviderPing {
                    name,
                    ok: false,
                    status_or_err: err.to_string(),
                    latency_ms,
                },
            };
            (idx, ping)
        });
    }

    let mut indexed = Vec::new();
    while let Some(joined) = set.join_next().await {
        if let Ok(entry) = joined {
            indexed.push(entry);
        }
    }
    indexed.sort_by_key(|(idx, _)| *idx);
    let pings = indexed.into_iter().map(|(_, p)| p).collect();

    let report = DoctorReport {
        mpv,
        fzf,
        kitty,
        config_status,
        history_status,
        cache_status,
        pings,
    };

    print!("{}", format_doctor_report(&report));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn formats_doctor_report_and_measures_cache_directory() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("meta.json"), vec![b'a'; 2048]).unwrap();
        fs::create_dir_all(dir.path().join("covers")).unwrap();
        fs::write(dir.path().join("covers/9253.jpg"), vec![b'b'; 4096]).unwrap();

        let (count, bytes) = directory_stats(dir.path());
        assert_eq!(count, 2);
        assert_eq!(bytes, 6144);
        assert_eq!(format_human_bytes(6144), "6.0 KiB");

        let report = DoctorReport {
            mpv: ToolCheck {
                name: "mpv".into(),
                available: true,
                detail: "mpv v0.41.0".into(),
            },
            fzf: ToolCheck {
                name: "fzf".into(),
                available: true,
                detail: "0.55.0".into(),
            },
            kitty: ToolCheck {
                name: "kitty".into(),
                available: false,
                detail: "standard terminal".into(),
            },
            config_status: "loaded (/home/user/.config/senku-anime-cli/config.toml)".into(),
            history_status: "4 entries".into(),
            cache_status: "2 files, 6.0 KiB".into(),
            pings: vec![
                ProviderPing {
                    name: "OkAnime".into(),
                    ok: true,
                    status_or_err: "HTTP 200 OK".into(),
                    latency_ms: 82,
                },
                ProviderPing {
                    name: "AniSkip API".into(),
                    ok: false,
                    status_or_err: "timed out".into(),
                    latency_ms: 3000,
                },
            ],
        };

        let formatted = format_doctor_report(&report);
        assert!(formatted.contains("mpv v0.41.0"));
        assert!(formatted.contains("OkAnime"));
        assert!(formatted.contains("82ms"));
        assert!(formatted.contains("6.0 KiB"));
        assert!(formatted.contains("4 entries"));
    }
}
