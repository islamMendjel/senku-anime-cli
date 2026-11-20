<div align="center">

# senku-anime-cli

[![Crates.io](https://img.shields.io/badge/crates.io-senku--anime--cli-orange?style=for-the-badge&logo=rust)](https://crates.io/crates/senku-anime-cli)
[![Rust](https://img.shields.io/badge/Rust-2021-black?style=for-the-badge&logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/License-GPL--3.0-red?style=for-the-badge)](LICENSE)
[![Version](https://img.shields.io/badge/version-1.8.0-blue?style=for-the-badge)](Cargo.toml)
[![Arch Linux](https://img.shields.io/badge/Arch%20Linux-PKGBUILD-1793D1?style=for-the-badge&logo=arch-linux)](PKGBUILD)

---

![senku-anime-cli demo](preview.gif)

---


```
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

```

---

**Lightning-fast, zero-bloat Arabic-subbed anime CLI — stream & download directly from your terminal.**

</div>

---

## What is this?

**`senku-anime-cli`** (alias: **`sac`**) is a high-performance, zero-bloat CLI written in Rust to search, stream, and download Arabic-subbed anime directly from the terminal. No Electron, no browser — just `mpv`, async Rust, and raw speed.

It concurrently races multiple providers, auto-selects the highest verified quality, seamlessly autoplays the next episode, and supports multi-threaded chunked downloads — all with zero telemetry.

---

## Features

| Feature | Description |
|---|---|
| **Multi-Provider Racing** | Races *Anime3rb*, *AnimeSlayer*, and *Animeify* concurrently via `tokio::task::JoinSet` — fastest provider wins, stalled mirrors never block you |
| **Smart Resolution Defaulting** | Parses `2160p` → `1080p` → `720p` → `480p` → `360p`, defaults to highest verified quality, validates streams with async `HEAD` requests |
| **Autoplay Next Episode** | Manages `mpv` over `/tmp/senku-anime-cli.sock` (JSON-IPC). Reaching `≥99%` or EOF automatically fetches and loads episode N+1 |
| **Chunked Multi-Thread Downloader** | Single episodes or ranges (`-e 1-12`) via concurrent HTTP `Range` requests — live `indicatif` progress bars with MB/s and ETA, zero `aria2c` dependency |
| **Watch History & Resume** | Persists state in `~/.local/share/senku-anime-cli/history.json` — zero telemetry |
| **Kitty & fzf Ready** | Native `$TERM == "xterm-kitty"` detection, `fzf` fuzzy selection with pure-Rust TTY fallback |
| **Arch Linux PKGBUILD** | `makepkg -si` and you're done |

---

## Install

### From Source (Cargo)

```bash
cargo build --release
cargo install --path .
```

Or copy binaries directly:

```bash
sudo install -Dm755 target/release/senku-anime-cli /usr/local/bin/senku-anime-cli
sudo ln -sf /usr/local/bin/senku-anime-cli /usr/local/bin/sac
```

### Arch Linux

```bash
makepkg -si
```

---

## Usage

```bash
# Search & stream (autoplays next episode)
sac "Steins;Gate"

# Resume last watched
sac -c

# Force 720p with Kitty video output
sac "One Piece" -q 720p --kitty

# Download episodes 1–12 to ~/Videos/Anime
sac "Attack on Titan" -e 1-12 -d -o ~/Videos/Anime
```

---

## Tech Stack

| Crate | Role |
|---|---|
| `tokio` | Async runtime — full features |
| `reqwest` | HTTP client — rustls, gzip, brotli, streaming |
| `scraper` | HTML parsing for provider scraping |
| `clap` | CLI argument parsing with shell completions |
| `indicatif` | Download progress bars |
| `crossterm` | Terminal UI |
| `owo-colors` | Terminal colors |
| `serde` / `serde_json` | History serialization |
| `anyhow` / `thiserror` | Error handling |

---

## Project Structure

```
senku-anime-cli/
├── src/
│   ├── main.rs           # Entry point
│   ├── bin/sac.rs        # sac alias binary
│   └── ...               # Providers, downloader, player, history
├── tests/
├── Cargo.toml
├── PKGBUILD              # Arch Linux package build
└── LICENSE               # GPL-3.0
```

---

## Requirements

- `mpv` — for streaming
- `fzf` *(optional)* — fuzzy selection (falls back to pure-Rust TTY)
- Rust `>=1.75` — for building from source

---

## License

GNU General Public License v3.0 — see [LICENSE](LICENSE).

---

<div align="center">

Built by [islamMendjel](https://github.com/islamMendjel)

</div>
