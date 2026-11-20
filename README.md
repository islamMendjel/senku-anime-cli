<div align="center">

# senku-anime-cli

[![Rust CI](https://github.com/islamMendjel/senku-anime-cli/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/islamMendjel/senku-anime-cli/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/islamMendjel/senku-anime-cli?color=00ff9f&logo=github)](https://github.com/islamMendjel/senku-anime-cli/releases)
[![Rust](https://img.shields.io/badge/language-Rust%202021-orange.svg)](https://www.rust-lang.org)
[![License: GPL-3.0](https://img.shields.io/badge/License-GPL--3.0-9775fa.svg)](LICENSE)

![senku-anime-cli Demo](preview.gif)

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

  :: Portal Engine Online — 10,000,000,000% Scientific Precision :: [TERM: xterm-kitty detected]
```

**Lightning-fast, zero-bloat Arabic-subbed anime CLI — stream & download directly from your terminal.**

*Dimension C-137 Portal Gun Engineering*

[![Language](https://img.shields.io/badge/Language-Rust%202021-orange?style=for-the-badge&logo=rust)](https://www.rust-lang.org)
[![Platform](https://img.shields.io/badge/Platform-Arch%20Linux%20%2F%20POSIX-1793D1?style=for-the-badge&logo=arch-linux)](https://archlinux.org)
[![License](https://img.shields.io/badge/License-GPL--3.0-red?style=for-the-badge)](LICENSE)
[![Version](https://img.shields.io/badge/Version-1.8.0-blue?style=for-the-badge)]()
[![Telemetry](https://img.shields.io/badge/Telemetry-Zero-brightgreen?style=for-the-badge)]()

</div>

---

## What is senku-anime-cli?

**`senku-anime-cli`** (alias: **`sac`**) is a high-performance, zero-bloat CLI written in Rust to search, stream, and download Arabic-subbed anime directly from the terminal. No Electron. No browser. No bloat. Just `mpv`, async Rust, and raw speed.

It spawns concurrent Tokio tasks that race three separate providers simultaneously — the fastest one wins, stalled mirrors are silently discarded. Streams are validated before playback, resolution is auto-detected and defaulted to the highest available, and when an episode ends the next one loads automatically without returning to the menu.

> Zero telemetry. Zero aria2c dependency. Zero external process managers — everything is native Rust.

---

## 🎨 The Portal Engine Color System

The TUI uses a Rick Sanchez–themed 24-bit TrueColor palette rendered through `owo-colors` with terminal capability detection. Every UI element maps to a palette role.

| Role | Color | Hex | Usage |
|---|---|---|---|
| **Portal Green** | 🟢 | `#52B788` | Active menu selection · provider success · stream validated |
| **Ice Blue** | 🔵 | `#A2D2FF` | Status line · progress bars · info text |
| **Amber** | 🟡 | `#FFD166` | Warnings · resolution override · kitty mode |
| **Neon Pink-Red** | 🔴 | `#EF476F` | Provider failures · stream errors · abort |
| **Aqua Teal** | 🩵 | `#7AE5C6` | Download progress · episode ranges · ETA |
| **Crisp White** | ⬜ | `#F4F6F8` | Main text · episode titles · anime names |
| **Slate Grey** | ⬛ | `#4A5568` | Muted provider labels · inactive mirrors |
| **Canvas** | ⚫ | `#000000` | Transparent terminal background |

The portal engine boot sequence renders in cyan with the `:: Portal Engine Online ::` prefix. Kitty terminal detection switches `[TERM: xterm-kitty detected]` live at startup.

---

## ✨ Features

### ⚡ Concurrent Multi-Provider Racing

Three providers, one winner — in under 300ms:

- **Anime3rb**, **AnimeSlayer**, and **Animeify** race simultaneously via `tokio::task::JoinSet`
- The fastest successful response wins immediately — stalled or slow mirrors are silently discarded
- Zero blocking — a dead provider never delays your stream

### 🎯 Smart Resolution Engine

- Parses `2160p` → `1080p` → `720p` → `480p` → `360p` in strict descending order
- Defaults to the highest *verified* quality automatically
- Override at any time with `-q <res>`
- Validates streams via async `HEAD` request with `Range: bytes=0-0` fallback before passing to `mpv`

### 🔁 Seamless Autoplay Next Episode

- Controls `mpv` over `/tmp/senku-anime-cli.sock` using JSON-IPC
- Monitors playback percentage in real time
- Reaching `≥99%` or natural EOF (`0`) automatically fetches and loads episode N+1 without returning to the menu
- No menu re-entry. No manual selection. Pure continuity.

### 📥 Multi-Threaded Chunked HTTP Downloader

A fully native Rust downloader — no `aria2c`, no `wget`, no shell-outs:

| Capability | Detail |
|---|---|
| **Range requests** | Concurrent HTTP `Range` splits per episode |
| **Batch ranges** | `-e 1-12` downloads a full season in parallel |
| **Progress bars** | Live `indicatif` bars with MB/s and ETA per episode |
| **Output path** | Configurable via `-o <dir>` |
| **Validation** | Stream URL verified before download begins |

### 📖 Watch History & Resume

- Persists playback state in `~/.local/share/senku-anime-cli/history.json`
- Resume last watched anime and episode with a single flag: `sac -c`
- Fully local — zero telemetry, nothing leaves your machine

### 🐱 Arch Linux & Kitty Ready

- Native `$TERM == "xterm-kitty"` detection with `--kitty` flag for video output passthrough to mpv
- `fzf` fuzzy selection for both anime search results and episode lists
- Pure-Rust TTY fallback picker when `fzf` is not installed
- `PKGBUILD` included for `senku-anime-cli-git` on Arch Linux

---

## ⌨️ CLI Reference

### Streaming

| Command | Description |
|---|---|
| `sac "<title>"` | Search & stream at highest verified quality, autoplay enabled |
| `sac "<title>" -q 720p` | Force specific resolution |
| `sac "<title>" --kitty` | Stream with Kitty terminal video passthrough flags |
| `sac -c` | Resume last watched anime at last episode |

### Downloading

| Command | Description |
|---|---|
| `sac "<title>" -d` | Download current episode |
| `sac "<title>" -e 5` | Stream or download specific episode |
| `sac "<title>" -e 1-12 -d` | Batch download episodes 1 through 12 |
| `sac "<title>" -e 1-12 -d -o ~/Videos/Anime` | Batch download to specific output directory |

### History

| Command | Description |
|---|---|
| `sac -c` | Continue watching — resume last anime & episode |
| `4` *(from menu)* | View full watch history |

---

## 🏗️ Architecture

```
senku-anime-cli/
├── src/
│   ├── main.rs           # Entry point · TUI menu · portal engine boot sequence
│   ├── bin/sac.rs        # sac alias binary
│   └── ...               # Providers · downloader · mpv IPC · history · resolution engine
├── tests/                # Integration tests
├── Cargo.toml            # v1.8.0 — full dependency manifest
├── Cargo.lock
├── PKGBUILD              # Arch Linux package build script
├── preview.gif
└── LICENSE               # GPL-3.0-only
```

### Core Dependency Stack (from `Cargo.toml`)

```toml
[dependencies]
tokio       = { version = "1.43", features = ["full"] }         # Async runtime
reqwest     = { version = "0.12", features = ["rustls-tls",     # HTTP client
               "json", "gzip", "brotli", "stream", "cookies"] }
scraper     = "0.22"                                            # HTML provider scraping
clap        = { version = "4.5", features = ["derive", "env",   # CLI args
               "wrap_help"] }
clap_complete = "4.5"                                           # Shell completions
indicatif   = "0.17"                                            # Progress bars
crossterm   = { version = "0.28", features = ["event-stream"] } # Terminal UI
owo-colors  = { version = "4.1", features = ["supports-colors"]}# TrueColor output
serde       = { version = "1.0", features = ["derive"] }        # Serialization
serde_json  = "1.0"                                             # JSON history
anyhow      = "1.0"                                             # Error handling
thiserror   = "2.0"                                             # Error types

[profile.release]
opt-level       = 3
lto             = "thin"
codegen-units   = 1
strip           = true
```

---

## 🔨 Build & Install

### Requirements

- Rust `>=1.75`
- `mpv` — for streaming and episode playback
- `fzf` *(optional)* — fuzzy anime and episode selection; falls back to pure-Rust TTY picker
- A terminal emulator with **Nerd Font** support
  - Recommended: Kitty · Alacritty · Foot · WezTerm

---

## Installation Options

### Option 1: Arch Linux (PKGBUILD)

```bash
git clone https://github.com/islamMendjel/senku-anime-cli.git
cd senku-anime-cli
makepkg -si
```

Installs and registers `senku-anime-cli` directly in pacman's database.

### Option 2: Compile from Source (Cargo)

```bash
git clone https://github.com/islamMendjel/senku-anime-cli.git
cd senku-anime-cli

# Build optimized release binary
cargo build --release

# Install sac + senku-anime-cli into ~/.cargo/bin
cargo install --path .
```

### Option 3: Copy Binaries Directly

```bash
sudo install -Dm755 target/release/senku-anime-cli /usr/local/bin/senku-anime-cli
sudo ln -sf /usr/local/bin/senku-anime-cli /usr/local/bin/sac
```

---

## Usage

```bash
# Stream anime at highest quality (autoplay next episode)
sac "Steins;Gate"

# Resume last watched
sac -c

# Force 720p with Kitty passthrough
sac "One Piece" -q 720p --kitty

# Download a full season
sac "Attack on Titan" -e 1-25 -d -o ~/Videos/Anime
```

---

## 🖥️ Terminal Setup

For the full portal engine aesthetic and correct Nerd Font glyph rendering:

**Font** — Any Nerd Font patched font works. Recommended:
```
JetBrainsMono Nerd Font
CaskaydiaCove Nerd Font
Iosevka Nerd Font
```

**Kitty config example:**
```ini
font_family        JetBrainsMono Nerd Font Mono
font_size          13.0
background         #000000
background_opacity 0.95
```

**Alacritty config example:**
```yaml
colors:
  primary:
    background: '#000000'
font:
  normal:
    family: "JetBrainsMono Nerd Font Mono"
  size: 13.0
```

The portal engine detects your terminal at boot and prints `[TERM: xterm-kitty detected]` when running inside Kitty, enabling native video passthrough mode automatically.

---

## 🛡️ Performance & Safety

senku-anime-cli is built with performance and correctness as first-class constraints:

- **Concurrent by design** — `tokio::task::JoinSet` races providers in parallel; no sequential fallback chain
- **Stream validation before playback** — async `HEAD` checks with `Range: bytes=0-0` fallback confirm stream availability before `mpv` is ever spawned
- **rustls-tls** — pure Rust TLS, no OpenSSL dependency, no system library version mismatches
- **Release profile hardened** — `opt-level = 3`, `lto = "thin"`, `codegen-units = 1`, `strip = true`
- **History is local** — `~/.local/share/senku-anime-cli/history.json` is the only file written; no network calls outside of provider scraping and stream fetching

---

## 📄 License

GNU General Public License v3.0 (`GPL-3.0-only`) — See [LICENSE](LICENSE) for the full text.

---

<div align="center">

Built from scratch by [islamMendjel](https://github.com/islamMendjel)

*Rust 2021 · Zero telemetry · Dimension C-137 portal engineering*

</div>
