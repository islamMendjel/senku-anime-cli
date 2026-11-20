# senku-anime-cli

> *"Listen Morty—\*burp\*—I turned an anime scraper into a zero-bloat Rust portal gun. 10 billion percent pure Arabic subs. Zero telemetry."*

**`senku-anime-cli`** (alias: **`sac`**) is a high-performance, zero-bloat CLI written in Rust to search, stream, and download Arabic-subbed anime directly from the terminal. No Electron, no browser — just `mpv`, async Rust, and raw speed.

---

```
     .----.     .----.
    /  \  \ __ /  /  \
   / /  \(  )/ /\ \  \
  /////   \/ \\\\\\   \      senku-anime-cli (sac) v1.8.0
 //// / // :   \\ \ \\\\     ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// /   / `   '\ \   \ \\     "Listen Morty—*burp*—I turned an anime scraper
//         //..\\         \\ into a zero-bloat Rust portal gun. 10 billion
       ====UU====UU====       percent pure Arabic subs. Zero telemetry."
           '//|||\\`
             '``

:: Portal Engine Online — 10,000,000,000% Scientific Precision :: [TERM: xterm-kitty detected]
```

---

![senku-anime-cli demo](preview.gif)

---

## Features

- **Concurrent Multi-Provider Racing (`<300ms`):** Concurrently races *Anime3rb*, *AnimeSlayer*, and *Animeify* via `tokio::task::JoinSet` — fastest provider wins, stalled mirrors never block you.
- **Smart Resolution Defaulting:** Automatically parses `2160p`/`1080p`/`720p`/`480p`/`360p`, strictly defaults to the highest verified quality (override with `-q <res>`), and validates streams via async `HEAD` (with `Range: bytes=0-0` fallback).
- **Seamless Autoplay Next Episode:** Manages `mpv` over `/tmp/senku-anime-cli.sock` (JSON-IPC). Reaching `≥99%` or natural EOF automatically fetches and loads episode N+1 without returning to the menu.
- **Multi-Threaded Chunked HTTP Downloader:** Supports single episodes and ranges (`-e 1-12`) using concurrent HTTP `Range` requests, live `indicatif` progress bars (`MB/s`, `ETA`), and zero `aria2c` dependency.
- **Watch History & Resume (`-c`):** Persists local state in `~/.local/share/senku-anime-cli/history.json` with zero telemetry.
- **Arch Linux & Kitty Ready:** Native `$TERM == "xterm-kitty"` detection (`--kitty`), `fzf` fuzzy selection (with pure-Rust TTY fallback), and `PKGBUILD` for `senku-anime-cli-git`.

---

## Build & Install

### From Source (Cargo)

```bash
cargo build --release
```

Install both `senku-anime-cli` and `sac` binaries into `~/.cargo/bin`:

```bash
cargo install --path .
```

Or copy directly into `/usr/local/bin`:

```bash
sudo install -Dm755 target/release/senku-anime-cli /usr/local/bin/senku-anime-cli
sudo ln -sf /usr/local/bin/senku-anime-cli /usr/local/bin/sac
```

### Arch Linux (PKGBUILD)

```bash
makepkg -si
```

---

## Usage

```bash
# Search & stream highest verified quality (autoplays N -> N+1)
senku-anime-cli "Steins;Gate"
sac "Steins;Gate"

# Resume last watched anime & episode
sac -c

# Force 720p and pass Kitty video output flags to mpv
sac "One Piece" -q 720p --kitty

# Batch chunk-download episodes 1 through 12
sac "Attack on Titan" -e 1-12 -d -o ~/Videos/Anime
```

---

## Project Structure

```
senku-anime-cli/
├── src/
│   ├── main.rs           # Entry point, TUI menu, portal engine boot
│   ├── bin/sac.rs        # sac alias binary
│   └── ...               # Providers, downloader, player, history
├── tests/
├── Cargo.toml            # v1.8.0 — tokio, reqwest, scraper, clap, indicatif
├── PKGBUILD              # Arch Linux package build
├── preview.gif
└── LICENSE               # GPL-3.0
```

---

## Requirements

- `mpv` — for streaming
- `fzf` *(optional)* — fuzzy selection (falls back to pure-Rust TTY)
- Rust `>=1.75` — for building from source

---

## License

GNU General Public License v3.0 (`GPL-3.0-only`). See [LICENSE](LICENSE).
