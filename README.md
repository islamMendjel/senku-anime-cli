# 🧪 senku-anime-cli

A fast, minimalist terminal CLI for searching, streaming, and downloading **Arabic-subbed anime** directly from the command line. 

Inspired by the Unix philosophy of [`ani-cli`](https://github.com/pystardust/ani-cli), rebuilt and tailored from the ground up as an Arabic-subbed edition with smart defaults, zero-buffer `mpv` playback, and automated episode queuing.

> **Status:** 🚧 *Work in Progress / Brainstorming & Architecture Phase*

---

## 🎯 Core Goals (Why this exists)
- Bring the speed and simplicity of `ani-cli` to Arabic anime streaming without bloated web browsers, pop-up ads, or heavy JS players.
- **Auto-Best Resolution:** Automatically lock onto the highest available stream quality (`1080p` / max bitrate) by default without nagging prompts.
- **Autoplay Next Episode:** Seamless binge-watching loop—when episode `N` finishes in `mpv`, episode `N+1` resolves and launches automatically.

---

## ⚡ Planned CLI Usage

```bash
# Search and stream interactively (defaults to 1080p + autoplay)
senku-anime-cli "One Piece"

To Add Later ....
