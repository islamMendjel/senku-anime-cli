use crate::cli::log_info;
use crate::error::{Result, SenkuError};
use crate::models::StreamSource;
use futures::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::header::{ACCEPT_RANGES, CONTENT_LENGTH, RANGE, REFERER};
use reqwest::{Client, StatusCode};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;

const MIN_CHUNKED_BYTES: u64 = 512 * 1024; // 512 KiB
const MAX_RETRIES: usize = 3;

/// Pure-Rust multi-threaded HTTP Range downloader with live indicatif progress,
/// MB/s throughput, and ETA — zero external tools like `aria2c`.
pub struct Downloader {
    client: Client,
    threads: usize,
}

#[derive(Debug)]
struct ProbeInfo {
    total_size: Option<u64>,
    supports_ranges: bool,
}

impl Downloader {
    pub fn new(client: Client, threads: usize) -> Self {
        Self {
            client,
            threads: threads.clamp(1, 32),
        }
    }

    /// Builds a clean, filesystem-safe UTF-8 filename like `Steins_Gate_E01_1080p.mp4`
    /// or `ون_بيس_E01_1080p.mp4` while stripping path separators and unsafe characters.
    pub fn build_filename(anime_title: &str, episode_num: u32, stream: &StreamSource) -> String {
        let safe_title: String = anime_title
            .chars()
            .map(|c| {
                let is_unsafe = matches!(
                    c,
                    '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' | '\0'
                ) || c.is_control();
                if is_unsafe {
                    '_'
                } else if c.is_alphanumeric() || c == '-' || c == '_' || is_arabic_combining_mark(c)
                {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        let collapsed: String = safe_title
            .split('_')
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
            .join("_");
        let trimmed_dots = collapsed.trim_start_matches('.');
        let base = if trimmed_dots.is_empty() {
            "anime".to_string()
        } else {
            truncate_utf8_bytes(trimmed_dots, 160)
        };
        format!("{base}_E{episode_num:02}_{}.mp4", stream.resolution)
    }

    pub async fn download_episode(
        &self,
        anime_title: &str,
        episode_num: u32,
        stream: &StreamSource,
        output_dir: &Path,
    ) -> Result<PathBuf> {
        tokio::fs::create_dir_all(output_dir).await?;
        let filename = Self::build_filename(anime_title, episode_num, stream);
        let final_path = output_dir.join(&filename);
        let part_path = output_dir.join(format!("{filename}.part"));

        let probe = self.probe_target(stream).await?;
        log_info(&format!(
            "Downloading {} [{}] ({})",
            filename,
            stream.resolution,
            if probe.supports_ranges && probe.total_size.unwrap_or(0) >= MIN_CHUNKED_BYTES {
                format!("{} threads, HTTP Range", self.threads)
            } else {
                "single-stream fallback".to_string()
            }
        ));

        let pb = make_progress_bar(probe.total_size, &filename);

        let result = match (probe.total_size, probe.supports_ranges) {
            (Some(total), true) if total >= MIN_CHUNKED_BYTES && self.threads > 1 => {
                self.download_chunked(stream, &part_path, total, &pb).await
            }
            _ => self.download_single(stream, &part_path, &pb).await,
        };

        if let Err(e) = result {
            pb.abandon_with_message(format!("Failed: {filename}"));
            return Err(e);
        }

        tokio::fs::rename(&part_path, &final_path).await?;
        pb.finish_with_message(format!("Saved: {}", final_path.display()));
        Ok(final_path)
    }

    async fn probe_target(&self, stream: &StreamSource) -> Result<ProbeInfo> {
        let mut req = self.client.head(&stream.url);
        if let Some(ref r) = stream.referer {
            req = req.header(REFERER, r);
        }

        if let Ok(resp) = req.send().await {
            if resp.status().is_success() {
                let total_size = resp
                    .headers()
                    .get(CONTENT_LENGTH)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok())
                    .or(stream.content_length);
                let supports_ranges = resp
                    .headers()
                    .get(ACCEPT_RANGES)
                    .and_then(|v| v.to_str().ok())
                    .map(|v| v.eq_ignore_ascii_case("bytes"))
                    .unwrap_or(false);

                if supports_ranges && total_size.is_some() {
                    return Ok(ProbeInfo {
                        total_size,
                        supports_ranges: true,
                    });
                }
            }
        }

        // Probe with Range: bytes=0-0 to verify actual server support & Content-Range total.
        let mut range_req = self.client.get(&stream.url).header(RANGE, "bytes=0-0");
        if let Some(ref r) = stream.referer {
            range_req = range_req.header(REFERER, r);
        }
        let resp = range_req.send().await?;
        let status = resp.status();

        if status == StatusCode::PARTIAL_CONTENT {
            let total_size = resp
                .headers()
                .get(reqwest::header::CONTENT_RANGE)
                .and_then(|v| v.to_str().ok())
                .and_then(|cr| cr.rsplit('/').next())
                .and_then(|total| total.parse::<u64>().ok())
                .or(stream.content_length);
            return Ok(ProbeInfo {
                total_size,
                supports_ranges: total_size.is_some(),
            });
        }

        let total_size = resp
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .or(stream.content_length);

        Ok(ProbeInfo {
            total_size,
            supports_ranges: false,
        })
    }

    async fn download_chunked(
        &self,
        stream: &StreamSource,
        part_path: &Path,
        total_size: u64,
        pb: &ProgressBar,
    ) -> Result<()> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(true)
            .open(part_path)?;
        file.set_len(total_size)?;
        let shared_file = Arc::new(file);

        let chunk_count = self.threads as u64;
        let base_chunk = total_size / chunk_count;
        let mut set = JoinSet::new();

        for i in 0..chunk_count {
            let start = i * base_chunk;
            let end = if i == chunk_count - 1 {
                total_size - 1
            } else {
                (i + 1) * base_chunk - 1
            };
            if start > end {
                continue;
            }

            let client = self.client.clone();
            let url = stream.url.clone();
            let referer = stream.referer.clone();
            let file_ref = Arc::clone(&shared_file);
            let pb_ref = pb.clone();
            let file_label = part_path.display().to_string();

            set.spawn(async move {
                fetch_chunk_with_retry(
                    &client,
                    &url,
                    referer.as_deref(),
                    start,
                    end,
                    file_ref,
                    pb_ref,
                    &file_label,
                )
                .await
            });
        }

        while let Some(joined) = set.join_next().await {
            match joined {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    set.abort_all();
                    return Err(e);
                }
                Err(join_err) => {
                    set.abort_all();
                    return Err(SenkuError::DownloadFailed {
                        file: part_path.display().to_string(),
                        reason: format!("worker panicked: {join_err}"),
                    });
                }
            }
        }

        shared_file.sync_all()?;
        Ok(())
    }

    async fn download_single(
        &self,
        stream: &StreamSource,
        part_path: &Path,
        pb: &ProgressBar,
    ) -> Result<()> {
        let mut req = self.client.get(&stream.url);
        if let Some(ref r) = stream.referer {
            req = req.header(REFERER, r);
        }
        let resp = req.send().await?;
        if !resp.status().is_success() {
            return Err(SenkuError::DownloadFailed {
                file: part_path.display().to_string(),
                reason: format!("HTTP {}", resp.status()),
            });
        }

        if let Some(len) = resp.content_length() {
            pb.set_length(len);
        }

        let mut file = File::create(part_path)?;
        let mut byte_stream = resp.bytes_stream();
        while let Some(chunk) = byte_stream.next().await {
            let bytes = chunk?;
            file.write_all(&bytes)?;
            pb.inc(bytes.len() as u64);
        }
        file.sync_all()?;
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
async fn fetch_chunk_with_retry(
    client: &Client,
    url: &str,
    referer: Option<&str>,
    start: u64,
    end: u64,
    file: Arc<File>,
    pb: ProgressBar,
    file_label: &str,
) -> Result<()> {
    let expected = (end - start) + 1;
    let mut attempt = 0usize;

    loop {
        attempt += 1;
        let mut written_this_attempt = 0u64;
        let range_header = format!("bytes={start}-{end}");
        let mut req = client.get(url).header(RANGE, range_header);
        if let Some(r) = referer {
            req = req.header(REFERER, r);
        }

        let res: Result<()> = async {
            let resp = req.send().await?;
            if resp.status() != StatusCode::PARTIAL_CONTENT && resp.status() != StatusCode::OK {
                return Err(SenkuError::DownloadFailed {
                    file: file_label.to_string(),
                    reason: format!("unexpected HTTP {} for range {start}-{end}", resp.status()),
                });
            }

            let mut offset = start;
            let mut body_stream = resp.bytes_stream();
            while let Some(chunk) = body_stream.next().await {
                let bytes = chunk?;
                let len = bytes.len() as u64;
                let file_clone = Arc::clone(&file);
                let write_offset = offset;
                tokio::task::spawn_blocking(move || file_clone.write_all_at(&bytes, write_offset))
                    .await
                    .map_err(|e| SenkuError::DownloadFailed {
                        file: file_label.to_string(),
                        reason: format!("disk write task failed: {e}"),
                    })??;
                offset += len;
                written_this_attempt += len;
                pb.inc(len);
            }

            if written_this_attempt != expected {
                return Err(SenkuError::DownloadFailed {
                    file: file_label.to_string(),
                    reason: format!(
                        "incomplete chunk {start}-{end}: wrote {written_this_attempt}/{expected} bytes"
                    ),
                });
            }
            Ok(())
        }
        .await;

        match res {
            Ok(()) => return Ok(()),
            Err(e) if attempt < MAX_RETRIES => {
                // Roll back progress increment from failed partial attempt before retrying.
                pb.set_position(pb.position().saturating_sub(written_this_attempt));
                tokio::time::sleep(Duration::from_millis(250 * attempt as u64)).await;
                let _ = e;
            }
            Err(e) => return Err(e),
        }
    }
}

fn is_arabic_combining_mark(c: char) -> bool {
    matches!(c as u32, 0x064B..=0x065F | 0x0670)
}

fn truncate_utf8_bytes(s: &str, max_bytes: usize) -> String {
    if s.len() <= max_bytes {
        return s.to_string();
    }
    let mut end = 0usize;
    for (idx, ch) in s.char_indices() {
        let next = idx + ch.len_utf8();
        if next > max_bytes {
            break;
        }
        end = next;
    }
    let out = s[..end].trim_end_matches(['_', '.', ' ']);
    if out.is_empty() {
        "anime".to_string()
    } else {
        out.to_string()
    }
}

fn make_progress_bar(total: Option<u64>, label: &str) -> ProgressBar {
    let pb = match total {
        Some(len) => ProgressBar::new(len),
        None => ProgressBar::new_spinner(),
    };
    let template = if total.is_some() {
        "{spinner:.green} [{elapsed_precise}] [{bar:38.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, ETA {eta}) {msg}"
    } else {
        "{spinner:.green} [{elapsed_precise}] {bytes} ({bytes_per_sec}) {msg}"
    };
    pb.set_style(
        ProgressStyle::with_template(template)
            .expect("valid indicatif template")
            .progress_chars("=>-"),
    );
    pb.set_message(label.to_string());
    pb
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{ProviderId, Resolution};
    use crate::providers::build_http_client;
    use tempfile::tempdir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    async fn spawn_range_server(payload: Arc<Vec<u8>>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                let data = Arc::clone(&payload);
                tokio::spawn(async move {
                    let mut buf = [0u8; 2048];
                    let n = sock.read(&mut buf).await.unwrap_or(0);
                    let req = String::from_utf8_lossy(&buf[..n]);
                    let first = req.lines().next().unwrap_or("");
                    let range_hdr = req
                        .lines()
                        .find(|l| l.to_ascii_lowercase().starts_with("range:"))
                        .and_then(|l| l.split_once(':').map(|(_, v)| v.trim().to_string()));

                    if first.starts_with("HEAD ") {
                        let resp = format!(
                            "HTTP/1.1 200 OK\r\nAccept-Ranges: bytes\r\nContent-Length: {}\r\nContent-Type: video/mp4\r\nConnection: close\r\n\r\n",
                            data.len()
                        );
                        let _ = sock.write_all(resp.as_bytes()).await;
                        return;
                    }

                    if let Some(r) =
                        range_hdr.and_then(|h| h.strip_prefix("bytes=").map(str::to_string))
                    {
                        if let Some((s, e)) = r.split_once('-') {
                            let start: usize = s.parse().unwrap_or(0);
                            let end: usize =
                                e.parse().unwrap_or(data.len() - 1).min(data.len() - 1);
                            let slice = &data[start..=end];
                            let hdr = format!(
                                "HTTP/1.1 206 Partial Content\r\nAccept-Ranges: bytes\r\nContent-Range: bytes {start}-{end}/{}\r\nContent-Length: {}\r\nContent-Type: video/mp4\r\nConnection: close\r\n\r\n",
                                data.len(),
                                slice.len()
                            );
                            let _ = sock.write_all(hdr.as_bytes()).await;
                            let _ = sock.write_all(slice).await;
                            return;
                        }
                    }

                    let hdr = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: video/mp4\r\nConnection: close\r\n\r\n",
                        data.len()
                    );
                    let _ = sock.write_all(hdr.as_bytes()).await;
                    let _ = sock.write_all(&data).await;
                });
            }
        });
        format!("http://{addr}/episode.mp4")
    }

    #[tokio::test]
    async fn multi_threaded_chunked_download_reassembles_exact_bytes() {
        // 1.5 MiB deterministic pattern so chunked mode triggers (>= 512 KiB).
        let payload: Vec<u8> = (0..(1536 * 1024)).map(|i| (i % 251) as u8).collect();
        let expected = Arc::new(payload);
        let url = spawn_range_server(Arc::clone(&expected)).await;

        let dir = tempdir().unwrap();
        let client = build_http_client(Duration::from_secs(5)).unwrap();
        let downloader = Downloader::new(client, 4);

        let stream = StreamSource {
            url,
            resolution: Resolution::Fhd1080,
            provider: ProviderId::Anime3rb,
            server_name: "local-test".into(),
            referer: None,
            content_length: Some(expected.len() as u64),
            verified: true,
            throughput_kbps: None,
            supports_ranges: true,
        };

        let out_path = downloader
            .download_episode("Steins;Gate", 1, &stream, dir.path())
            .await
            .unwrap();

        let actual = tokio::fs::read(&out_path).await.unwrap();
        assert_eq!(actual.len(), expected.len());
        assert_eq!(actual, *expected);
    }

    #[test]
    fn sanitizes_arabic_utf8_and_unsafe_path_titles() {
        let stream = StreamSource {
            url: "https://cdn.example/ep.mp4".into(),
            resolution: Resolution::Fhd1080,
            provider: ProviderId::Animerco,
            server_name: "cdn".into(),
            referer: None,
            content_length: None,
            verified: true,
            throughput_kbps: None,
            supports_ranges: true,
        };

        let ar_name = Downloader::build_filename("ون بيس / هجوم العمالقة: الجزء؟ *1*", 7, &stream);
        assert_eq!(ar_name, "ون_بيس_هجوم_العمالقة_الجزء_1_E07_1080p.mp4");
        assert!(!ar_name.contains(['/', '\\', ':', '*', '?', '"', '<', '>', '|', '\0']));

        let empty_name = Downloader::build_filename("///:::***", 1, &stream);
        assert_eq!(empty_name, "anime_E01_1080p.mp4");

        let long_ar = "ون ".repeat(110);
        let capped = Downloader::build_filename(&format!("../../{long_ar}\0"), 99, &stream);
        assert!(
            capped.len() < 200,
            "filename byte len {} must fit cleanly under ext4 NAME_MAX",
            capped.len()
        );
        assert!(!capped.starts_with('.'));
        assert!(!capped.contains(".."));
    }
}
