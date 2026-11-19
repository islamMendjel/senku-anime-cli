use thiserror::Error;

/// Domain-specific error types for `senku-anime-cli`.
#[derive(Debug, Error)]
pub enum SenkuError {
    #[error("HTTP/network transport failure: {0}")]
    Http(#[from] reqwest::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("URL parse error: {0}")]
    Url(#[from] url::ParseError),

    #[error("No anime results found for query \"{0}\" across active Arabic providers")]
    NoSearchResults(String),

    #[error("Provider \"{provider}\" failed: {reason}")]
    ProviderFailure { provider: String, reason: String },

    #[error("All providers failed or timed out within {timeout_ms}ms:\n{details}")]
    ProvidersExhausted { timeout_ms: u64, details: String },

    #[error("No episodes available for \"{0}\"")]
    NoEpisodesFound(String),

    #[error("Episode {requested} not found (available: {available})")]
    EpisodeNotFound { requested: u32, available: String },

    #[error(
        "Invalid episode specification \"{0}\" (expected e.g. \"5\", \"1-12\", or \"1,3,5-8\")"
    )]
    InvalidEpisodeSpec(String),

    #[error("Invalid resolution \"{0}\" (expected e.g. \"1080p\", \"720p\", \"480p\", \"360p\")")]
    InvalidResolution(String),

    #[error("No valid/reachable stream URLs survived HEAD verification for episode {episode}")]
    NoValidStreams { episode: u32 },

    #[error("Selection cancelled by user")]
    SelectionCancelled,

    #[error("External tool missing: {tool} — {hint}")]
    MissingBinary { tool: String, hint: String },

    #[error("mpv IPC error: {0}")]
    MpvIpc(String),

    #[error("mpv exited unexpectedly with status {0}")]
    MpvExit(i32),

    #[error("Chunked download failed for \"{file}\": {reason}")]
    DownloadFailed { file: String, reason: String },

    #[error("No watch history found at {path} — watch an episode first before using -c")]
    EmptyHistory { path: String },
}

pub type Result<T> = std::result::Result<T, SenkuError>;
