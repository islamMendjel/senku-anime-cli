pub mod cli;
pub mod config;
pub mod doctor;
pub mod downloader;
pub mod error;
pub mod history;
pub mod metadata;
pub mod models;
pub mod player;
pub mod preview;
pub mod providers;
pub mod proxy;
pub mod resolver;
pub mod selector;

use anyhow::Context;
use chrono::{DateTime, Utc};
use clap::Parser;
use cli::{log_err, log_info, log_warn, print_banner, Cli};
use config::{AppConfig, EffectiveSettings};
use downloader::Downloader;
use error::{Result, SenkuError};
use history::{check_new_episodes, HistoryStore, NewEpisodeAlert};
use models::{
    AnimeResult, Episode, EpisodeSelection, ProviderId, Resolution, StreamSource, WatchHistoryEntry,
};
use owo_colors::OwoColorize;
use player::{MpvPlayer, PlaybackStop};
use providers::{build_http_client, ProviderEngine, DEFAULT_FIRST_RESULT_GRACE};
use reqwest::Client;
use resolver::StreamResolver;
use selector::{LauncherAction, PostPlaybackAction};
use std::collections::{HashMap, HashSet};
use std::io::{self, BufRead, Write};
use std::sync::Arc;
use std::time::Duration;

/// Shared async entry point for both `senku-anime-cli` and `sac` binaries.
pub async fn cli_main() {
    if let Err(err) = run().await {
        if matches!(
            err.downcast_ref::<SenkuError>(),
            Some(SenkuError::SelectionCancelled)
        ) {
            std::process::exit(0);
        }
        log_err(&format!("{err}"));
        std::process::exit(1);
    }
}

async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();

    if let Some(shell) = cli.completions {
        cli::write_completions(shell, &mut io::stdout());
        return Ok(());
    }

    if let Some(ref raw_query) = cli.preview {
        let client = build_http_client(Duration::from_secs(5))?;
        preview::run_preview(raw_query, &client).await;
        return Ok(());
    }

    let app_config = AppConfig::load_default();
    let settings = app_config.resolve_settings(&cli);
    let history = HistoryStore::default_store().context("failed to initialize watch history")?;

    if cli.clear_history {
        history.clear()?;
        log_info(&format!(
            "Cleared watch history at {}",
            history.path().display()
        ));
        return Ok(());
    }

    if cli.show_history {
        print_history(&history)?;
        return Ok(());
    }

    print_banner(cli.no_banner);

    // Single pooled HTTP/2 + keep-alive client reused for the entire session.
    let client = build_http_client(Duration::from_secs(15))?;
    let engine = Arc::new(ProviderEngine::default_engine(client.clone()));

    if cli.doctor {
        doctor::run_doctor(&client, &engine, &history).await?;
        return Ok(());
    }

    if cli.updates {
        return run_updates_radar(&engine, &history).await;
    }

    let resolver = Arc::new(StreamResolver::new(client.clone()));

    if cli.is_zero_arg_launcher() {
        return run_interactive_launcher(&cli, &settings, &client, &engine, &resolver, &history)
            .await;
    }

    let (anime, resume_episode, resume_position) = if cli.continue_watch {
        let entry = history.latest()?;
        let pos_note = entry
            .position_secs
            .filter(|&s| s > 10)
            .map(|s| format!(" @ {:02}:{:02}", s / 60, s % 60))
            .unwrap_or_default();
        log_info(&format!(
            "Resuming from history: {} (Episode {:02}{pos_note})",
            entry.title.bright_white().bold(),
            entry.episode
        ));
        resolve_history_entry(&entry, &engine, &cli, &settings).await?
    } else {
        let query = cli
            .query
            .clone()
            .filter(|q| !q.trim().is_empty())
            .ok_or(SenkuError::SelectionCancelled)?;
        let picked = search_and_pick_anime(&query, &engine, &cli, &settings).await?;
        (picked, None, None)
    };

    if cli.download {
        return run_download_flow(
            &anime,
            cli.episode.as_deref(),
            &cli,
            &settings,
            &client,
            &engine,
            &resolver,
            &history,
        )
        .await;
    }

    let episodes = engine.get_episodes(&anime).await?;
    let mut guides: HashMap<u32, metadata::EpisodeMeta> = HashMap::new();
    let start_idx = if cli.episode.is_none() && resume_episode.is_none() {
        pick_episode_smart(
            &client,
            &anime,
            &episodes,
            &mut guides,
            &history,
            cli.no_fzf,
        )
        .await?
    } else {
        resolve_stream_start_index(
            &episodes,
            cli.episode.as_deref(),
            resume_episode,
            cli.no_fzf,
        )?
    };

    // Only pass saved position_secs when starting on the exact resumed episode.
    let initial_start_secs =
        if cli.episode.is_none() && resume_episode == Some(episodes[start_idx].number) {
            resume_position.filter(|&s| s > 10)
        } else {
            None
        };

    if cli.url_only {
        let start_ep = &episodes[start_idx];
        let streams = resolve_episode_streams_with_fallback(
            &anime,
            start_ep,
            settings.quality,
            &engine,
            &resolver,
        )
        .await?;
        cli::writeln_ignore_broken_pipe(&mut io::stdout(), &streams[0].url)?;
        return Ok(());
    }

    stream_session_loop(
        anime,
        episodes,
        start_idx,
        initial_start_secs,
        guides,
        false,
        &cli,
        &settings,
        &client,
        &engine,
        &resolver,
        &history,
    )
    .await
}

async fn run_updates_radar(
    engine: &Arc<ProviderEngine>,
    history: &HistoryStore,
) -> anyhow::Result<()> {
    let entries = history.load_all()?;
    if entries.is_empty() {
        log_info("No watch history yet. Watch an anime first!");
        return Ok(());
    }

    log_info("Checking your recent anime for newly dropped episodes...");
    let mut cache = HashMap::new();
    let alerts = check_new_episodes(&entries, engine, &mut cache).await;
    if alerts.is_empty() {
        log_info("All caught up — no new episodes detected for your recent anime.");
        return Ok(());
    }

    eprintln!("\n{}:", "New Episode Radar".bright_cyan().bold());
    for entry in entries.iter().take(6) {
        if let Some(alert) = alerts.get(&entry.anime_slug) {
            let prov = entry.provider.map(|p| p.short_tag()).unwrap_or("?");
            eprintln!(
                "  [{}] {} — watched Ep {:02} → {} (latest: Ep {:02})",
                prov.bright_green(),
                entry.title.bright_white().bold(),
                alert.watched_episode,
                format!("✦ Ep {:02} NEW!", alert.next_episode_to_watch)
                    .bright_green()
                    .bold(),
                alert.latest_available
            );
        }
    }
    eprintln!(
        "\nRun {} and open {} to launch the next episode immediately.",
        "sac".bright_cyan(),
        "Continue Watching".bright_white().bold()
    );
    Ok(())
}

async fn run_interactive_launcher(
    cli: &Cli,
    settings: &EffectiveSettings,
    client: &Client,
    engine: &Arc<ProviderEngine>,
    resolver: &Arc<StreamResolver>,
    history: &HistoryStore,
) -> anyhow::Result<()> {
    let mut radar_cache: HashMap<String, Option<NewEpisodeAlert>> = HashMap::new();
    loop {
        let action = match selector::select_launcher_action(cli.no_fzf) {
            Ok(act) => act,
            Err(SenkuError::SelectionCancelled) => return Ok(()),
            Err(e) => return Err(e.into()),
        };

        match action {
            LauncherAction::SearchAnime => {
                let query = match prompt_line("Search Arabic anime >")? {
                    Some(q) => q,
                    None => continue,
                };
                let anime = match search_and_pick_anime(&query, engine, cli, settings).await {
                    Ok(a) => a,
                    Err(SenkuError::SelectionCancelled) => continue,
                    Err(e) => {
                        log_err(&e.to_string());
                        continue;
                    }
                };
                let episodes = match engine.get_episodes(&anime).await {
                    Ok(eps) => eps,
                    Err(e) => {
                        log_err(&e.to_string());
                        continue;
                    }
                };
                let mut guides = HashMap::new();
                let start_idx = match pick_episode_smart(
                    client,
                    &anime,
                    &episodes,
                    &mut guides,
                    history,
                    cli.no_fzf,
                )
                .await
                {
                    Ok(idx) => idx,
                    Err(SenkuError::SelectionCancelled) => continue,
                    Err(e) => return Err(e.into()),
                };
                stream_session_loop(
                    anime, episodes, start_idx, None, guides, true, cli, settings, client, engine,
                    resolver, history,
                )
                .await?;
                return Ok(());
            }
            LauncherAction::ContinueWatching => {
                let entries = history.load_all()?;
                if entries.is_empty() {
                    log_warn("Watch history is empty. Search an anime first!");
                    continue;
                }
                let alerts = check_new_episodes(&entries, engine, &mut radar_cache).await;
                let idx = match selector::select_history_entry_with_alerts(
                    &entries,
                    &alerts,
                    cli.no_fzf,
                    settings.preview,
                ) {
                    Ok(i) => i,
                    Err(SenkuError::SelectionCancelled) => continue,
                    Err(e) => return Err(e.into()),
                };
                let selected_entry = &entries[idx];
                let (anime, mut resume_ep, mut resume_pos) =
                    match resolve_history_entry(selected_entry, engine, cli, settings).await {
                        Ok(res) => res,
                        Err(e) => {
                            log_err(&e.to_string());
                            continue;
                        }
                    };
                if selected_entry.position_secs.is_none_or(|p| p <= 10) {
                    if let Some(alert) = alerts.get(&selected_entry.anime_slug) {
                        resume_ep = Some(alert.next_episode_to_watch);
                        resume_pos = None;
                    }
                }
                let episodes = match engine.get_episodes(&anime).await {
                    Ok(eps) => eps,
                    Err(e) => {
                        log_err(&e.to_string());
                        continue;
                    }
                };
                let start_idx = resolve_stream_start_index(&episodes, None, resume_ep, cli.no_fzf)?;
                let start_secs = if resume_ep == Some(episodes[start_idx].number) {
                    resume_pos.filter(|&s| s > 10)
                } else {
                    None
                };
                stream_session_loop(
                    anime,
                    episodes,
                    start_idx,
                    start_secs,
                    HashMap::new(),
                    true,
                    cli,
                    settings,
                    client,
                    engine,
                    resolver,
                    history,
                )
                .await?;
                return Ok(());
            }
            LauncherAction::DownloadEpisodes => {
                let query = match prompt_line("Search anime to download >")? {
                    Some(q) => q,
                    None => continue,
                };
                let anime = match search_and_pick_anime(&query, engine, cli, settings).await {
                    Ok(a) => a,
                    Err(SenkuError::SelectionCancelled) => continue,
                    Err(e) => {
                        log_err(&e.to_string());
                        continue;
                    }
                };
                let episodes = engine.get_episodes(&anime).await?;
                let avail = format_available_range(&episodes);
                let range_prompt =
                    format!("Episode or range to download (available: {avail}, blank = pick) >");
                let ep_spec = prompt_line(&range_prompt)?;
                run_download_flow(
                    &anime,
                    ep_spec.as_deref(),
                    cli,
                    settings,
                    client,
                    engine,
                    resolver,
                    history,
                )
                .await?;
            }
            LauncherAction::WatchHistory => {
                manage_history_menu(history, cli.no_fzf)?;
            }
            LauncherAction::Quit => return Ok(()),
        }
    }
}

fn manage_history_menu(history: &HistoryStore, no_fzf: bool) -> Result<()> {
    loop {
        let entries = history.load_all()?;
        if entries.is_empty() {
            log_info(&format!(
                "Watch history is empty ({}).",
                history.path().display()
            ));
            return Ok(());
        }

        print_history(history)?;
        let options = ["Delete a specific entry", "Clear all history", "Back"];
        let choice = match selector::select_menu_item(&options, "History > ", no_fzf) {
            Ok(i) => i,
            Err(SenkuError::SelectionCancelled) => return Ok(()),
            Err(e) => return Err(e),
        };

        match choice {
            0 => {
                if let Ok(idx) = selector::select_history_entry(&entries, no_fzf) {
                    let removed = &entries[idx];
                    if history.remove_entry(&removed.anime_slug)? {
                        log_info(&format!(
                            "Removed \"{}\" from watch history.",
                            removed.title
                        ));
                    }
                }
            }
            1 => {
                history.clear()?;
                log_info("Cleared all watch history.");
                return Ok(());
            }
            _ => return Ok(()),
        }
    }
}

fn initial_quality_session_state(
    cli: &Cli,
    settings: &EffectiveSettings,
) -> (Option<Resolution>, bool) {
    let quality_selected =
        selector::should_skip_quality_prompt(cli.quality, cli.best, settings.auto_select_best);
    (settings.quality, quality_selected)
}

async fn pick_episode_smart(
    client: &Client,
    anime: &AnimeResult,
    episodes: &[Episode],
    guides: &mut HashMap<u32, metadata::EpisodeMeta>,
    history: &HistoryStore,
    no_fzf: bool,
) -> Result<usize> {
    if episodes.len() <= 1 {
        return Ok(0);
    }
    let hist_entry = history
        .load_all()
        .ok()
        .and_then(|list| list.into_iter().find(|e| e.anime_slug == anime.slug));
    let anchor_ep = hist_entry
        .as_ref()
        .map(|e| {
            if e.position_secs.is_some_and(|s| s > 10) {
                e.episode
            } else {
                e.episode + 1
            }
        })
        .unwrap_or_else(|| episodes.first().map(|e| e.number).unwrap_or(1));

    if !guides.contains_key(&anchor_ep) {
        let fetched =
            metadata::fetch_episode_guide_for_title(client, &anime.title, anchor_ep).await;
        guides.extend(fetched);
    }

    let ctx = selector::EpisodePickerContext {
        watched_episodes: hist_entry
            .as_ref()
            .map(|e| e.watched_episodes.as_slice())
            .unwrap_or(&[]),
        in_progress_episode: hist_entry.as_ref().map(|e| e.episode),
        in_progress_secs: hist_entry.as_ref().and_then(|e| e.position_secs),
        guides: Some(guides),
    };
    selector::select_episode_with_context(episodes, no_fzf, &ctx)
}

/// Extracts and verifies stream mirrors with a 3-stage resilience cascade:
/// 1. Strict HTTP reachability & partial-read verification on pooled/primary streams
/// 2. Alternate active provider lookup (`find_alternate_provider_episode`) if all strict probes fail
/// 3. Up to 4 unique valid `http(s)` candidates retained as `[Unverified]` for direct `mpv` playback
pub async fn resolve_episode_streams_with_fallback(
    anime: &AnimeResult,
    ep: &Episode,
    preferred: Option<Resolution>,
    engine: &Arc<ProviderEngine>,
    resolver: &Arc<StreamResolver>,
) -> Result<Vec<StreamSource>> {
    let mut primary_candidates = engine.get_streams(ep).await.unwrap_or_default();
    let stalled = HashSet::new();

    if !primary_candidates.is_empty() {
        if let Ok(strict) = resolver
            .resolve_strict_verified_with_penalties(
                ep.number,
                primary_candidates.clone(),
                preferred,
                &stalled,
            )
            .await
        {
            return Ok(strict);
        }
    }

    let mut tried_providers: HashSet<ProviderId> = HashSet::new();
    tried_providers.insert(anime.provider);
    tried_providers.insert(ep.provider);
    for s in anime.all_sources() {
        tried_providers.insert(s.provider);
    }
    for s in ep.all_sources() {
        tried_providers.insert(s.provider);
    }

    while let Some((alt_anime, alt_ep)) = engine
        .find_alternate_provider_episode(
            &anime.title,
            ep.number,
            &tried_providers,
            Duration::from_secs(4),
        )
        .await
    {
        tried_providers.insert(alt_anime.provider);
        log_info(&format!(
            "Checking alternate provider {} for Episode {:02}...",
            alt_anime.provider.display_name().bright_cyan(),
            ep.number
        ));
        if let Ok(alt_candidates) = engine.get_streams(&alt_ep).await {
            if let Ok(strict) = resolver
                .resolve_strict_verified_with_penalties(
                    ep.number,
                    alt_candidates.clone(),
                    preferred,
                    &stalled,
                )
                .await
            {
                return Ok(strict);
            }
            primary_candidates.extend(alt_candidates);
        }
    }

    let unverified = resolver::fallback_unverified_streams(primary_candidates, preferred, &stalled);
    if !unverified.is_empty() {
        log_warn(&format!(
            "Strict HEAD/Range verification dropped all mirrors for Episode {:02}; retaining {} [Unverified] link(s) for direct mpv playback.",
            ep.number,
            unverified.len()
        ));
        return Ok(unverified);
    }

    Err(SenkuError::NoValidStreams { episode: ep.number })
}

#[allow(clippy::too_many_arguments)]
async fn stream_session_loop(
    mut anime: AnimeResult,
    mut episodes: Vec<Episode>,
    mut start_idx: usize,
    mut initial_start_secs: Option<u64>,
    mut guides: HashMap<u32, metadata::EpisodeMeta>,
    interactive_mode: bool,
    cli: &Cli,
    settings: &EffectiveSettings,
    client: &Client,
    engine: &Arc<ProviderEngine>,
    resolver: &Arc<StreamResolver>,
    history: &HistoryStore,
) -> anyhow::Result<()> {
    if settings.skip_filler && guides.is_empty() {
        let ep_num = episodes.get(start_idx).map(|e| e.number).unwrap_or(1);
        guides = metadata::fetch_episode_guide_for_title(client, &anime.title, ep_num).await;
    }

    let (mut session_quality, mut quality_selected) = initial_quality_session_state(cli, settings);

    loop {
        log_info(&format!(
            "Target: {} [{}]",
            anime.title.bright_white().bold(),
            anime.provider_display().bright_cyan()
        ));

        let ep = &episodes[start_idx];
        let mut verified_streams = match resolve_episode_streams_with_fallback(
            &anime,
            ep,
            session_quality,
            engine,
            resolver,
        )
        .await
        {
            Ok(streams) => streams,
            Err(err) if interactive_mode => {
                log_err(&format!("Episode {:02}: {err}", ep.number));
                Vec::new()
            }
            Err(err) => return Err(err.into()),
        };

        let post_action = if verified_streams.is_empty() {
            let has_next = start_idx + 1 < episodes.len();
            let has_prev = start_idx > 0;
            selector::prompt_post_playback(has_next, has_prev, cli.no_fzf)?
        } else {
            if !quality_selected {
                let turbo_opt = settings.turbo_proxy.then_some(settings.proxy_threads);
                let pick =
                    selector::select_stream_quality(&verified_streams, cli.no_fzf, turbo_opt)?;
                if let Some(chosen_res) = selector::apply_quality_pick(&mut verified_streams, pick)
                {
                    session_quality = Some(chosen_res);
                }
                quality_selected = true;
            }

            let player = MpvPlayer::new(settings.kitty)
                .with_verbose(cli.verbose)
                .with_autoplay(settings.autoplay)
                .with_skip_intro_secs(settings.skip_intro_secs)
                .with_auto_skip_op(settings.auto_skip_op)
                .with_auto_skip_ed(settings.auto_skip_ed)
                .with_no_fzf(cli.no_fzf)
                .with_turbo_proxy(settings.turbo_proxy)
                .with_proxy_threads(settings.proxy_threads)
                .with_cinema_dashboard(settings.cinema_dashboard)
                .with_skip_filler(settings.skip_filler)
                .with_episode_guides(guides.clone());

            let stop = player
                .play_loop(
                    &anime,
                    &episodes,
                    start_idx,
                    verified_streams,
                    initial_start_secs.take(),
                    &mut session_quality,
                    client.clone(),
                    Arc::clone(engine),
                    Arc::clone(resolver),
                    history,
                )
                .await?;

            match stop {
                PlaybackStop::CompletedSeries | PlaybackStop::Interrupted => return Ok(()),
                PlaybackStop::UserStopped { episode_index } => {
                    start_idx = episode_index;
                    let has_next = start_idx + 1 < episodes.len();
                    let has_prev = start_idx > 0;
                    selector::prompt_post_playback(has_next, has_prev, cli.no_fzf)?
                }
            }
        };

        let has_next = start_idx + 1 < episodes.len();
        let has_prev = start_idx > 0;
        match post_action {
            PostPlaybackAction::NextEpisode if has_next => {
                start_idx += 1;
            }
            PostPlaybackAction::NextEpisode => {
                log_warn("Already at the last available episode.");
            }
            PostPlaybackAction::PreviousEpisode if has_prev => {
                start_idx -= 1;
            }
            PostPlaybackAction::PreviousEpisode => {
                log_warn("Already at the first episode.");
            }
            PostPlaybackAction::ReplayCurrent => {}
            PostPlaybackAction::SelectEpisode => {
                match pick_episode_smart(
                    client,
                    &anime,
                    &episodes,
                    &mut guides,
                    history,
                    cli.no_fzf,
                )
                .await
                {
                    Ok(new_idx) => start_idx = new_idx,
                    Err(SenkuError::SelectionCancelled) => return Ok(()),
                    Err(e) => return Err(e.into()),
                }
            }
            PostPlaybackAction::DownloadCurrent => {
                let current_ep = &episodes[start_idx];
                match resolve_episode_streams_with_fallback(
                    &anime,
                    current_ep,
                    session_quality,
                    engine,
                    resolver,
                )
                .await
                {
                    Ok(streams) => {
                        let downloader = Downloader::new(client.clone(), settings.threads);
                        downloader
                            .download_episode(
                                &anime.title,
                                current_ep.number,
                                &streams[0],
                                &settings.download_dir,
                            )
                            .await?;
                    }
                    Err(e) if interactive_mode => log_err(&e.to_string()),
                    Err(e) => return Err(e.into()),
                }
            }
            PostPlaybackAction::SearchAnother => {
                let Some(query) = prompt_line("Search Arabic anime >")? else {
                    return Ok(());
                };
                match search_and_pick_anime(&query, engine, cli, settings).await {
                    Ok(found) => {
                        anime = found;
                        episodes = engine.get_episodes(&anime).await?;
                        guides.clear();
                        start_idx = pick_episode_smart(
                            client,
                            &anime,
                            &episodes,
                            &mut guides,
                            history,
                            cli.no_fzf,
                        )
                        .await?;
                        (session_quality, quality_selected) =
                            initial_quality_session_state(cli, settings);
                    }
                    Err(SenkuError::SelectionCancelled) => return Ok(()),
                    Err(e) if interactive_mode => log_err(&e.to_string()),
                    Err(e) => return Err(e.into()),
                }
            }
            PostPlaybackAction::Quit => return Ok(()),
        }
    }
}

async fn search_and_pick_anime(
    query: &str,
    engine: &ProviderEngine,
    cli: &Cli,
    settings: &EffectiveSettings,
) -> Result<AnimeResult> {
    let race_budget = Duration::from_millis(settings.timeout_ms);
    let (results, elapsed_ms) = engine
        .search_race(
            query,
            cli.provider.to_filter(),
            race_budget,
            DEFAULT_FIRST_RESULT_GRACE,
            cli.include_blocked,
            cli.verbose,
        )
        .await?;

    log_info(&format!(
        "Raced in {}ms — {} match(es)",
        elapsed_ms.to_string().bright_cyan(),
        results.len()
    ));

    let picked_idx = selector::select_anime_with_preview(&results, cli.no_fzf, settings.preview)?;
    Ok(results[picked_idx].clone())
}

#[allow(clippy::too_many_arguments)]
async fn run_download_flow(
    anime: &AnimeResult,
    ep_spec: Option<&str>,
    cli: &Cli,
    settings: &EffectiveSettings,
    client: &Client,
    engine: &ProviderEngine,
    resolver: &StreamResolver,
    history: &HistoryStore,
) -> anyhow::Result<()> {
    let episodes = engine.get_episodes(anime).await?;
    let selected_eps = pick_episodes_for_download(&episodes, ep_spec, cli.no_fzf)?;
    let downloader = Downloader::new(client.clone(), settings.threads);
    for ep in selected_eps {
        let streams = engine.get_streams(&ep).await?;
        let best = resolver
            .resolve_best(ep.number, streams, settings.quality)
            .await?;
        downloader
            .download_episode(&anime.title, ep.number, &best, &settings.download_dir)
            .await?;
        history.record_watch(anime, ep.number)?;
    }
    Ok(())
}

async fn resolve_history_entry(
    entry: &WatchHistoryEntry,
    engine: &ProviderEngine,
    cli: &Cli,
    settings: &EffectiveSettings,
) -> Result<(AnimeResult, Option<u32>, Option<u64>)> {
    if let (Some(url), Some(provider)) = (entry.anime_url.clone(), entry.provider) {
        return Ok((
            AnimeResult {
                slug: entry.anime_slug.clone(),
                title: entry.title.clone(),
                alt_title: None,
                url,
                provider,
                year: None,
                episode_count: None,
                latency_ms: 0,
                sources: entry.sources.clone(),
            },
            Some(entry.episode),
            entry.position_secs,
        ));
    }

    let (results, _) = engine
        .search_race(
            &entry.title,
            cli.provider.to_filter().or(entry.provider),
            Duration::from_millis(settings.timeout_ms),
            DEFAULT_FIRST_RESULT_GRACE,
            cli.include_blocked,
            cli.verbose,
        )
        .await?;

    let matched = results
        .into_iter()
        .find(|r| r.slug == entry.anime_slug)
        .unwrap_or_else(|| AnimeResult {
            slug: entry.anime_slug.clone(),
            title: entry.title.clone(),
            alt_title: None,
            url: String::new(),
            provider: entry.provider.unwrap_or(ProviderId::Animerco),
            year: None,
            episode_count: None,
            latency_ms: 0,
            sources: Vec::new(),
        });

    if matched.url.is_empty() {
        return Err(SenkuError::NoSearchResults(entry.title.clone()));
    }

    Ok((matched, Some(entry.episode), entry.position_secs))
}

fn pick_episodes_for_download(
    episodes: &[Episode],
    ep_spec: Option<&str>,
    no_fzf: bool,
) -> Result<Vec<Episode>> {
    if let Some(spec) = ep_spec {
        let parsed = EpisodeSelection::parse(spec)?;
        let mut chosen = Vec::with_capacity(parsed.episodes.len());
        for num in parsed.episodes {
            let ep = episodes
                .iter()
                .find(|e| e.number == num)
                .cloned()
                .ok_or_else(|| SenkuError::EpisodeNotFound {
                    requested: num,
                    available: format_available_range(episodes),
                })?;
            chosen.push(ep);
        }
        return Ok(chosen);
    }

    let idx = selector::select_episode(episodes, no_fzf)?;
    Ok(vec![episodes[idx].clone()])
}

fn resolve_stream_start_index(
    episodes: &[Episode],
    ep_spec: Option<&str>,
    resume_ep: Option<u32>,
    no_fzf: bool,
) -> Result<usize> {
    if let Some(spec) = ep_spec {
        let parsed = EpisodeSelection::parse(spec)?;
        let target = parsed.episodes[0];
        return episodes
            .iter()
            .position(|e| e.number == target)
            .ok_or_else(|| SenkuError::EpisodeNotFound {
                requested: target,
                available: format_available_range(episodes),
            });
    }

    if let Some(ep_num) = resume_ep {
        if let Some(pos) = episodes.iter().position(|e| e.number == ep_num) {
            return Ok(pos);
        }
    }

    selector::select_episode(episodes, no_fzf)
}

fn format_available_range(episodes: &[Episode]) -> String {
    match (episodes.first(), episodes.last()) {
        (Some(a), Some(b)) if a.number == b.number => format!("{}", a.number),
        (Some(a), Some(b)) => format!("{}-{}", a.number, b.number),
        _ => "none".to_string(),
    }
}

fn prompt_line(prompt: &str) -> Result<Option<String>> {
    eprint!("{} ", prompt.bright_cyan().bold());
    io::stderr().flush()?;
    let mut line = String::new();
    let bytes = io::stdin().lock().read_line(&mut line)?;
    if bytes == 0 {
        return Ok(None);
    }
    let trimmed = line.trim().to_string();
    if trimmed.is_empty() {
        Ok(None)
    } else {
        Ok(Some(trimmed))
    }
}

fn print_history(history: &HistoryStore) -> Result<()> {
    let entries = history.load_all()?;
    if entries.is_empty() {
        log_info(&format!(
            "No watch history yet ({}).",
            history.path().display()
        ));
        return Ok(());
    }

    eprintln!(
        "{} ({}):",
        "Watch History".bright_cyan().bold(),
        history.path().display()
    );
    for (idx, item) in entries.iter().enumerate() {
        let dt = DateTime::<Utc>::from_timestamp(item.timestamp, 0)
            .map(|d| d.format("%Y-%m-%d %H:%M UTC").to_string())
            .unwrap_or_else(|| item.timestamp.to_string());
        let prov = item.provider.map(|p| p.short_tag()).unwrap_or("?");
        let pos_part = item
            .position_secs
            .filter(|&s| s > 10)
            .map(|s| format!(" @ {:02}:{:02}", s / 60, s % 60))
            .unwrap_or_default();
        eprintln!(
            "  {:>2}. [{}] {} — Episode {:02}{} ({})",
            idx + 1,
            prov.bright_green(),
            item.title.bright_white().bold(),
            item.episode,
            pos_part.bright_cyan(),
            dt.dimmed()
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_zero_arg_launcher_invocation() {
        let zero = Cli::parse_from(["senku-anime-cli"]);
        assert!(zero.is_zero_arg_launcher());

        let with_query = Cli::parse_from(["senku-anime-cli", "One Piece"]);
        assert!(!with_query.is_zero_arg_launcher());

        let with_continue = Cli::parse_from(["senku-anime-cli", "-c"]);
        assert!(!with_continue.is_zero_arg_launcher());

        let with_history = Cli::parse_from(["senku-anime-cli", "--history"]);
        assert!(!with_history.is_zero_arg_launcher());

        let with_doctor = Cli::parse_from(["senku-anime-cli", "--doctor"]);
        assert!(!with_doctor.is_zero_arg_launcher());

        let with_comp = Cli::parse_from(["senku-anime-cli", "--completions", "zsh"]);
        assert!(!with_comp.is_zero_arg_launcher());

        let with_updates = Cli::parse_from(["senku-anime-cli", "--updates"]);
        assert!(!with_updates.is_zero_arg_launcher());

        let with_minimal = Cli::parse_from(["senku-anime-cli", "--minimal", "One Piece"]);
        assert!(with_minimal.minimal);
        assert!(!with_minimal.is_zero_arg_launcher());
    }

    #[test]
    fn generates_zsh_completions_for_both_binaries() {
        let zsh = cli::render_completions_to_string(clap_complete::Shell::Zsh);
        assert!(zsh.contains("#compdef senku-anime-cli"));
        assert!(zsh.contains("#compdef sac"));
        assert!(zsh.contains("--doctor"));
        assert!(zsh.contains("--auto-skip"));
        assert!(zsh.contains("--completions"));
        assert!(zsh.contains("--minimal"));
        assert!(zsh.contains("--updates"));
        assert!(zsh.contains("--skip-filler"));
    }

    #[test]
    fn resets_session_quality_on_new_anime_unless_explicitly_overridden() {
        let cli_default = Cli::parse_from(["senku-anime-cli", "Black Clover"]);
        let settings_default = AppConfig::default().resolve_settings(&cli_default);
        let (q, skip) = initial_quality_session_state(&cli_default, &settings_default);
        assert_eq!(q, None);
        assert!(
            !skip,
            "must present quality picker when -q/-b/auto_select_best are unset"
        );

        let cli_best = Cli::parse_from(["senku-anime-cli", "-b", "Black Clover"]);
        let settings_best = AppConfig::default().resolve_settings(&cli_best);
        let (_, skip_best) = initial_quality_session_state(&cli_best, &settings_best);
        assert!(skip_best);

        let cli_q = Cli::parse_from(["senku-anime-cli", "-q", "720p", "Black Clover"]);
        let settings_q = AppConfig::default().resolve_settings(&cli_q);
        let (q_res, skip_q) = initial_quality_session_state(&cli_q, &settings_q);
        assert_eq!(q_res, Some(Resolution::Hd720));
        assert!(skip_q);
    }
}
