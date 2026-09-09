use crate::state::State;
use crate::state_type;
use base64::Engine;
use recorder::platforms::PlatformType;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;

const TRANSITION_DURATION_SECONDS: f64 = 1.0;

#[cfg(feature = "gui")]
use tauri::State as TauriState;

#[derive(Debug, Serialize, Deserialize)]
pub struct VideoFrame {
    pub timestamp: f64,
    pub image_base64: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VideoMetadata {
    pub duration: f64,
    pub width: u32,
    pub height: u32,
    pub video_codec: String,
    pub audio_codec: String,
    pub bitrate: u64,
    pub fps: f64,
    pub file_size: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DanmuHighlight {
    pub start_time: f64,
    pub end_time: f64,
    pub comment_count: usize,
    pub density: f64,
    pub sample_comments: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DanmuKeywordMatch {
    pub timestamp: f64,
    pub content: String,
    pub keyword: String,
    pub context_start: f64,
    pub context_end: f64,
}

// Helper function to get ffmpeg path
fn get_ffmpeg_path() -> PathBuf {
    let mut path = Path::new("ffmpeg").to_path_buf();
    if cfg!(windows) {
        path.set_extension("exe");
    }
    path
}

// Helper function to get ffprobe path
fn get_ffprobe_path() -> PathBuf {
    let mut path = Path::new("ffprobe").to_path_buf();
    if cfg!(windows) {
        path.set_extension("exe");
    }
    path
}

fn resolve_video_path(output_dir: &Path, file: &str) -> PathBuf {
    let path = PathBuf::from(file);
    if path.is_absolute() {
        path
    } else {
        output_dir.join(path)
    }
}

fn build_transition_filter(durations: &[f64], transition_type: &str) -> Result<String, String> {
    if durations.len() < 2 {
        return Err("At least two videos are required for a transition".to_string());
    }
    if durations
        .iter()
        .any(|duration| *duration <= TRANSITION_DURATION_SECONDS)
    {
        return Err("Every video must be longer than the transition duration".to_string());
    }
    let xfade_transition = match transition_type {
        "fade" => "fade",
        "dissolve" => "dissolve",
        "wipeleft" => "wipeleft",
        "wiperight" => "wiperight",
        "slideup" => "slideup",
        "slidedown" => "slidedown",
        _ => return Err(format!("Unsupported transition: {transition_type}")),
    };

    let mut filters = Vec::new();
    for i in 0..(durations.len() - 1) {
        let first_video = if i == 0 {
            "[0:v]".to_string()
        } else {
            format!("[v{i}]")
        };
        let first_audio = if i == 0 {
            "[0:a]".to_string()
        } else {
            format!("[a{i}]")
        };
        let is_last = i == durations.len() - 2;
        let video_output = if is_last {
            "[outv]".to_string()
        } else {
            format!("[v{}]", i + 1)
        };
        let audio_output = if is_last {
            "[outa]".to_string()
        } else {
            format!("[a{}]", i + 1)
        };
        let offset = durations.iter().take(i + 1).sum::<f64>()
            - (i as f64 + 1.0) * TRANSITION_DURATION_SECONDS;
        filters.push(format!(
            "{first_video}[{}:v]xfade=transition={xfade_transition}:duration={TRANSITION_DURATION_SECONDS}:offset={offset}{video_output}",
            i + 1
        ));
        filters.push(format!(
            "{first_audio}[{}:a]acrossfade=d={TRANSITION_DURATION_SECONDS}{audio_output}",
            i + 1
        ));
    }
    Ok(filters.join(";"))
}

/// Extract frames from a video at specific timestamps or evenly distributed
#[cfg_attr(feature = "gui", tauri::command)]
pub async fn extract_video_frames(
    state: state_type!(),
    video_id: i64,
    timestamps: Vec<f64>,
    max_frames: usize,
) -> Result<Vec<VideoFrame>, String> {
    if timestamps
        .iter()
        .any(|timestamp| !timestamp.is_finite() || *timestamp < 0.0)
    {
        return Err("timestamps must contain only non-negative finite numbers".to_string());
    }

    // Get video info
    let video = state
        .db
        .get_video(video_id)
        .await
        .map_err(|e| format!("Failed to get video: {}", e))?;

    let output_dir = PathBuf::from(&state.config.read().await.output);
    let video_path = resolve_video_path(&output_dir, &video.file);
    if !video_path.exists() {
        return Err(format!("Video file not found: {}", video_path.display()));
    }

    // Get video duration
    let metadata = get_video_metadata_internal(&video_path).await?;
    if !metadata.duration.is_finite() || metadata.duration <= 0.0 {
        return Err("Video duration is unavailable or invalid".to_string());
    }

    // Determine timestamps to extract
    let extract_timestamps = if timestamps.is_empty() {
        // Evenly distribute frames
        let count = max_frames.clamp(1, 10);
        (0..count)
            .map(|i| (i as f64 / count as f64) * metadata.duration)
            .collect::<Vec<_>>()
    } else {
        timestamps
            .into_iter()
            .take(max_frames.clamp(1, 10))
            .collect()
    };

    let mut frames = Vec::new();

    for ts in extract_timestamps {
        if ts >= metadata.duration {
            continue;
        }

        // Extract frame using ffmpeg
        let frame_data = extract_frame_at_timestamp(&video_path, ts).await?;
        frames.push(VideoFrame {
            timestamp: ts,
            image_base64: frame_data,
        });
    }

    Ok(frames)
}

/// Extract a single frame at a specific timestamp
async fn extract_frame_at_timestamp(video_path: &Path, timestamp: f64) -> Result<String, String> {
    let output_path = std::env::temp_dir().join(format!(
        "bsr_frame_{}_{}.jpg",
        timestamp,
        uuid::Uuid::new_v4()
    ));

    let ffmpeg_path = get_ffmpeg_path();
    let mut cmd = tokio::process::Command::new(ffmpeg_path);

    #[cfg(target_os = "windows")]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x08000000;
        cmd.creation_flags(CREATE_NO_WINDOW);
    }

    cmd.arg("-ss")
        .arg(timestamp.to_string())
        .arg("-i")
        .arg(video_path)
        .args(["-vframes", "1", "-q:v", "2", "-y"])
        .arg(&output_path);

    let output = match cmd.output().await {
        Ok(output) => output,
        Err(error) => {
            let _ = tokio::fs::remove_file(&output_path).await;
            return Err(format!("Failed to run ffmpeg: {error}"));
        }
    };

    if !output.status.success() {
        let _ = tokio::fs::remove_file(&output_path).await;
        return Err(format!(
            "FFmpeg failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    // Read and encode to base64
    let image_data = tokio::fs::read(&output_path).await;
    let _ = tokio::fs::remove_file(&output_path).await;
    let image_data = image_data.map_err(|e| format!("Failed to read frame: {}", e))?;

    let base64_data = base64::engine::general_purpose::STANDARD.encode(&image_data);

    Ok(base64_data)
}

/// Get detailed video metadata
#[cfg_attr(feature = "gui", tauri::command)]
pub async fn get_video_metadata(
    state: state_type!(),
    video_id: i64,
) -> Result<VideoMetadata, String> {
    let video = state
        .db
        .get_video(video_id)
        .await
        .map_err(|e| format!("Failed to get video: {}", e))?;

    let output_dir = PathBuf::from(&state.config.read().await.output);
    let video_path = resolve_video_path(&output_dir, &video.file);
    get_video_metadata_internal(&video_path).await
}

/// Internal function to get video metadata
async fn get_video_metadata_internal(video_path: &Path) -> Result<VideoMetadata, String> {
    if !video_path.exists() {
        return Err("Video file not found".to_string());
    }

    let ffprobe_path = get_ffprobe_path();
    let mut cmd = tokio::process::Command::new(ffprobe_path);

    #[cfg(target_os = "windows")]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x08000000;
        cmd.creation_flags(CREATE_NO_WINDOW);
    }

    cmd.args([
        "-v",
        "quiet",
        "-print_format",
        "json",
        "-show_format",
        "-show_streams",
    ])
    .arg(video_path);

    let output = cmd
        .output()
        .await
        .map_err(|e| format!("Failed to run ffprobe: {}", e))?;

    if !output.status.success() {
        return Err("FFprobe failed".to_string());
    }

    let json_str = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&json_str)
        .map_err(|e| format!("Failed to parse ffprobe output: {}", e))?;

    // Extract metadata
    let format = json.get("format").ok_or("No format info")?;
    let streams = json
        .get("streams")
        .and_then(|s| s.as_array())
        .ok_or("No streams")?;

    let duration = format
        .get("duration")
        .and_then(|d| d.as_str())
        .and_then(|d| d.parse::<f64>().ok())
        .filter(|duration| duration.is_finite() && *duration >= 0.0)
        .unwrap_or(0.0);

    let file_size = format
        .get("size")
        .and_then(|s| s.as_str())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);

    let bitrate = format
        .get("bit_rate")
        .and_then(|b| b.as_str())
        .and_then(|b| b.parse::<u64>().ok())
        .unwrap_or(0);

    // Find video stream
    let video_stream = streams
        .iter()
        .find(|s| s.get("codec_type").and_then(|t| t.as_str()) == Some("video"))
        .ok_or("No video stream found")?;

    let width = video_stream
        .get("width")
        .and_then(|w| w.as_u64())
        .unwrap_or(0) as u32;
    let height = video_stream
        .get("height")
        .and_then(|h| h.as_u64())
        .unwrap_or(0) as u32;
    let video_codec = video_stream
        .get("codec_name")
        .and_then(|c| c.as_str())
        .unwrap_or("unknown")
        .to_string();

    // Calculate FPS
    let fps = video_stream
        .get("r_frame_rate")
        .and_then(|r| r.as_str())
        .and_then(|r| {
            let parts: Vec<&str> = r.split('/').collect();
            if parts.len() == 2 {
                let num = parts[0].parse::<f64>().ok()?;
                let den = parts[1].parse::<f64>().ok()?;
                let fps = num / den;
                (den != 0.0 && fps.is_finite() && fps >= 0.0).then_some(fps)
            } else {
                None
            }
        })
        .unwrap_or(0.0);

    // Find audio stream
    let audio_stream = streams
        .iter()
        .find(|s| s.get("codec_type").and_then(|t| t.as_str()) == Some("audio"));

    let audio_codec = audio_stream
        .and_then(|s| s.get("codec_name"))
        .and_then(|c| c.as_str())
        .unwrap_or("none")
        .to_string();

    Ok(VideoMetadata {
        duration,
        width,
        height,
        video_codec,
        audio_codec,
        bitrate,
        fps,
        file_size,
    })
}

/// Analyze danmu to find highlight moments based on comment density
#[cfg_attr(feature = "gui", tauri::command)]
pub async fn analyze_danmu_highlights(
    state: state_type!(),
    platform: String,
    room_id: String,
    live_id: String,
    time_window: f64,
    min_density: usize,
) -> Result<Vec<DanmuHighlight>, String> {
    if !time_window.is_finite() || time_window < 0.001 {
        return Err("time_window must be at least 0.001 seconds".to_string());
    }
    // Get danmu records using recorder_manager
    let platform_type = PlatformType::from_str(&platform)?;
    let danmu_records = state
        .recorder_manager
        .load_relative_danmus(platform_type, &room_id, &live_id)
        .await
        .map_err(|e| format!("Failed to get danmu: {}", e))?;

    if danmu_records.is_empty() {
        return Ok(Vec::new());
    }

    let mut windows = BTreeMap::<usize, Vec<&recorder::danmu::DanmuEntry>>::new();
    for record in &danmu_records {
        let timestamp = record.ts as f64 / 1000.0;
        let index = (timestamp / time_window).floor() as usize;
        windows.entry(index).or_default().push(record);
    }
    let mut highlights = Vec::new();
    for (index, comments_in_window) in windows {
        let count = comments_in_window.len();
        if count >= min_density {
            let sample_comments: Vec<String> = comments_in_window
                .iter()
                .take(5)
                .map(|d| d.content.clone())
                .collect();
            let start_time = index as f64 * time_window;
            highlights.push(DanmuHighlight {
                start_time,
                end_time: start_time + time_window,
                comment_count: count,
                density: count as f64 / time_window,
                sample_comments,
            });
        }
    }

    Ok(highlights)
}

/// Search for specific keywords in danmu
#[cfg_attr(feature = "gui", tauri::command)]
pub async fn search_danmu_keywords(
    state: state_type!(),
    platform: String,
    room_id: String,
    live_id: String,
    keywords: Vec<String>,
    context_seconds: f64,
) -> Result<Vec<DanmuKeywordMatch>, String> {
    if !context_seconds.is_finite() || context_seconds < 0.0 {
        return Err("context_seconds must be a non-negative finite number".to_string());
    }
    let keywords = keywords
        .into_iter()
        .map(|keyword| keyword.trim().to_string())
        .filter(|keyword| !keyword.is_empty())
        .collect::<Vec<_>>();
    if keywords.is_empty() {
        return Err("keywords must contain at least one non-empty value".to_string());
    }
    let platform_type = PlatformType::from_str(&platform)?;
    let danmu_records = state
        .recorder_manager
        .load_relative_danmus(platform_type, &room_id, &live_id)
        .await
        .map_err(|e| format!("Failed to get danmu: {}", e))?;

    let mut matches = Vec::new();

    for record in danmu_records {
        for keyword in &keywords {
            if record.content.contains(keyword) {
                let timestamp = record.ts as f64 / 1000.0;
                let context_start = (timestamp - context_seconds).max(0.0);
                let context_end = (timestamp + context_seconds).min(f64::MAX);

                matches.push(DanmuKeywordMatch {
                    timestamp,
                    content: record.content.clone(),
                    keyword: keyword.clone(),
                    context_start,
                    context_end,
                });
                break; // Only match once per comment
            }
        }
    }

    Ok(matches)
}

/// Merge multiple videos into one
#[cfg_attr(feature = "gui", tauri::command)]
pub async fn merge_videos(
    state: state_type!(),
    video_ids: Vec<i64>,
    output_title: String,
    output_note: String,
    transition: Option<String>,
) -> Result<i64, String> {
    if video_ids.is_empty() {
        return Err("No videos to merge".to_string());
    }

    // Get all videos
    let mut videos = Vec::new();
    for id in &video_ids {
        let video = state
            .db
            .get_video(*id)
            .await
            .map_err(|e| format!("Failed to get video {}: {}", id, e))?;
        videos.push(video);
    }

    // Determine output path
    let output_dir = PathBuf::from(&state.config.read().await.output);
    tokio::fs::create_dir_all(&output_dir)
        .await
        .map_err(|error| format!("Failed to create output directory: {error}"))?;
    let video_paths = videos
        .iter()
        .map(|video| resolve_video_path(&output_dir, &video.file))
        .collect::<Vec<_>>();
    let output_filename = format!(
        "merged_{}_{}.mp4",
        chrono::Local::now().format("%Y%m%d_%H%M%S"),
        uuid::Uuid::new_v4()
    );
    let output_path = output_dir.join(&output_filename);

    let ffmpeg_path = get_ffmpeg_path();
    let transition_type = transition.as_deref().unwrap_or("none");

    // If no transition or only one video, use simple concat
    if transition_type == "none" || videos.len() == 1 {
        // Create concat file list
        let concat_file = std::env::temp_dir().join(format!("concat_{}.txt", uuid::Uuid::new_v4()));
        let mut concat_content = String::new();

        for video_path in &video_paths {
            // Escape path for FFmpeg concat demuxer
            // Only escape backslashes and single quotes
            // Square brackets work fine inside single quotes
            let path_str = video_path
                .to_string_lossy()
                .replace('\\', "\\\\")
                .replace('\'', "'\\''");
            concat_content.push_str(&format!("file '{}'\n", path_str));
        }

        tokio::fs::write(&concat_file, concat_content)
            .await
            .map_err(|e| format!("Failed to write concat file: {}", e))?;

        // Run ffmpeg concat
        let mut cmd = tokio::process::Command::new(ffmpeg_path);

        #[cfg(target_os = "windows")]
        {
            use std::os::windows::process::CommandExt;
            const CREATE_NO_WINDOW: u32 = 0x08000000;
            cmd.creation_flags(CREATE_NO_WINDOW);
        }

        cmd.args(["-f", "concat", "-safe", "0", "-i"])
            .arg(&concat_file)
            .args(["-c", "copy", "-y"])
            .arg(&output_path);

        let output = cmd.output().await;
        let _ = tokio::fs::remove_file(&concat_file).await;
        let output = match output {
            Ok(output) => output,
            Err(error) => {
                let _ = tokio::fs::remove_file(&output_path).await;
                return Err(format!("Failed to run ffmpeg: {error}"));
            }
        };

        if !output.status.success() {
            let _ = tokio::fs::remove_file(&output_path).await;
            return Err(format!(
                "FFmpeg merge failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
    } else {
        // Use xfade filter for transitions
        let durations = videos
            .iter()
            .map(|video| video.length as f64)
            .collect::<Vec<_>>();
        let filter_complex = build_transition_filter(&durations, transition_type)?;

        // Build ffmpeg command with multiple inputs
        let mut cmd = tokio::process::Command::new(&ffmpeg_path);

        #[cfg(target_os = "windows")]
        {
            use std::os::windows::process::CommandExt;
            const CREATE_NO_WINDOW: u32 = 0x08000000;
            cmd.creation_flags(CREATE_NO_WINDOW);
        }

        // Add all input files
        for video_path in &video_paths {
            cmd.arg("-i").arg(video_path);
        }

        // Add filter complex
        cmd.args([
            "-filter_complex",
            &filter_complex,
            "-map",
            "[outv]",
            "-map",
            "[outa]",
            "-c:v",
            "libx264",
            "-preset",
            "medium",
            "-crf",
            "23",
            "-c:a",
            "aac",
            "-y",
        ])
        .arg(&output_path);

        let output = match cmd.output().await {
            Ok(output) => output,
            Err(error) => {
                let _ = tokio::fs::remove_file(&output_path).await;
                return Err(format!("Failed to run ffmpeg: {error}"));
            }
        };

        if !output.status.success() {
            let _ = tokio::fs::remove_file(&output_path).await;
            return Err(format!(
                "FFmpeg merge with transition failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
    }

    // Get file size
    let file_size = match tokio::fs::metadata(&output_path).await {
        Ok(metadata) => metadata.len(),
        Err(error) => {
            let _ = tokio::fs::remove_file(&output_path).await;
            return Err(format!("Failed to get file size: {error}"));
        }
    };

    // Calculate total duration
    let transition_overlap = if transition_type == "none" || videos.len() == 1 {
        0
    } else {
        ((videos.len() - 1) as f64 * TRANSITION_DURATION_SECONDS) as i64
    };
    let total_duration = videos.iter().map(|video| video.length).sum::<i64>() - transition_overlap;
    let cover_filename = Path::new(&output_filename)
        .with_extension("jpg")
        .to_string_lossy()
        .to_string();
    let cover = if crate::ffmpeg::generate_thumbnail(&output_path, 0.0)
        .await
        .is_ok()
    {
        cover_filename
    } else {
        let _ = tokio::fs::remove_file(output_path.with_extension("jpg")).await;
        String::new()
    };

    // Create new video row
    let new_video = crate::database::video::VideoRow {
        id: 0, // Will be auto-generated
        room_id: videos[0].room_id.clone(),
        cover,
        file: output_filename,
        note: output_note.clone(),
        length: total_duration,
        size: file_size as i64,
        status: 0,
        bvid: String::new(),
        title: output_title.clone(),
        desc: String::new(),
        tags: String::new(),
        area: 0,
        created_at: chrono::Utc::now().to_rfc3339(),
        platform: videos[0].platform.clone(),
    };

    // Insert into database
    let result = match state.db.add_video(&new_video).await {
        Ok(result) => result,
        Err(error) => {
            let _ = tokio::fs::remove_file(&output_path).await;
            if !new_video.cover.is_empty() {
                let _ = tokio::fs::remove_file(output_dir.join(&new_video.cover)).await;
            }
            return Err(format!("Failed to insert video: {error}"));
        }
    };

    Ok(result.id)
}

/// Extract audio from video
#[cfg_attr(feature = "gui", tauri::command)]
pub async fn extract_video_audio(state: state_type!(), video_id: i64) -> Result<String, String> {
    let video = state
        .db
        .get_video(video_id)
        .await
        .map_err(|e| format!("Failed to get video: {}", e))?;

    let output_dir = PathBuf::from(&state.config.read().await.output);
    let video_path = resolve_video_path(&output_dir, &video.file);
    if !video_path.exists() {
        return Err(format!("Video file not found: {}", video_path.display()));
    }

    // Determine output path
    let output_filename = format!(
        "audio_{}_{}_{}.mp3",
        video.id,
        chrono::Local::now().format("%Y%m%d_%H%M%S"),
        uuid::Uuid::new_v4()
    );
    let output_path = output_dir.join(&output_filename);
    tokio::fs::create_dir_all(&output_dir)
        .await
        .map_err(|error| format!("Failed to create output directory: {error}"))?;

    // Extract audio using ffmpeg
    let ffmpeg_path = get_ffmpeg_path();
    let mut cmd = tokio::process::Command::new(ffmpeg_path);

    #[cfg(target_os = "windows")]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x08000000;
        cmd.creation_flags(CREATE_NO_WINDOW);
    }

    cmd.arg("-i")
        .arg(&video_path)
        .args(["-vn", "-acodec", "libmp3lame", "-q:a", "2", "-y"])
        .arg(&output_path);

    let output = match cmd.output().await {
        Ok(output) => output,
        Err(error) => {
            let _ = tokio::fs::remove_file(&output_path).await;
            return Err(format!("Failed to run ffmpeg: {error}"));
        }
    };

    if !output.status.success() {
        let _ = tokio::fs::remove_file(&output_path).await;
        return Err(format!(
            "FFmpeg audio extraction failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    Ok(output_path.to_string_lossy().to_string())
}

/// Get archive metadata
#[cfg_attr(feature = "gui", tauri::command)]
pub async fn get_archive_metadata(
    state: state_type!(),
    platform: String,
    room_id: String,
    live_id: String,
) -> Result<serde_json::Value, String> {
    let platform = PlatformType::from_str(&platform)?;
    // Use get_record instead of get_archive
    let archive = state
        .db
        .get_record(&room_id, &live_id)
        .await
        .map_err(|e| format!("Failed to get archive: {}", e))?;
    if archive.platform != platform.as_str() {
        return Err("Archive platform does not match the request".to_string());
    }

    // Recordings are HLS archives. There is no `output.mp4` until the user
    // creates a clip, so inspect the actual media playlist here.
    let cache_dir = PathBuf::from(&state.config.read().await.cache);
    let file_path = cache_dir
        .join(platform.as_str())
        .join(&archive.room_id)
        .join(&archive.live_id)
        .join("playlist.m3u8");

    let (file_size, video_metadata) = if file_path.exists() {
        let metadata = get_video_metadata_internal(&file_path).await.ok();
        (u64::try_from(archive.size).unwrap_or_default(), metadata)
    } else {
        (0, None)
    };

    Ok(serde_json::json!({
        "live_id": archive.live_id,
        "room_id": archive.room_id,
        "platform": platform.as_str(),
        "title": archive.title,
        "file_path": file_path.to_string_lossy(),
        "file_size": file_size,
        "created_at": archive.created_at,
        "video_metadata": video_metadata,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relative_video_paths_are_resolved_from_output_directory() {
        let output_dir = Path::new("/configured/output");
        assert_eq!(
            resolve_video_path(output_dir, "clip.mp4"),
            output_dir.join("clip.mp4")
        );
    }

    #[test]
    fn absolute_video_paths_are_preserved() {
        let video_path = std::env::temp_dir().join("clip.mp4");
        assert_eq!(
            resolve_video_path(
                Path::new("/configured/output"),
                video_path.to_str().unwrap()
            ),
            video_path
        );
    }

    #[test]
    fn transition_filter_maps_final_video_and_audio_outputs() {
        assert_eq!(
            build_transition_filter(&[10.0, 20.0], "fade").unwrap(),
            "[0:v][1:v]xfade=transition=fade:duration=1:offset=9[outv];[0:a][1:a]acrossfade=d=1[outa]"
        );
        assert_eq!(
            build_transition_filter(&[10.0, 20.0, 30.0], "dissolve").unwrap(),
            "[0:v][1:v]xfade=transition=dissolve:duration=1:offset=9[v1];[0:a][1:a]acrossfade=d=1[a1];[v1][2:v]xfade=transition=dissolve:duration=1:offset=28[outv];[a1][2:a]acrossfade=d=1[outa]"
        );
        assert!(build_transition_filter(&[10.0, 20.0], "unknown").is_err());
    }
}
