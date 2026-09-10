use std::path::{Path, PathBuf};

use m3u8_rs::{Map, MediaPlaylist};
use tokio::io::AsyncWriteExt;

use crate::progress::progress_reporter::ProgressReporterTrait;

#[cfg(target_os = "windows")]
use crate::ffmpeg::CREATE_NO_WINDOW;
#[cfg(target_os = "windows")]
#[allow(unused_imports)]
use std::os::windows::process::CommandExt;

use super::Range;

pub async fn clip_multiple_from_playlist(
    reporter: Option<&impl ProgressReporterTrait>,
    playlist_path: &Path,
    output_path: &Path,
    ranges: &[Range],
    transition: Option<&str>,
) -> Result<(), String> {
    let mut to_remove = Vec::new();
    for (i, range) in ranges.iter().enumerate() {
        let video_path = output_path.with_extension(format!("{}.mp4", i));
        if let Err(e) =
            clip_from_playlist(reporter, playlist_path, &video_path, Some(range.clone())).await
        {
            log::error!("Failed to generate playlist video: {e}");
            // clean up to_remove
            for path in to_remove {
                let _ = tokio::fs::remove_file(path).await;
            }
            return Err(e);
        }
        to_remove.push(video_path.clone());
    }
    super::general::concat_videos_with_transition(reporter, &to_remove, output_path, transition)
        .await?;
    // clean up to_remove
    for path in to_remove {
        let _ = tokio::fs::remove_file(path).await;
    }
    Ok(())
}

pub async fn clip_from_playlist(
    reporter: Option<&impl ProgressReporterTrait>,
    playlist_path: &Path,
    output_path: &Path,
    range: Option<Range>,
) -> Result<(), String> {
    let playlist_bytes = tokio::fs::read(playlist_path)
        .await
        .map_err(|e| format!("Failed to read playlist '{}': {e}", playlist_path.display()))?;
    let playlist = parse_media_playlist(&playlist_bytes, playlist_path)?;
    let mut start_offset = None;
    let mut segments = Vec::new();
    if let Some(range) = &range {
        let mut duration = 0.0;
        for s in playlist.segments.clone() {
            if range.is_in(duration) || range.is_in(duration + s.duration as f64) {
                segments.push(s.clone());
                if start_offset.is_none() {
                    start_offset = Some(range.start - duration);
                }
            }
            duration += s.duration as f64;
        }
    } else {
        segments = playlist.segments.clone();
    }

    if segments.is_empty() {
        return Err("No segments found".to_string());
    }

    let first_segment = playlist
        .segments
        .first()
        .ok_or_else(|| "Playlist contains no segments".to_string())?;
    let mut header_url = first_segment
        .unknown_tags
        .iter()
        .find(|t| t.tag == "X-MAP")
        .and_then(|tag| tag.rest.as_deref())
        .and_then(parse_map_uri);
    if header_url.is_none() {
        // map: Some(Map { uri: "h1758725308.m4s"
        if let Some(Map { uri, .. }) = &first_segment.map {
            header_url = Some(uri.clone());
        }
    }

    // write all segments to clip_file
    {
        let playlist_folder = playlist_path.parent().unwrap_or_else(|| Path::new("."));
        let output_folder = output_path.parent().unwrap_or_else(|| Path::new("."));
        if !output_folder.exists() {
            std::fs::create_dir_all(output_folder).map_err(|e| {
                format!(
                    "Failed to create output folder '{}': {e}",
                    output_folder.display()
                )
            })?;
        }
        let mut file = tokio::fs::File::create(&output_path)
            .await
            .map_err(|e| format!("Failed to create output file: {}", e))?;
        if let Some(header_url) = header_url {
            let header_data = tokio::fs::read(playlist_folder.join(header_url))
                .await
                .map_err(|e| format!("Failed to read header file: {}", e))?;
            file.write_all(&header_data)
                .await
                .map_err(|e| format!("Failed to write header file: {}", e))?;
        }
        for s in segments {
            // read segment
            let uri = s.uri.split('?').next().unwrap_or(&s.uri);
            let segment_file_path = playlist_folder.join(uri);
            let segment_data = tokio::fs::read(&segment_file_path)
                .await
                .map_err(|e| format!("Failed to read segment file: {}", e))?;
            // append segment data to clip_file
            file.write_all(&segment_data)
                .await
                .map_err(|e| format!("Failed to write segment file: {}", e))?;
        }
        file.flush()
            .await
            .map_err(|e| format!("Failed to flush file: {}", e))?;
    }

    // transcode copy to fix timestamp
    {
        let tmp_output_path = output_path.with_extension("tmp.mp4");
        if let Err(error) = super::transcode(reporter, output_path, &tmp_output_path, true).await {
            let _ = tokio::fs::remove_file(&tmp_output_path).await;
            return Err(error);
        }

        replace_output_file(&tmp_output_path, output_path, "timestamp repair").await?;
    }

    // trim for precised duration
    if let (Some(start_offset), Some(range)) = (start_offset, range.as_ref()) {
        let tmp_output_path = output_path.with_extension("tmp.mp4");
        if let Err(error) = super::trim_video(
            reporter,
            output_path,
            &tmp_output_path,
            start_offset,
            range.duration(),
        )
        .await
        {
            let _ = tokio::fs::remove_file(&tmp_output_path).await;
            return Err(error);
        }

        replace_output_file(&tmp_output_path, output_path, "precise trim").await?;
    }

    Ok(())
}

fn parse_media_playlist(bytes: &[u8], playlist_path: &Path) -> Result<MediaPlaylist, String> {
    m3u8_rs::parse_media_playlist(bytes)
        .map(|(_, playlist)| playlist)
        .map_err(|_| {
            let input_context = if bytes.is_empty() {
                "input is empty"
            } else if bytes.iter().all(|byte| *byte == 0) {
                "input is zero-filled"
            } else {
                "invalid playlist syntax"
            };
            format!(
                "Failed to parse media playlist '{}': {input_context} ({} bytes)",
                playlist_path.display(),
                bytes.len()
            )
        })
}

fn parse_map_uri(rest: &str) -> Option<String> {
    rest.split_once('=').and_then(|(_, value)| {
        let unescaped = value.trim().replace("\\\"", "\"");
        let uri = unescaped.trim_matches('"');
        (!uri.is_empty()).then(|| uri.to_string())
    })
}

async fn replace_output_file(
    temporary_path: &Path,
    output_path: &Path,
    operation: &str,
) -> Result<(), String> {
    let processed_metadata = tokio::fs::metadata(temporary_path).await.map_err(|error| {
        format!(
            "Processed output '{}' is unavailable after {operation}: {error}",
            temporary_path.display()
        )
    })?;
    if processed_metadata.len() == 0 {
        return Err(format!(
            "Processed output '{}' is empty after {operation}",
            temporary_path.display()
        ));
    }

    tokio::fs::remove_file(output_path).await.map_err(|error| {
        format!(
            "Failed to remove intermediate output '{}' after {operation}: {error}",
            output_path.display()
        )
    })?;
    tokio::fs::rename(temporary_path, output_path)
        .await
        .map_err(|error| {
            format!(
                "Failed to install processed output '{}' after {operation}: {error}",
                output_path.display()
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_zero_filled_playlist_without_panicking() {
        let path = Path::new("recordings/playlist.m3u8");
        let result = parse_media_playlist(&vec![0; 1024], path);

        assert_eq!(
            result.unwrap_err(),
            "Failed to parse media playlist 'recordings/playlist.m3u8': input is zero-filled (1024 bytes)"
        );
    }

    #[test]
    fn reports_empty_playlist_without_exposing_content() {
        let result = parse_media_playlist(&[], Path::new("empty.m3u8"));

        assert_eq!(
            result.unwrap_err(),
            "Failed to parse media playlist 'empty.m3u8': input is empty (0 bytes)"
        );
    }

    #[test]
    fn reports_invalid_playlist_without_exposing_content() {
        let result = parse_media_playlist(
            b"sensitive invalid playlist content",
            Path::new("invalid.m3u8"),
        );

        let error = result.unwrap_err();
        assert_eq!(
            error,
            "Failed to parse media playlist 'invalid.m3u8': invalid playlist syntax (34 bytes)"
        );
        assert!(!error.contains("sensitive"));
    }

    #[test]
    fn parses_map_uri() {
        assert_eq!(
            parse_map_uri(r#"URI=\"header.m4s\""#),
            Some("header.m4s".to_string())
        );
        assert_eq!(parse_map_uri("malformed"), None);
    }

    #[tokio::test]
    async fn replacing_a_processed_segment_is_checked() {
        let base = std::env::temp_dir().join(format!(
            "bili-shadowreplay-replace-{}",
            uuid::Uuid::new_v4()
        ));
        let output = base.with_extension("mp4");
        let temporary = base.with_extension("tmp.mp4");
        tokio::fs::write(&output, b"old").await.unwrap();
        tokio::fs::write(&temporary, b"processed").await.unwrap();

        replace_output_file(&temporary, &output, "test")
            .await
            .unwrap();

        assert_eq!(tokio::fs::read(&output).await.unwrap(), b"processed");
        assert!(!temporary.exists());
        tokio::fs::remove_file(output).await.unwrap();
    }

    #[tokio::test]
    async fn missing_processed_segment_is_reported_without_removing_the_original() {
        let base = std::env::temp_dir().join(format!(
            "bili-shadowreplay-missing-processed-{}",
            uuid::Uuid::new_v4()
        ));
        let output = base.with_extension("mp4");
        let missing_temporary = base.with_extension("tmp.mp4");
        tokio::fs::write(&output, b"original").await.unwrap();

        let error = replace_output_file(&missing_temporary, &output, "test")
            .await
            .unwrap_err();

        assert!(error.contains("is unavailable after test"));
        assert_eq!(tokio::fs::read(&output).await.unwrap(), b"original");
        tokio::fs::remove_file(output).await.unwrap();
    }
}

pub async fn concat_playlists_to_video(
    reporter: Option<&impl ProgressReporterTrait>,
    playlists: &[&Path],
    danmu_ass_files: Vec<Option<PathBuf>>,
    output_path: &Path,
) -> Result<(), String> {
    if playlists.is_empty() {
        return Err("No playlists to concatenate".to_string());
    }
    if playlists.len() != danmu_ass_files.len() {
        return Err(format!(
            "Playlist/danmu file count mismatch: {} playlists, {} danmu files",
            playlists.len(),
            danmu_ass_files.len()
        ));
    }

    let mut to_remove = Vec::new();
    let mut segments = Vec::new();
    for (i, playlist) in playlists.iter().enumerate() {
        let mut video_path = output_path.with_extension(format!("{}.mp4", i));
        // Add the target before processing so a partially written segment is
        // also removed when playlist extraction fails.
        to_remove.push(video_path.clone());
        if let Err(error) = require_playlist_segment(
            clip_from_playlist(reporter, playlist, &video_path, None).await,
            i,
            playlists.len(),
            playlist,
        ) {
            log::error!("{error}");
            remove_temporary_segments(&to_remove).await;
            return Err(error);
        }

        if let Some(danmu_ass_file) = &danmu_ass_files[i] {
            let expected_encoded_path = video_path.with_file_name(format!(
                "{}{}",
                crate::constants::PREFIX_DANMAKU,
                video_path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .ok_or_else(|| {
                        format!("Invalid playlist segment path: {}", video_path.display())
                    })?
            ));
            // Track the output before ffmpeg starts so a failed encode cannot
            // leave a partial [danmaku] file behind.
            to_remove.push(expected_encoded_path.clone());
            video_path =
                match super::encode_video_danmu(reporter, &video_path, danmu_ass_file).await {
                    Ok(encoded_path) => encoded_path,
                    Err(error) => {
                        let error = format!(
                            "Failed to burn danmu for playlist segment {}/{} ('{}'): {error}",
                            i + 1,
                            playlists.len(),
                            playlist.display()
                        );
                        log::error!("{error}");
                        remove_temporary_segments(&to_remove).await;
                        return Err(error);
                    }
                };
            if video_path != expected_encoded_path {
                to_remove.push(video_path.clone());
            }
        }
        segments.push(video_path);
    }

    let concat_result = super::general::concat_videos(reporter, &segments, output_path)
        .await
        .map_err(|error| {
            format!(
                "Failed to concatenate all {} playlist segments: {error}",
                playlists.len()
            )
        });
    remove_temporary_segments(&to_remove).await;

    concat_result
}

fn require_playlist_segment<T>(
    result: Result<T, String>,
    index: usize,
    total: usize,
    playlist: &Path,
) -> Result<T, String> {
    result.map_err(|error| {
        format!(
            "Failed to generate playlist segment {}/{} from '{}': {error}",
            index + 1,
            total,
            playlist.display()
        )
    })
}

async fn remove_temporary_segments(paths: &[PathBuf]) {
    for path in paths {
        let _ = tokio::fs::remove_file(path).await;
    }
}

#[cfg(test)]
mod concat_tests {
    use super::*;

    #[test]
    fn playlist_segment_failure_remains_fatal() {
        let result = require_playlist_segment::<()>(
            Err("segment file is missing".to_string()),
            1,
            3,
            Path::new("douyin/room/live-2/playlist.m3u8"),
        );

        let error = result.unwrap_err();
        assert!(error.contains("segment 2/3"));
        assert!(error.contains("douyin/room/live-2/playlist.m3u8"));
        assert!(error.contains("segment file is missing"));
    }

    #[tokio::test]
    async fn rejects_missing_playlist_or_danmu_entries_before_processing() {
        let playlist = Path::new("unused.m3u8");
        let result = concat_playlists_to_video(
            None::<&crate::progress::progress_reporter::ProgressReporter>,
            &[playlist],
            Vec::new(),
            Path::new("unused.mp4"),
        )
        .await;

        assert_eq!(
            result.unwrap_err(),
            "Playlist/danmu file count mismatch: 1 playlists, 0 danmu files"
        );
    }

    #[tokio::test]
    async fn concat_stops_at_a_missing_playlist_segment() {
        let missing_playlist = std::env::temp_dir().join(format!(
            "bili-shadowreplay-missing-{}.m3u8",
            uuid::Uuid::new_v4()
        ));
        let output = missing_playlist.with_extension("mp4");
        let playlists = [missing_playlist.as_path()];

        let result = concat_playlists_to_video(
            None::<&crate::progress::progress_reporter::ProgressReporter>,
            &playlists,
            vec![None],
            &output,
        )
        .await;

        let error = result.unwrap_err();
        let missing_playlist = missing_playlist.display().to_string();
        assert!(error.contains("segment 1/1"));
        assert!(error.contains(missing_playlist.as_str()));
        assert!(error.contains("Failed to read playlist"));
        assert!(!output.exists());
    }
}
