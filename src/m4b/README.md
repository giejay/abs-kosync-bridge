# M4B Conversion Pipeline

This package adds automatic M4B generation from ABS audiobook source files.

## Main components

- `chapter_detector.py`: Detects chapter boundaries from transcript JSON using language-aware markers.
- `path_resolver.py`: Resolves ABS-reported host paths into container-visible paths.
- `ffmpeg_m4b_converter.py`: Builds a chaptered M4B from source audio files.
- `m4b_service.py`: Orchestrates status updates, chapter detection, conversion, and optional ABS rescan.

## Config keys

- `M4B_ENABLED` (default: `true`)
- `M4B_OUTPUT_MODE` (default: `alongside`)
- `M4B_OUTPUT_DIR` (default: `/data/m4b`)
- `M4B_UPLOAD_TO_ABS_WATCH_DIR` (optional fallback target)
- `M4B_PATH_MAPPINGS` (`host_prefix=>container_prefix;...`)
- `M4B_REPLACE_IF_EXISTS` (default: `false`)
- `M4B_TRIGGER_ABS_SCAN` (default: `false`)
- `M4B_SKIP_FILE_CREATION` (default: `false`, updates ABS chapters only and skips writing `.m4b`)
- `M4B_LANGUAGE` (default: `auto`, recognizes both English and Dutch markers)

When file creation is enabled, the generated `.m4b` filename uses the first resolved source audio filename stem (for example, `track01.mp3` -> `track01.m4b`).

## Quick local test

Use the unit tests in `tests/test_m4b_chapter_detector.py` and `tests/test_m4b_path_resolver.py`.


