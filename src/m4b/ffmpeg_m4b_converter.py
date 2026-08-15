"""FFmpeg-based M4B converter with chapter metadata support."""

from __future__ import annotations

import logging
import subprocess
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)


class FfmpegM4BConverter:
    """Convert multiple audio parts into a single chaptered M4B file."""

    def convert(self, source_files: list[Path], output_file: Path, chapters: list[dict]) -> Path:
        if not source_files:
            raise ValueError("No source audio files were provided for M4B conversion")

        output_file.parent.mkdir(parents=True, exist_ok=True)

        with tempfile.TemporaryDirectory(prefix="m4b-build-") as tmp_dir:
            tmp = Path(tmp_dir)
            concat_file = tmp / "concat.txt"
            metadata_file = tmp / "chapters.ffmeta"

            with open(concat_file, "w", encoding="utf-8") as handle:
                for path in source_files:
                    escaped = str(path).replace("'", "'\\''")
                    handle.write(f"file '{escaped}'\n")

            self._write_ffmetadata(metadata_file, chapters)

            cmd = [
                "ffmpeg",
                "-y",
                "-f",
                "concat",
                "-safe",
                "0",
                "-i",
                str(concat_file),
                "-i",
                str(metadata_file),
                "-map_metadata",
                "1",
                "-map",
                "0:a",
                "-c:a",
                "aac",
                "-b:a",
                "96k",
                "-movflags",
                "+faststart",
                str(output_file),
            ]

            logger.info("[M4B] Running ffmpeg to build %s", output_file)
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                raise RuntimeError(f"ffmpeg failed: {result.stderr[-500:]}")

        return output_file

    @staticmethod
    def _write_ffmetadata(metadata_path: Path, chapters: list[dict]) -> None:
        lines = [";FFMETADATA1\n"]
        for idx, chapter in enumerate(chapters):
            start = int(float(chapter.get("start", 0.0)) * 1000)
            if idx + 1 < len(chapters):
                end = int(float(chapters[idx + 1].get("start", 0.0)) * 1000) - 1
            else:
                end = start + 1000
            if end <= start:
                end = start + 1000
            title = chapter.get("title", f"Chapter {idx + 1}")
            lines.extend([
                "[CHAPTER]\n",
                "TIMEBASE=1/1000\n",
                f"START={start}\n",
                f"END={end}\n",
                f"title={title}\n",
            ])

        with open(metadata_path, "w", encoding="utf-8") as handle:
            handle.writelines(lines)

