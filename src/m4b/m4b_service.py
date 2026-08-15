"""Orchestrates M4B generation and status updates."""

from __future__ import annotations

import logging
import os
import json
from pathlib import Path
from typing import Callable

from src.db.models import Book
from src.m4b.path_resolver import M4BPathResolver

logger = logging.getLogger(__name__)


class M4BService:
    """High-level M4B workflow entrypoint used by SyncManager."""

    def __init__(self, abs_client, database_service, chapter_detector, converter):
        self.abs_client = abs_client
        self.database_service = database_service
        self.chapter_detector = chapter_detector
        self.converter = converter
        self.enabled = os.getenv("M4B_ENABLED", "true").lower() == "true"
        self.output_mode = os.getenv("M4B_OUTPUT_MODE", "alongside").lower()
        self.output_dir = Path(os.getenv("M4B_OUTPUT_DIR", "/data/m4b"))
        self.watch_dir = os.getenv("M4B_UPLOAD_TO_ABS_WATCH_DIR", "").strip()
        self.replace_if_exists = os.getenv("M4B_REPLACE_IF_EXISTS", "false").lower() == "true"
        self.trigger_abs_scan = os.getenv("M4B_TRIGGER_ABS_SCAN", "false").lower() == "true"
        self.default_language = os.getenv("M4B_LANGUAGE", "auto")

        self.path_resolver = M4BPathResolver(
            audio_root=os.getenv("AUDIOBOOKS_DIR", "/audiobooks"),
            mappings=os.getenv("M4B_PATH_MAPPINGS", ""),
        )

    def process_book(
        self,
        book: Book,
        transcript_path: Path,
        item_details: dict | None,
        progress_callback: Callable[[float], None] | None = None,
        force: bool = False,
    ) -> None:
        logger.info("[M4B] Processing %s (force=%s)", getattr(book, "abs_title", book.abs_id), force)
        if not self.enabled:
            self._update_book(book, status="skipped", error=None)
            return

        audio_files = (item_details or {}).get("media", {}).get("audioFiles", [])
        self._update_book(book, status="detecting_chapters", progress=0.05, error=None)
        if progress_callback:
            progress_callback(0.15)

        total_duration = float(book.duration or 0.0)
        chapters = self.chapter_detector.detect_from_transcript(
            transcript_path,
            total_duration=total_duration,
            language=self.default_language,
        )
        self._update_abs_chapters(book, chapters, item_details, transcript_path)

        if self._is_already_m4b(audio_files):
            self._update_book(book, status="not_needed", progress=1.0, error=None)
            if self.trigger_abs_scan:
                self._trigger_scan(book)
            return

        source_paths, strategies = self._resolve_source_audio_paths(audio_files)
        if not source_paths:
            self._update_book(
                book,
                status="failed",
                error="m4b_source_path_unresolved",
                source_paths=[],
                path_strategy="unresolved",
            )
            return

        output_path, output_strategy = self._determine_output_path(book, source_paths)
        self._update_book(
            book,
            status="detecting_chapters",
            progress=0.2,
            output_file=str(output_path),
            error=None,
            source_paths=source_paths,
            path_strategy=self._compress_strategies(strategies, output_strategy),
        )
        if output_path.exists() and not (force or self.replace_if_exists):
            self._update_book(
                book,
                status="completed",
                progress=1.0,
                output_file=str(output_path),
                error=None,
                source_paths=source_paths,
                path_strategy=self._compress_strategies(strategies, output_strategy),
            )
            return

        self._update_book(
            book,
            status="converting",
            progress=0.35,
            output_file=str(output_path),
            error=None,
            source_paths=source_paths,
            path_strategy=self._compress_strategies(strategies, output_strategy),
        )
        if progress_callback:
            progress_callback(0.55)

        result = self.converter.convert(source_paths, output_path, chapters)

        self._update_book(
            book,
            status="completed",
            progress=1.0,
            output_file=str(result),
            error=None,
            source_paths=source_paths,
            path_strategy=self._compress_strategies(strategies, output_strategy),
        )

        if self.trigger_abs_scan:
            self._trigger_scan(book)

    def _resolve_source_audio_paths(self, audio_files: list[dict]) -> tuple[list[Path], list[str]]:
        resolved: list[Path] = []
        strategies: list[str] = []
        sorted_audio = sorted(audio_files, key=lambda x: (x.get("disc", 0) or 0, x.get("track", 0) or 0))

        for af in sorted_audio:
            metadata = af.get("metadata", {})
            raw_path = metadata.get("path")
            filename = metadata.get("filename")
            path, strategy = self.path_resolver.resolve_with_strategy(raw_path=raw_path, filename=filename)
            if not path:
                logger.warning("[M4B] Could not resolve source file path for %s", filename or raw_path)
                continue
            resolved.append(path)
            strategies.append(strategy)

        return resolved, strategies

    def _determine_output_path(self, book: Book, source_paths: list[Path]) -> tuple[Path, str]:
        target_name = f"{book.abs_title or book.abs_id}.m4b".replace("/", "_").replace("\\", "_")

        if self.output_mode == "alongside" and source_paths:
            return source_paths[0].parent / target_name, "alongside"

        if self.watch_dir:
            return Path(self.watch_dir) / target_name, "watch_dir"

        return self.output_dir / target_name, "output_dir"

    @staticmethod
    def _compress_strategies(source_strategies: list[str], output_strategy: str) -> str:
        source_part = ",".join(sorted(set(source_strategies))) if source_strategies else "unresolved"
        return f"src:{source_part}|dst:{output_strategy}"

    @staticmethod
    def _is_already_m4b(audio_files: list[dict]) -> bool:
        if not audio_files:
            return False
        if len(audio_files) == 1:
            af = audio_files[0]
            ext = str(af.get("ext", "")).lower()
            metadata = af.get("metadata", {})
            path = str(metadata.get("path", "")).lower()
            filename = str(metadata.get("filename", "")).lower()
            return ext == "m4b" or path.endswith(".m4b") or filename.endswith(".m4b")
        return False

    def _trigger_scan(self, book: Book) -> None:
        try:
            if hasattr(self.abs_client, "trigger_library_scan"):
                self.abs_client.trigger_library_scan()
        except Exception as exc:
            logger.warning("[M4B] ABS scan trigger failed: %s", exc)

    def _update_abs_chapters(
        self,
        book: Book,
        chapters: list[dict],
        item_details: dict | None,
        transcript_path: Path,
    ) -> None:
        if not chapters or len(chapters) <= 1:
            logger.info("[M4B] Skipping ABS chapter update for %s - insufficient detected chapters", getattr(book, "abs_title", book.abs_id))
            return

        if not hasattr(self.abs_client, "update_chapters"):
            return

        media = (item_details or {}).get("media", {})
        total_duration = float(media.get("duration") or book.duration or 0.0)
        if total_duration <= 0:
            try:
                with open(transcript_path, "r", encoding="utf-8") as handle:
                    segments = json.load(handle)
                total_duration = max((float(seg.get("end", 0.0) or 0.0) for seg in segments), default=0.0)
            except Exception as exc:
                logger.debug("[M4B] Could not infer transcript duration for ABS chapter update: %s", exc)

        payload = []
        for idx, chapter in enumerate(chapters):
            start = float(chapter.get("start", 0.0) or 0.0)
            if idx + 1 < len(chapters):
                end = float(chapters[idx + 1].get("start", start) or start)
            else:
                end = total_duration if total_duration > start else start

            end = max(end - 0.001, start)
            payload.append({
                "id": idx,
                "start": round(start, 3),
                "end": round(end, 3),
                "title": chapter.get("title") or f"Chapter {idx + 1}",
                "error": None,
            })

        try:
            updated = self.abs_client.update_chapters(book.abs_id, payload)
            if not updated:
                logger.warning("[M4B] ABS chapter update was not accepted for %s", getattr(book, "abs_title", book.abs_id))
        except Exception as exc:
            logger.warning("[M4B] ABS chapter update failed for %s: %s", getattr(book, "abs_title", book.abs_id), exc)

    def _update_book(
        self,
        book: Book,
        status: str,
        progress: float | None = None,
        output_file: str | None = None,
        error: str | None = None,
        source_paths: list[Path] | None = None,
        path_strategy: str | None = None,
    ) -> None:
        book.m4b_status = status
        if progress is not None:
            book.m4b_progress = progress
        if output_file is not None:
            book.m4b_output_file = output_file
        if source_paths is not None:
            book.m4b_source_paths = json.dumps([str(p) for p in source_paths])
        if path_strategy is not None:
            book.m4b_path_strategy = path_strategy
        book.m4b_error = error
        book.m4b_updated_at = __import__("time").time()
        self.database_service.save_book(book)



