"""Transcript chapter detection utilities for M4B conversion."""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path

from src.m4b.language_profiles import get_profile

logger = logging.getLogger(__name__)


class ChapterDetector:
    """Find probable chapter starts from transcript segment text."""

    def __init__(self, default_language: str = "en"):
        self.default_language = default_language

    def detect_from_transcript(
        self,
        transcript_path: str | Path,
        total_duration: float,
        language: str | None = None,
    ) -> list[dict]:
        with open(transcript_path, "r", encoding="utf-8") as handle:
            segments = json.load(handle)
        return self.detect_from_segments(segments, total_duration=total_duration, language=language)

    def detect_from_segments(
        self,
        segments: list[dict],
        total_duration: float,
        language: str | None = None,
    ) -> list[dict]:
        if not segments:
            return [{"start": 0.0, "title": "Chapter 1"}]

        effective_language = language or self.default_language
        if str(effective_language).lower() == "auto":
            profile_en = get_profile("en")
            profile_nl = get_profile("nl")
            profile = type(profile_en)(
                markers=tuple(dict.fromkeys(profile_en.markers + profile_nl.markers)),
                excluded_phrases=tuple(dict.fromkeys(profile_en.excluded_phrases + profile_nl.excluded_phrases)),
            )
        else:
            profile = get_profile(effective_language)
        starts: list[dict] = [{"start": 0.0, "title": "Chapter 1"}]
        seen_starts: set[int] = {0}
        chapter_counter = 1

        min_gap_seconds = 45

        for seg in segments:
            text = str(seg.get("text", "")).strip().lower()
            start = float(seg.get("start", 0.0) or 0.0)

            if not text or any(excluded in text for excluded in profile.excluded_phrases):
                continue

            marker = self._match_marker(text, profile.markers)
            if not marker:
                continue

            rounded_start = int(start)
            if rounded_start in seen_starts:
                continue
            if (rounded_start - int(starts[-1]["start"])) < min_gap_seconds:
                continue

            if marker in ("prologue", "proloog"):
                title = "Prologue"
            elif marker in ("epilogue", "epiloog"):
                title = "Epilogue"
            else:
                chapter_counter += 1
                title = f"Chapter {chapter_counter}"

            starts.append({"start": start, "title": title})
            seen_starts.add(rounded_start)

        starts = [c for c in starts if c["start"] < total_duration]
        starts.sort(key=lambda x: x["start"])

        logger.info("[M4B] Detected %s chapter markers", len(starts))
        return starts

    @staticmethod
    def _match_marker(text: str, markers: tuple[str, ...]) -> str | None:
        for marker in markers:
            if re.search(rf"\b{re.escape(marker)}\b", text, re.IGNORECASE):
                return marker
        return None


