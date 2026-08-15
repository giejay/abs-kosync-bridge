"""Transcript chapter detection utilities for M4B conversion."""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path

from src.m4b.language_profiles import ChapterLanguageProfile, get_profile

logger = logging.getLogger(__name__)


class ChapterDetector:
    """Find probable chapter starts from transcript segment text."""

    def __init__(self, default_language: str = "en"):
        self.default_language = default_language
        self.verbose_debug = str(__import__("os").getenv("M4B_CHAPTER_DEBUG_VERBOSE", "false")).lower() == "true"

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

        inferred_duration = max((float(seg.get("end", 0.0) or 0.0) for seg in segments), default=0.0)
        effective_total_duration = float(total_duration or 0.0)
        if effective_total_duration <= 0 and inferred_duration > 0:
            logger.debug(
                "[M4B][ChapterDetector] total_duration missing/zero; using transcript inferred duration %.2f",
                inferred_duration,
            )
            effective_total_duration = inferred_duration

        effective_language = language or self.default_language
        if str(effective_language).lower() == "auto":
            profile_en = get_profile("en")
            profile_nl = get_profile("nl")
            profile = type(profile_en)(
                markers=tuple(dict.fromkeys(profile_en.markers + profile_nl.markers)),
                excluded_phrases=tuple(dict.fromkeys(profile_en.excluded_phrases + profile_nl.excluded_phrases)),
                standalone_number_words=tuple(dict.fromkeys(profile_en.standalone_number_words + profile_nl.standalone_number_words)),
            )
        else:
            profile = get_profile(effective_language)
        starts: list[dict] = [{"start": 0.0, "title": "Chapter 1"}]
        seen_starts: set[int] = {0}
        chapter_counter = 1

        min_gap_seconds = 45

        logger.debug(
            "[M4B][ChapterDetector] language=%s markers=%s excluded=%s segments=%s total_duration=%s",
            effective_language,
            profile.markers,
            profile.excluded_phrases,
            len(segments),
            effective_total_duration,
        )
        if not self.verbose_debug:
            logger.debug("[M4B][ChapterDetector] verbose per-segment logging disabled (set M4B_CHAPTER_DEBUG_VERBOSE=true to enable)")

        for idx, seg in enumerate(segments):
            text = str(seg.get("text", "")).strip().lower()
            start = float(seg.get("start", 0.0) or 0.0)

            if self.verbose_debug and logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "[M4B][ChapterDetector] seg=%s start=%.2f text=%r",
                    idx,
                    start,
                    text[:120],
                )

            if not text or any(excluded in text for excluded in profile.excluded_phrases):
                if self.verbose_debug:
                    logger.debug(
                        "[M4B][ChapterDetector] skip seg=%s reason=%s",
                        idx,
                        "empty_text" if not text else "excluded_phrase",
                    )
                continue

            marker = self._match_marker(text, profile.markers)
            standalone_chapter_number = None
            if not marker:
                standalone_chapter_number = self._match_standalone_number_heading(text, profile)
            if not marker and standalone_chapter_number is None:
                if self.verbose_debug:
                    logger.debug("[M4B][ChapterDetector] skip seg=%s reason=no_marker", idx)
                continue

            rounded_start = int(start)
            if rounded_start in seen_starts:
                if self.verbose_debug:
                    logger.debug("[M4B][ChapterDetector] skip seg=%s reason=duplicate_start start=%s", idx, rounded_start)
                continue
            if len(starts) > 1 and (rounded_start - int(starts[-1]["start"])) < min_gap_seconds:
                if self.verbose_debug:
                    logger.debug(
                        "[M4B][ChapterDetector] skip seg=%s reason=min_gap start=%s last=%s gap=%s",
                        idx,
                        rounded_start,
                        int(starts[-1]["start"]),
                        rounded_start - int(starts[-1]["start"]),
                    )
                continue

            if marker in ("prologue", "proloog"):
                title = "Prologue"
            elif marker in ("epilogue", "epiloog"):
                title = "Epilogue"
            elif standalone_chapter_number is not None:
                chapter_counter = max(chapter_counter, standalone_chapter_number)
                title = f"Chapter {standalone_chapter_number}"
            else:
                chapter_counter += 1
                title = f"Chapter {chapter_counter}"

            starts.append({"start": start, "title": title})
            seen_starts.add(rounded_start)
            logger.debug(
                "[M4B][ChapterDetector] detected marker=%s standalone_number=%s title=%s start=%.2f",
                marker,
                standalone_chapter_number,
                title,
                start,
            )

        starts = [c for c in starts if c["start"] < effective_total_duration or effective_total_duration <= 0]
        starts.sort(key=lambda x: x["start"])

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "[M4B][ChapterDetector] chapter_start_summary=%s",
                [f"{c['title']}@{c['start']:.1f}" for c in starts],
            )

        logger.info("[M4B] Detected %s chapter markers", max(0, len(starts) - 1))
        return starts

    @staticmethod
    def _match_marker(text: str, markers: tuple[str, ...]) -> str | None:
        for marker in markers:
            pattern = rf"\b{re.escape(marker)}\b"
            if re.search(pattern, text, re.IGNORECASE):
                return marker
        return None

    @staticmethod
    def _match_standalone_number_heading(text: str, profile: ChapterLanguageProfile) -> int | None:
        normalized = re.sub(r"\s+", " ", text.strip().lower())
        if not normalized:
            return None

        for word, number in profile.standalone_number_words:
            pattern = rf"^{re.escape(word)}\.?$"
            if re.fullmatch(pattern, normalized, re.IGNORECASE):
                return number
        return None


