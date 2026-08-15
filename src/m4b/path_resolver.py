"""Path resolution helpers for ABS host paths and Docker mount paths."""

from __future__ import annotations

import os
from pathlib import Path


class M4BPathResolver:
    """Resolve ABS file paths into container-visible absolute paths."""

    def __init__(self, audio_root: str | Path | None = None, mappings: str | None = None):
        self.audio_root = Path(audio_root) if audio_root else None
        self.mappings = self._parse_mappings(mappings or os.getenv("M4B_PATH_MAPPINGS", ""))

    def resolve(self, raw_path: str | None, filename: str | None = None) -> Path | None:
        path, _strategy = self.resolve_with_strategy(raw_path=raw_path, filename=filename)
        return path

    def resolve_with_strategy(self, raw_path: str | None, filename: str | None = None) -> tuple[Path | None, str]:
        if raw_path:
            direct = Path(raw_path)
            if direct.exists():
                return direct, "direct"

            mapped = self._apply_mappings(raw_path)
            if mapped and mapped.exists():
                return mapped, "mapping"

        if self.audio_root and self.audio_root.exists() and filename:
            matches = list(self.audio_root.glob(f"**/{filename}"))
            if matches:
                return matches[0], "audio_root_search"

        return None, "unresolved"

    def _apply_mappings(self, raw_path: str) -> Path | None:
        normalized = raw_path.replace("\\", "/")
        for src_prefix, dst_prefix in self.mappings:
            src = src_prefix.replace("\\", "/")
            if normalized.lower().startswith(src.lower()):
                remainder = normalized[len(src):].lstrip("/")
                return Path(dst_prefix) / Path(remainder)
        return None

    @staticmethod
    def _parse_mappings(raw_mappings: str) -> list[tuple[str, str]]:
        parsed: list[tuple[str, str]] = []
        for entry in [x.strip() for x in raw_mappings.split(";") if x.strip()]:
            if "=>" not in entry:
                continue
            src, dst = entry.split("=>", 1)
            src = src.strip()
            dst = dst.strip()
            if src and dst:
                parsed.append((src, dst))
        return parsed


