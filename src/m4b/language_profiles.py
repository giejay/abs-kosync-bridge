"""Language-specific chapter marker profiles for transcript parsing."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ChapterLanguageProfile:
    markers: tuple[str, ...]
    excluded_phrases: tuple[str, ...]


ENGLISH_PROFILE = ChapterLanguageProfile(
    markers=("prologue", "chapter", "ch.", "epilogue"),
    excluded_phrases=(
        "chapter and verse",
        "this chapter",
        "that chapter",
        "in this chapter",
        "next chapter",
        "from the epilogue",
    ),
)

DUTCH_PROFILE = ChapterLanguageProfile(
    markers=("proloog", "hoofdstuk", "hst.", "hfst.", "epiloog"),
    excluded_phrases=(
        "in dit hoofdstuk",
        "volgend hoofdstuk",
        "hoofdstuk van",
    ),
)

LANGUAGE_PROFILES = {
    "en": ENGLISH_PROFILE,
    "en-us": ENGLISH_PROFILE,
    "nl": DUTCH_PROFILE,
    "nl-nl": DUTCH_PROFILE,
}


def get_profile(language: str | None) -> ChapterLanguageProfile:
    """Return language profile, defaulting to English when unknown."""
    if not language:
        return ENGLISH_PROFILE
    return LANGUAGE_PROFILES.get(language.lower(), ENGLISH_PROFILE)

