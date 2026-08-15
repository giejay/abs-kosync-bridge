"""Language-specific chapter marker profiles for transcript parsing."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ChapterLanguageProfile:
    markers: tuple[str, ...]
    excluded_phrases: tuple[str, ...]
    standalone_number_words: tuple[tuple[str, int], ...] = ()


ENGLISH_STANDALONE_NUMBER_WORDS = (
    ("one", 1),
    ("two", 2),
    ("three", 3),
    ("four", 4),
    ("five", 5),
    ("six", 6),
    ("seven", 7),
    ("eight", 8),
    ("nine", 9),
    ("ten", 10),
    ("eleven", 11),
    ("twelve", 12),
    ("thirteen", 13),
    ("fourteen", 14),
    ("fifteen", 15),
    ("sixteen", 16),
    ("seventeen", 17),
    ("eighteen", 18),
    ("nineteen", 19),
    ("twenty", 20),
    ("twenty one", 21),
    ("twenty two", 22),
    ("twenty three", 23),
    ("twenty four", 24),
    ("twenty five", 25),
)


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
    standalone_number_words=ENGLISH_STANDALONE_NUMBER_WORDS,
)

DUTCH_PROFILE = ChapterLanguageProfile(
    markers=("proloog", "hoofdstuk", "hst.", "hfst.", "aflevering", "epiloog"),
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

