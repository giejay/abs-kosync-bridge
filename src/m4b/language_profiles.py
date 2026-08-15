"""Language-specific chapter marker profiles for transcript parsing."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ChapterLanguageProfile:
    markers: tuple[str, ...]
    excluded_phrases: tuple[str, ...]
    standalone_number_words: tuple[tuple[str, int], ...] = ()


def _english_number_word(value: int) -> str:
    one_to_nineteen = {
        1: "one",
        2: "two",
        3: "three",
        4: "four",
        5: "five",
        6: "six",
        7: "seven",
        8: "eight",
        9: "nine",
        10: "ten",
        11: "eleven",
        12: "twelve",
        13: "thirteen",
        14: "fourteen",
        15: "fifteen",
        16: "sixteen",
        17: "seventeen",
        18: "eighteen",
        19: "nineteen",
    }
    tens_words = {
        20: "twenty",
        30: "thirty",
        40: "forty",
        50: "fifty",
    }

    if value in one_to_nineteen:
        return one_to_nineteen[value]
    if value in tens_words:
        return tens_words[value]

    tens = (value // 10) * 10
    ones = value % 10
    if tens in tens_words and ones in one_to_nineteen:
        return f"{tens_words[tens]} {one_to_nineteen[ones]}"

    raise ValueError(f"Unsupported english number word: {value}")


ENGLISH_STANDALONE_NUMBER_WORDS = tuple(
    (_english_number_word(i), i) for i in range(1, 51)
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

