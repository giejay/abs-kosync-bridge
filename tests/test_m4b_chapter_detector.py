import unittest

from src.m4b.chapter_detector import ChapterDetector


class TestM4BChapterDetector(unittest.TestCase):
    def test_detects_english_and_dutch_markers(self):
        detector = ChapterDetector(default_language="en")
        segments = [
            {"start": 0, "end": 5, "text": "Prologue"},
            {"start": 70, "end": 75, "text": "Chapter one"},
            {"start": 130, "end": 140, "text": "hoofdstuk twee"},
            {"start": 210, "end": 215, "text": "Epilogue"},
        ]

        chapters = detector.detect_from_segments(segments, total_duration=500, language="en")

        self.assertGreaterEqual(len(chapters), 3)
        self.assertEqual(chapters[0]["start"], 0.0)

    def test_excludes_common_false_positives(self):
        detector = ChapterDetector(default_language="en")
        segments = [
            {"start": 0, "end": 10, "text": "Welcome"},
            {"start": 60, "end": 70, "text": "in this chapter we discuss"},
            {"start": 120, "end": 130, "text": "chapter three"},
        ]

        chapters = detector.detect_from_segments(segments, total_duration=500, language="en")

        self.assertEqual(len(chapters), 2)
        self.assertEqual(int(chapters[1]["start"]), 120)

    def test_detects_dutch_aflevering_marker(self):
        detector = ChapterDetector(default_language="nl")
        segments = [
            {"start": 0, "end": 10, "text": "Intro"},
            {"start": 80, "end": 90, "text": "Aflevering 1"},
            {"start": 180, "end": 190, "text": "Aflevering 2."},
        ]

        chapters = detector.detect_from_segments(segments, total_duration=500, language="nl")
        starts = [int(c["start"]) for c in chapters]

        self.assertIn(80, starts)
        self.assertIn(180, starts)

    def test_detects_standalone_english_number_word_headings(self):
        detector = ChapterDetector(default_language="en")
        segments = [
            {"start": 0, "end": 10, "text": "Preface"},
            {"start": 70, "end": 72, "text": "One"},
            {"start": 140, "end": 142, "text": "Two."},
            {"start": 210, "end": 214, "text": "Three and a half memories"},
        ]

        chapters = detector.detect_from_segments(segments, total_duration=500, language="en")
        summary = {int(c["start"]): c["title"] for c in chapters}

        self.assertEqual(summary[70], "Chapter 1")
        self.assertEqual(summary[140], "Chapter 2")
        self.assertNotIn(210, summary)

    def test_detects_standalone_numeric_headings_from_reverse_mapping(self):
        detector = ChapterDetector(default_language="en")
        segments = [
            {"start": 0, "end": 10, "text": "Intro"},
            {"start": 70, "end": 71, "text": "1"},
            {"start": 140, "end": 141, "text": "10."},
            {"start": 210, "end": 214, "text": "10 things I learned"},
        ]

        chapters = detector.detect_from_segments(segments, total_duration=500, language="en")
        summary = {int(c["start"]): c["title"] for c in chapters}

        self.assertEqual(summary[70], "Chapter 1")
        self.assertEqual(summary[140], "Chapter 10")
        self.assertNotIn(210, summary)

    def test_skips_decreasing_standalone_numeric_headings(self):
        detector = ChapterDetector(default_language="en")
        segments = [
            {"start": 0, "end": 10, "text": "Intro"},
            {"start": 60, "end": 61, "text": "1"},
            {"start": 120, "end": 121, "text": "2"},
            {"start": 180, "end": 181, "text": "3"},
            {"start": 240, "end": 241, "text": "1"},
        ]

        chapters = detector.detect_from_segments(segments, total_duration=600, language="en")
        starts = [int(c["start"]) for c in chapters]

        self.assertIn(60, starts)
        self.assertIn(120, starts)
        self.assertIn(180, starts)
        self.assertNotIn(240, starts)


if __name__ == "__main__":
    unittest.main()

