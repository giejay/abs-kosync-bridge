import unittest

from src.m4b.chapter_detector import ChapterDetector


class TestM4BChapterDetectorDebug(unittest.TestCase):
    def test_hoofdstuk_punctuation_is_detected(self):
        detector = ChapterDetector(default_language="auto")
        segments = [
            {"start": 0.0, "end": 3.0, "text": "Intro"},
            {"start": 12.0, "end": 14.0, "text": "Hoofdstuk 1."},
            {"start": 80.0, "end": 84.0, "text": "Hoofdstuk 2"},
        ]

        chapters = detector.detect_from_segments(segments, total_duration=300, language="auto")
        starts = [round(c["start"], 1) for c in chapters]

        self.assertIn(12.0, starts)
        self.assertIn(80.0, starts)

    def test_standalone_number_word_with_period_is_detected(self):
        detector = ChapterDetector(default_language="en")
        segments = [
            {"start": 0.0, "end": 3.0, "text": "Intro"},
            {"start": 55.0, "end": 56.0, "text": "Twenty three."},
        ]

        chapters = detector.detect_from_segments(segments, total_duration=300, language="en")
        chapter = next((c for c in chapters if round(c["start"], 1) == 55.0), None)

        self.assertIsNotNone(chapter)
        self.assertEqual(chapter["title"], "Chapter 23")

    def test_upper_bound_standalone_number_mappings_include_fifty(self):
        detector = ChapterDetector(default_language="en")
        segments = [
            {"start": 0.0, "end": 2.0, "text": "Intro"},
            {"start": 60.0, "end": 62.0, "text": "Fifty."},
            {"start": 130.0, "end": 131.0, "text": "50"},
        ]

        chapters = detector.detect_from_segments(segments, total_duration=400, language="en")
        summary = {int(c["start"]): c["title"] for c in chapters}

        self.assertEqual(summary[60], "Chapter 50")
        self.assertNotIn(130, summary)


if __name__ == "__main__":
    unittest.main()

