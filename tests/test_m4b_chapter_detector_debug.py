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


if __name__ == "__main__":
    unittest.main()

