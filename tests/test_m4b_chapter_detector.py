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


if __name__ == "__main__":
    unittest.main()

