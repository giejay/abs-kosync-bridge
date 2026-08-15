import unittest
from unittest.mock import Mock

from src.api.api_clients import ABSClient


class TestABSClientChapterUpdates(unittest.TestCase):
    def setUp(self):
        self.client = ABSClient()
        self.client.base_url = "http://abs.local"
        self.client.session = Mock()

    def test_update_chapters_posts_expected_payload(self):
        response = Mock(status_code=200, text="ok")
        self.client.session.post.return_value = response

        chapters = [
            {"id": 0, "start": 0.0, "end": 10.0, "title": "Chapter 1", "error": None},
            {"id": 1, "start": 10.0, "end": 20.0, "title": "Chapter 2", "error": None},
        ]

        result = self.client.update_chapters("book-123", chapters)

        self.assertTrue(result)
        self.client.session.post.assert_called_once_with(
            "http://abs.local/api/items/book-123/chapters",
            json={"chapters": chapters},
            timeout=15,
        )

    def test_update_chapters_returns_false_for_empty_payload(self):
        self.assertFalse(self.client.update_chapters("book-123", []))
        self.client.session.post.assert_not_called()


if __name__ == "__main__":
    unittest.main()

