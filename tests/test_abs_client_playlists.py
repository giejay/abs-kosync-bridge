import unittest
from unittest.mock import Mock

from src.api.api_clients import ABSClient


class TestABSClientPlaylists(unittest.TestCase):
    def setUp(self):
        self.client = ABSClient()
        self.client.base_url = "http://abs.local"
        self.client.session = Mock()

    def test_get_playlist_by_name_returns_existing_playlist(self):
        response = Mock(status_code=200)
        response.json.return_value = {
            "playlists": [
                {"id": "pl-1", "name": "Next", "items": []},
                {"id": "pl-2", "name": "Later", "items": []},
            ]
        }
        self.client.session.get.return_value = response

        playlist = self.client.get_playlist_by_name("Next")

        self.assertIsNotNone(playlist)
        self.assertEqual(playlist.get("id"), "pl-1")

    def test_get_playlist_item_ids_extracts_library_item_ids(self):
        playlist = {
            "id": "pl-1",
            "name": "Next",
            "items": [
                {"libraryItemId": "book-1"},
                {"libraryItemId": "book-2"},
                {"libraryItemId": "book-2"},
            ],
        }

        item_ids = self.client.get_playlist_item_ids(playlist)

        self.assertEqual(item_ids, ["book-1", "book-2"])

    def test_add_item_to_playlist_returns_true_on_success(self):
        success_response = Mock(status_code=200)
        self.client.session.post.return_value = success_response

        ok = self.client.add_item_to_playlist("pl-1", "book-1")

        self.assertTrue(ok)


if __name__ == "__main__":
    unittest.main()


