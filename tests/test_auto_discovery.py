"""
Tests for the auto-discovery daemon functionality.
"""

import unittest
from unittest.mock import Mock
from pathlib import Path
import tempfile
import time

from src.auto_discovery_daemon import AutoDiscoveryDaemon
from src.db.models import Book


class TestAutoDiscoveryDaemon(unittest.TestCase):
    """Test suite for AutoDiscoveryDaemon class."""

    def setUp(self):
        """Set up test fixtures."""
        # Create mock clients and services
        self.mock_abs_client = Mock()
        self.mock_database_service = Mock()

        # Create temp directory for epub cache
        self.temp_dir = tempfile.mkdtemp()
        self.epub_cache_dir = Path(self.temp_dir) / "epub_cache"

        # Initialize daemon
        self.daemon = AutoDiscoveryDaemon(
            abs_client=self.mock_abs_client,
            database_service=self.mock_database_service,
            epub_cache_dir=self.epub_cache_dir,
            lookback_days=7
        )

    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_initialization(self):
        """Test daemon initializes correctly."""
        self.assertIsNotNone(self.daemon)
        self.assertEqual(self.daemon.lookback_days, 7)
        self.assertTrue(self.epub_cache_dir.exists())

    def test_get_recently_played_items_empty(self):
        """Test getting recently played items when none exist."""
        self.mock_abs_client.get_all_progress_raw.return_value = {}

        items = self.daemon.get_recently_played_items()

        self.assertEqual(len(items), 0)

    def test_get_recently_played_items_with_data(self):
        """Test getting recently played items with valid data."""
        current_time = time.time()

        self.mock_abs_client.get_all_progress_raw.return_value = {
            'item-1': {
                'duration': 10000,
                'currentTime': 5000,
                'lastUpdate': current_time
            },
            'item-2': {
                'duration': 8000,
                'currentTime': 100,  # 1.25% progress
                'lastUpdate': current_time - (3 * 24 * 60 * 60)  # 3 days ago
            },
            'item-3': {
                'duration': 12000,
                'currentTime': 50,  # 0.4% progress (below threshold)
                'lastUpdate': current_time
            }
        }

        items = self.daemon.get_recently_played_items()

        # Should have 2 items (item-1 and item-2), item-3 is below progress threshold
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0]['id'], 'item-1')
        self.assertAlmostEqual(items[0]['progress'], 0.5, places=2)

    def test_get_recently_played_items_old_data_excluded(self):
        """Test that items outside the lookback window are excluded."""
        current_time = time.time()
        old_time = current_time - (10 * 24 * 60 * 60)  # 10 days ago

        self.mock_abs_client.get_all_progress_raw.return_value = {
            'old-item': {
                'duration': 10000,
                'currentTime': 5000,
                'lastUpdate': old_time,
                'isFinished': False
            }
        }

        items = self.daemon.get_recently_played_items()

        # Should be empty because item is too old
        self.assertEqual(len(items), 0)

    def test_get_recently_played_items_excludes_finished(self):
        """Test that finished/completed books are excluded from discovery."""
        current_time = time.time()

        self.mock_abs_client.get_all_progress_raw.return_value = {
            'finished-item': {
                'duration': 10000,
                'currentTime': 10000,
                'lastUpdate': current_time,
                'isFinished': True  # Marked as finished
            },
            'active-item': {
                'duration': 10000,
                'currentTime': 5000,
                'lastUpdate': current_time,
                'isFinished': False  # Not finished
            },
            'completed-item': {
                'duration': 8000,
                'currentTime': 8000,  # 100% progress
                'lastUpdate': current_time
                # No isFinished field, but progress is 100%
            }
        }

        items = self.daemon.get_recently_played_items()

        # Should only have active-item (not finished, <100% progress)
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]['id'], 'active-item')
        self.assertAlmostEqual(items[0]['progress'], 0.5, places=2)

    def test_get_unmapped_items(self):
        """Test filtering for unmapped items."""
        # Setup database to return some mapped books
        mapped_book = Book(abs_id='mapped-1', abs_title='Mapped Book')
        self.mock_database_service.get_all_books.return_value = [mapped_book]

        recent_items = [
            {'id': 'mapped-1', 'progress': 0.5},
            {'id': 'unmapped-1', 'progress': 0.3},
            {'id': 'unmapped-2', 'progress': 0.7}
        ]

        unmapped = self.daemon.get_unmapped_items(recent_items)

        # Should have 2 unmapped items
        self.assertEqual(len(unmapped), 2)
        self.assertEqual(unmapped[0]['id'], 'unmapped-1')
        self.assertEqual(unmapped[1]['id'], 'unmapped-2')

    def test_fetch_ebook_from_abs_no_ebook(self):
        """Test fetching ebook when item has no ebook file."""
        self.mock_abs_client.get_item_details.return_value = {
            'media': {
                'metadata': {'title': 'Test Book'},
                'ebookFile': None
            }
        }

        result = self.daemon.fetch_ebook_from_abs('test-item')

        self.assertIsNone(result)

    def test_fetch_ebook_from_abs_cached(self):
        """Test fetching ebook when it's already cached."""
        # Create a cached file
        self.epub_cache_dir.mkdir(parents=True, exist_ok=True)
        cached_file = self.epub_cache_dir / "test-book.epub"
        cached_file.write_bytes(b"fake epub content")

        self.mock_abs_client.get_item_details.return_value = {
            'media': {
                'metadata': {'title': 'Test Book'},
                'ebookFile': {
                    'metadata': {'filename': 'test-book.epub'}
                }
            }
        }

        result = self.daemon.fetch_ebook_from_abs('test-item')

        self.assertEqual(result, cached_file)

    def test_fetch_ebook_from_abs_download(self):
        """Test fetching ebook with successful download."""
        self.mock_abs_client.get_item_details.return_value = {
            'media': {
                'metadata': {'title': 'Test Book'},
                'ebookFile': {
                    'metadata': {'filename': 'new-book.epub'}
                }
            }
        }

        # Mock successful HTTP response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b"new epub content"
        self.mock_abs_client.session.get.return_value = mock_response
        self.mock_abs_client.base_url = "http://test-server"

        result = self.daemon.fetch_ebook_from_abs('test-item')

        self.assertIsNotNone(result)
        self.assertEqual(result.name, 'new-book.epub')
        self.assertTrue(result.exists())
        self.assertEqual(result.read_bytes(), b"new epub content")

    def test_create_sync_job(self):
        """Test creating a sync job for a book."""
        self.mock_abs_client.get_item_details.return_value = {
            'media': {
                'metadata': {'title': 'New Book'},
                'duration': 12345
            }
        }

        result = self.daemon.create_sync_job('new-item', 'new-book.epub')

        self.assertTrue(result)
        # Verify save_book was called
        self.mock_database_service.save_book.assert_called_once()

        # Verify book has correct attributes
        saved_book = self.mock_database_service.save_book.call_args[0][0]
        self.assertEqual(saved_book.abs_id, 'new-item')
        self.assertEqual(saved_book.abs_title, 'New Book')
        self.assertEqual(saved_book.ebook_filename, 'new-book.epub')
        self.assertEqual(saved_book.status, 'pending')

    def test_get_status(self):
        """Test getting daemon status."""
        self.mock_abs_client.get_all_progress_raw.return_value = {
            'item-1': {
                'duration': 10000,
                'currentTime': 5000,
                'lastUpdate': time.time()
            }
        }
        self.mock_database_service.get_all_books.return_value = []

        status = self.daemon.get_status()

        self.assertTrue(status['enabled'])
        self.assertEqual(status['lookback_days'], 7)
        self.assertEqual(status['recent_items'], 1)
        self.assertEqual(status['unmapped_items'], 1)


if __name__ == '__main__':
    unittest.main()

