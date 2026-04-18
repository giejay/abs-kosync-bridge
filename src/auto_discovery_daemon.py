"""
Auto-discovery daemon for automatically syncing recently played audiobooks.

This daemon periodically checks Audiobookshelf for recently played items,
attempts to fetch their ebooks, and creates sync jobs automatically.
"""

import logging
import os
import time
from pathlib import Path
from typing import Set, Optional
import requests

from src.db.database_service import DatabaseService
from src.db.models import Book
from src.utils.logging_utils import sanitize_log_data

logger = logging.getLogger(__name__)


class AutoDiscoveryDaemon:
    """
    Daemon that automatically discovers and syncs recently played audiobooks.

    Features:
    - Checks for items played within the last week
    - Identifies unmapped audiobooks (not in database)
    - Attempts to download ebook from ABS ebook endpoint
    - Creates sync jobs for newly mapped books
    """

    def __init__(self,
                 abs_client,
                 database_service: DatabaseService,
                 epub_cache_dir: Path = None,
                 lookback_days: int = 7):
        """
        Initialize the auto-discovery daemon.

        Args:
            abs_client: Audiobookshelf API client
            database_service: Database service for book management
            epub_cache_dir: Directory to cache downloaded ebooks
            lookback_days: How many days back to check for activity (default: 7)
        """
        self.abs_client = abs_client
        self.database_service = database_service
        self.lookback_days = lookback_days

        # Setup cache directory
        data_dir = Path(os.environ.get("DATA_DIR", "/data"))
        self.epub_cache_dir = epub_cache_dir or (data_dir / "epub_cache")
        self.epub_cache_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"🔍 Auto-discovery daemon initialized (lookback: {lookback_days} days)")

    def get_recently_played_items(self) -> list:
        """
        Fetch items that have been played recently (within lookback_days).

        Returns:
            List of item dictionaries with progress data
        """
        try:
            # Get all progress data
            progress_map = self.abs_client.get_all_progress_raw()

            if not progress_map:
                logger.debug("No progress data found")
                return []

            # Calculate cutoff timestamp (current time - lookback_days)
            cutoff_timestamp = time.time() - (self.lookback_days * 24 * 60 * 60)

            recent_items = []
            for item_id, progress_data in progress_map.items():
                # Skip completed/finished books
                is_finished = progress_data.get('isFinished', False)
                if is_finished:
                    logger.debug(f"[{item_id}] Skipping completed book")
                    continue

                # Check if item was updated recently
                last_update = progress_data.get('lastUpdate', 0)
                if isinstance(last_update, (int, float)):
                    # Convert from milliseconds if needed
                    if last_update > 10000000000:  # Likely milliseconds
                        last_update = last_update / 1000.0

                    if last_update >= cutoff_timestamp:
                        # Only include if it has meaningful progress
                        duration = progress_data.get('duration', 0)
                        current_time = progress_data.get('currentTime', 0)

                        if duration > 0:
                            progress_pct = current_time / duration
                            # Include items with at least 1% progress but not finished
                            if 0.01 <= progress_pct < 1.0:
                                recent_items.append({
                                    'id': item_id,
                                    'duration': duration,
                                    'currentTime': current_time,
                                    'progress': progress_pct,
                                    'lastUpdate': last_update
                                })

            if recent_items:
                logger.info(f"📊 Found {len(recent_items)} recently played items")

            return recent_items

        except Exception as e:
            logger.error(f"Failed to get recently played items: {e}")
            return []

    def get_unmapped_items(self, recent_items: list) -> list:
        """
        Filter recent items to only include those not yet in the database.

        Args:
            recent_items: List of recently played items

        Returns:
            List of unmapped items (not in database)
        """
        try:
            # Get all mapped book IDs from database
            all_books = self.database_service.get_all_books()
            mapped_ids: Set[str] = {book.abs_id for book in all_books}

            # Filter for unmapped items
            unmapped = [item for item in recent_items if item['id'] not in mapped_ids]

            if unmapped:
                logger.info(f"🆕 Found {len(unmapped)} unmapped items (out of {len(recent_items)} recent)")

            return unmapped

        except Exception as e:
            logger.error(f"Failed to filter unmapped items: {e}")
            return []

    def fetch_ebook_from_abs(self, item_id: str) -> Optional[Path]:
        """
        Attempt to download the ebook file from Audiobookshelf.

        Uses the endpoint: /api/items/{item_id}/ebook

        Args:
            item_id: The ABS item ID

        Returns:
            Path to the downloaded ebook file, or None if failed
        """
        try:
            # Get item details first to determine filename
            item_details = self.abs_client.get_item_details(item_id)
            if not item_details:
                logger.debug(f"[{item_id}] Could not fetch item details")
                return None

            # Extract metadata for logging
            media = item_details.get('media', {})
            metadata = media.get('metadata', {})
            title = metadata.get('title', 'Unknown')

            # Check if item has an ebook
            ebook_file = media.get('ebookFile')
            if not ebook_file:
                logger.debug(f"[{item_id}] No ebook file available for '{sanitize_log_data(title)}'")
                return None

            # Get the ebook filename
            ebook_filename = ebook_file.get('metadata', {}).get('filename')
            if not ebook_filename:
                logger.warning(f"[{item_id}] Ebook exists but no filename found")
                return None

            # Check if we already have this file cached
            cached_path = self.epub_cache_dir / ebook_filename
            if cached_path.exists():
                logger.info(f"[{item_id}] ✅ Ebook already cached: {sanitize_log_data(ebook_filename)}")
                return cached_path

            # Download the ebook
            ebook_url = f"{self.abs_client.base_url}/api/items/{item_id}/ebook"
            logger.info(f"[{item_id}] 📥 Downloading ebook: {sanitize_log_data(ebook_filename)}")

            response = self.abs_client.session.get(ebook_url, timeout=30)

            if response.status_code == 200:
                # Save to cache
                with open(cached_path, 'wb') as f:
                    f.write(response.content)

                file_size_mb = len(response.content) / (1024 * 1024)
                logger.info(f"[{item_id}] ✅ Downloaded ebook ({file_size_mb:.1f} MB): {sanitize_log_data(ebook_filename)}")
                return cached_path
            else:
                logger.warning(f"[{item_id}] Failed to download ebook: HTTP {response.status_code}")
                return None

        except requests.exceptions.Timeout:
            logger.warning(f"[{item_id}] Ebook download timed out")
            return None
        except Exception as e:
            logger.error(f"[{item_id}] Failed to fetch ebook: {e}")
            return None

    def create_sync_job(self, item_id: str, ebook_filename: str) -> bool:
        """
        Create a new sync job for an audiobook with its ebook.

        Args:
            item_id: The ABS item ID
            ebook_filename: The ebook filename

        Returns:
            True if job created successfully
        """
        try:
            # Get item details
            item_details = self.abs_client.get_item_details(item_id)
            if not item_details:
                logger.error(f"[{item_id}] Cannot create job - failed to get item details")
                return False

            # Extract metadata
            media = item_details.get('media', {})
            metadata = media.get('metadata', {})
            title = metadata.get('title', 'Unknown')
            duration = media.get('duration', 0)

            # Create book record with 'pending' status to trigger job queue
            book = Book(
                abs_id=item_id,
                abs_title=title,
                ebook_filename=ebook_filename,
                status='pending',  # This will trigger the job queue
                duration=duration
            )

            # Save to database
            self.database_service.save_book(book)

            logger.info(f"[{item_id}] ✅ Created sync job for '{sanitize_log_data(title)}'")
            return True

        except Exception as e:
            logger.error(f"[{item_id}] Failed to create sync job: {e}")
            return False

    def discover_and_sync(self):
        """
        Main discovery cycle:
        1. Get recently played items
        2. Filter for unmapped items
        3. Attempt to fetch ebooks
        4. Create sync jobs for successful downloads
        """
        try:
            logger.debug("🔍 Running auto-discovery cycle...")

            # Step 1: Get recently played items
            recent_items = self.get_recently_played_items()
            if not recent_items:
                logger.debug("No recently played items found")
                return

            # Step 2: Filter for unmapped items
            unmapped_items = self.get_unmapped_items(recent_items)
            if not unmapped_items:
                logger.debug("All recent items are already mapped")
                return

            # Step 3 & 4: Try to fetch ebooks and create jobs
            success_count = 0
            for item in unmapped_items:
                item_id = item['id']

                # Attempt to download ebook
                ebook_path = self.fetch_ebook_from_abs(item_id)

                if ebook_path:
                    # Create sync job
                    if self.create_sync_job(item_id, ebook_path.name):
                        success_count += 1

                    # Rate limiting - don't hammer the server
                    time.sleep(1)

            if success_count > 0:
                logger.info(f"🎉 Auto-discovery completed: {success_count} new book(s) queued for sync")
            else:
                logger.debug("Auto-discovery completed: no new books added")

        except Exception as e:
            logger.error(f"Auto-discovery cycle failed: {e}")
            import traceback
            logger.debug(traceback.format_exc())

    def get_status(self) -> dict:
        """
        Get current status of the auto-discovery daemon.

        Returns:
            Dictionary with status information
        """
        try:
            recent_items = self.get_recently_played_items()
            unmapped_items = self.get_unmapped_items(recent_items)

            return {
                'enabled': True,
                'lookback_days': self.lookback_days,
                'recent_items': len(recent_items),
                'unmapped_items': len(unmapped_items),
                'cache_dir': str(self.epub_cache_dir),
                'cache_size_mb': self._get_cache_size_mb()
            }
        except Exception as e:
            logger.error(f"Failed to get auto-discovery status: {e}")
            return {
                'enabled': True,
                'error': str(e)
            }

    def _get_cache_size_mb(self) -> float:
        """Calculate total size of epub cache in MB."""
        try:
            total_size = 0
            for file in self.epub_cache_dir.glob("**/*.epub"):
                if file.is_file():
                    total_size += file.stat().st_size
            return total_size / (1024 * 1024)
        except Exception:
            return 0.0

