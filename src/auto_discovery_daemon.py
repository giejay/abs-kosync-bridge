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
                 ebook_parser=None,
                 booklore_client=None,
                 epub_cache_dir: Path = None,
                 lookback_days: int = 7):
        """
        Initialize the auto-discovery daemon.

        Args:
            abs_client: Audiobookshelf API client
            database_service: Database service for book management
            ebook_parser: Ebook parser for calculating KOSync hash
            booklore_client: Booklore client for fetching ebooks
            epub_cache_dir: Directory to cache downloaded ebooks
            lookback_days: How many days back to check for activity (default: 7)
        """
        self.abs_client = abs_client
        self.database_service = database_service
        self.ebook_parser = ebook_parser
        self.booklore_client = booklore_client
        self.lookback_days = lookback_days
        next_playlist_env = os.environ.get("AUTO_DISCOVERY_NEXT_PLAYLIST")
        self.next_playlist_name = (next_playlist_env or "Next").strip() or "Next"
        self.next_playlist_from_env = bool(next_playlist_env and next_playlist_env.strip())

        # Setup cache directory
        data_dir = Path(os.environ.get("DATA_DIR", "/data"))
        self.epub_cache_dir = epub_cache_dir or (data_dir / "epub_cache")
        self.epub_cache_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"🔍 Auto-discovery daemon initialized (lookback: {lookback_days} days)")

    def _get_next_playlist(self) -> Optional[dict]:
        if not hasattr(self.abs_client, "get_playlist_by_name"):
            logger.warning("Auto-discovery playlist sync unavailable: ABS client has no playlist helpers")
            return None
        try:
            playlist = self.abs_client.get_playlist_by_name(self.next_playlist_name)
            if not playlist:
                if self.next_playlist_from_env:
                    logger.warning(
                        f"Playlist '{self.next_playlist_name}' was configured but not found; queue processing is skipped"
                    )
                else:
                    logger.debug(f"Playlist '{self.next_playlist_name}' not found; skipping playlist queue")
            return playlist
        except Exception as e:
            logger.warning(f"Failed resolving playlist '{self.next_playlist_name}': {e}")
            return None

    def sync_continue_listening_to_next_playlist(self, recent_items: list[dict]) -> tuple[Optional[str], int]:
        """Mirror recent Continue Listening items into the Next playlist."""
        playlist = self._get_next_playlist()
        if not playlist:
            return None, 0

        if not hasattr(self.abs_client, "get_playlist_item_ids") or not hasattr(self.abs_client, "add_item_to_playlist"):
            logger.warning("Auto-discovery playlist sync unavailable: ABS client missing playlist item helpers")
            return playlist.get("id"), 0

        playlist_id = playlist.get("id")
        existing_ids = set(self.abs_client.get_playlist_item_ids(playlist))
        added_count = 0
        for item in recent_items:
            item_id = item.get("id")
            if not item_id or item_id in existing_ids:
                continue
            if self.abs_client.add_item_to_playlist(playlist_id, item_id):
                existing_ids.add(item_id)
                added_count += 1

        if added_count > 0:
            logger.info(f"📌 Added {added_count} continue-listening item(s) to playlist '{self.next_playlist_name}'")
        return playlist_id, added_count

    def mirror_continue_listening_to_next_playlist(self) -> tuple[Optional[str], int]:
        """Standalone job: mirror continue-listening items into the Next playlist."""
        recent_items = self.get_recently_played_items()
        if not recent_items:
            return None, 0
        return self.sync_continue_listening_to_next_playlist(recent_items)

    def get_unprocessed_items_from_next_playlist(self, playlist: Optional[dict]) -> list[dict]:
        """Get playlist items that are not mapped yet and can be queued."""
        if not playlist or not hasattr(self.abs_client, "get_playlist_item_ids"):
            return []

        try:
            queued_ids = self.abs_client.get_playlist_item_ids(playlist)
            if not queued_ids:
                return []

            all_books = self.database_service.get_all_books()
            mapped_ids: Set[str] = {book.abs_id for book in all_books}

            return [{"id": item_id} for item_id in queued_ids if item_id not in mapped_ids]
        except Exception as e:
            logger.error(f"Failed to inspect playlist '{self.next_playlist_name}': {e}")
            return []

    def process_next_playlist_queue(self) -> int:
        """Standalone job: process unmapped queue items from the Next playlist."""
        playlist = self._get_next_playlist()
        queue_items = self.get_unprocessed_items_from_next_playlist(playlist)
        if not queue_items:
            logger.debug("No unprocessed items found in playlist queue")
            return 0

        success_count = 0
        for item in queue_items:
            item_id = item['id']
            ebook_path = self.fetch_ebook_from_abs(item_id)
            if ebook_path and self.create_sync_job(item_id, ebook_path.name):
                success_count += 1
            time.sleep(1)

        if success_count > 0:
            logger.info(f"🎉 Playlist queue processing completed: {success_count} new book(s) queued for sync")
        return success_count

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
            logger.debug(
                "[auto-discovery] Evaluating %d progress item(s), cutoff=%s (%d day lookback)",
                len(progress_map),
                int(cutoff_timestamp),
                self.lookback_days,
            )

            recent_items = []
            stats = {
                'total': 0,
                'finished': 0,
                'invalid_last_update': 0,
                'too_old': 0,
                'invalid_duration': 0,
                'invalid_progress': 0,
                'accepted': 0,
            }
            for item_id, progress_data in progress_map.items():
                stats['total'] += 1
                # Skip completed/finished books
                is_finished = progress_data.get('isFinished', False)
                if is_finished:
                    stats['finished'] += 1
                    logger.debug(f"[{item_id}] Skipping completed book")
                    continue

                # Check if item was updated recently
                last_update = progress_data.get('lastUpdate', 0)
                if isinstance(last_update, (int, float)):
                    # Convert from milliseconds if needed
                    if last_update > 10000000000:  # Likely milliseconds
                        logger.debug(f"[{item_id}] Converting lastUpdate from ms to s: {last_update}")
                        last_update = last_update / 1000.0

                    if last_update >= cutoff_timestamp:
                        # Only include if it has meaningful progress
                        duration = progress_data.get('duration', 0)
                        current_time = progress_data.get('currentTime', 0)

                        if duration > 0:
                            progress_pct = current_time / duration
                            # Include items with at least 1% progress but not finished
                            if 0.01 <= progress_pct < 1.0:
                                stats['accepted'] += 1
                                recent_items.append({
                                    'id': item_id,
                                    'duration': duration,
                                    'currentTime': current_time,
                                    'progress': progress_pct,
                                    'lastUpdate': last_update
                                })
                            else:
                                stats['invalid_progress'] += 1
                                logger.debug(
                                    f"[{item_id}] Excluded by progress threshold: currentTime={current_time}, "
                                    f"duration={duration}, progress={progress_pct:.4f}"
                                )
                        else:
                            stats['invalid_duration'] += 1
                            logger.debug(f"[{item_id}] Excluded because duration is not > 0: duration={duration}")
                    else:
                        stats['too_old'] += 1
                        logger.debug(
                            f"[{item_id}] Excluded because lastUpdate is too old: "
                            f"lastUpdate={last_update}, cutoff={cutoff_timestamp}"
                        )
                else:
                    stats['invalid_last_update'] += 1
                    logger.debug(
                        f"[{item_id}] Excluded because lastUpdate is non-numeric: "
                        f"value={last_update!r}, type={type(last_update).__name__}"
                    )

            logger.debug(
                "[auto-discovery] Filter summary: total=%(total)d, accepted=%(accepted)d, "
                "finished=%(finished)d, too_old=%(too_old)d, invalid_last_update=%(invalid_last_update)d, "
                "invalid_duration=%(invalid_duration)d, invalid_progress=%(invalid_progress)d",
                stats,
            )

            if recent_items:
                logger.info(f"📊 Found {len(recent_items)} recently played items")
            else:
                logger.info("📊 Found 0 recently played items after filtering")

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

            # Calculate KOSync document ID
            kosync_doc_id = None
            if self.ebook_parser:
                # Try to get booklore_id if Booklore is configured
                booklore_id = None
                if self.booklore_client and self.booklore_client.is_configured():
                    try:
                        book_info = self.booklore_client.find_book_by_filename(ebook_filename)
                        if book_info:
                            booklore_id = book_info.get('id')
                            # Try to get hash from Booklore download
                            content = self.booklore_client.download_book(booklore_id)
                            if content:
                                kosync_doc_id = self.ebook_parser.get_kosync_id_from_bytes(ebook_filename, content)
                                if kosync_doc_id:
                                    logger.debug(f"[{item_id}] Computed KOSync ID from Booklore: {kosync_doc_id}")
                    except Exception as e:
                        logger.debug(f"[{item_id}] Failed to get hash from Booklore: {e}")
                
                # Fall back to cached file if hash not yet calculated
                if not kosync_doc_id:
                    cached_path = self.epub_cache_dir / ebook_filename
                    if cached_path.exists():
                        kosync_doc_id = self.ebook_parser.get_kosync_id(cached_path)
                        if kosync_doc_id:
                            logger.debug(f"[{item_id}] Computed KOSync ID from cached file: {kosync_doc_id}")
            
            if not kosync_doc_id:
                logger.warning(f"[{item_id}] Could not compute KOSync ID for '{sanitize_log_data(ebook_filename)}'")
                # Still create the book but log the issue - it can be fixed later via "Update Hash" button

            # Create book record with 'pending' status to trigger job queue.
            # Prefer ORM model when available, but keep tests runnable without SQLAlchemy.
            try:
                from src.db.models import Book as OrmBook

                book = OrmBook(
                    abs_id=item_id,
                    abs_title=title,
                    ebook_filename=ebook_filename,
                    kosync_doc_id=kosync_doc_id,
                    status='pending',
                    duration=duration,
                )
            except Exception:
                class _BookRecord:
                    def __init__(self):
                        self.abs_id = item_id
                        self.abs_title = title
                        self.ebook_filename = ebook_filename
                        self.kosync_doc_id = kosync_doc_id
                        self.status = 'pending'
                        self.duration = duration

                book = _BookRecord()

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
        1. Read Continue Listening / recently played items
        2. Mirror them into the 'Next' playlist
        3. Queue processing is handled by process_next_playlist_queue()
        """
        try:
            logger.debug("🔍 Running auto-discovery cycle...")

            # Step 1: Get recently played items
            playlist_id, _ = self.mirror_continue_listening_to_next_playlist()
            if not playlist_id:
                logger.debug("Continue listening mirror skipped - playlist unavailable")

            logger.debug("Auto-discovery mirror cycle completed")

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
            playlist = self._get_next_playlist()
            queued_unprocessed = self.get_unprocessed_items_from_next_playlist(playlist)

            return {
                'enabled': True,
                'lookback_days': self.lookback_days,
                'next_playlist': self.next_playlist_name,
                'recent_items': len(recent_items),
                # Backward-compatible key kept for existing consumers.
                'unmapped_items': len(queued_unprocessed),
                'queued_unprocessed_items': len(queued_unprocessed),
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

