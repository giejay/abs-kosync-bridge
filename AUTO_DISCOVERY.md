# Auto-Discovery Daemon

The Auto-Discovery Daemon automatically detects recently played audiobooks in Audiobookshelf and creates sync jobs for them if an ebook is available.

## How It Works

1. **Periodic Scanning**: Every hour (configurable), the daemon checks Audiobookshelf for items that have been played recently
2. **Progress Filter**: Only considers items with at least 1% progress and less than 100% (excludes completed books)
2. **Progress Filter**: Only considers items with at least 1% progress and less than 100% (excludes completed books)
3. **Completion Filter**: Automatically excludes books marked as finished/completed in ABS
4. **Time Window**: By default, looks at items played in the last 7 days
5. **Unmapped Detection**: Identifies audiobooks not yet in the sync database
6. **Ebook Download**: Attempts to download the ebook from ABS using `/api/items/{item_id}/ebook` endpoint
7. **Job Creation**: If ebook is available, creates a sync job automatically

## Configuration

Configure the daemon using environment variables:

### AUTO_DISCOVERY_ENABLED
- **Type**: Boolean (true/false)
- **Default**: `true`
- **Description**: Enable or disable the auto-discovery daemon

### AUTO_DISCOVERY_INTERVAL_HOURS
- **Type**: Integer
- **Default**: `1`
- **Description**: How often to run the discovery scan (in hours)

### AUTO_DISCOVERY_LOOKBACK_DAYS
- **Type**: Integer
- **Default**: `7`
- **Description**: How many days back to check for recently played items

## Example Configuration

```yaml
# docker-compose.yml
environment:
  - AUTO_DISCOVERY_ENABLED=true
  - AUTO_DISCOVERY_INTERVAL_HOURS=2
  - AUTO_DISCOVERY_LOOKBACK_DAYS=14
```

## API Endpoints

### Get Status
**GET** `/api/auto-discovery/status`

Returns current status of the auto-discovery daemon:

```json
{
  "enabled": true,
  "lookback_days": 7,
  "recent_items": 15,
  "unmapped_items": 3,
  "cache_dir": "/data/epub_cache",
  "cache_size_mb": 42.5
}
```

### Trigger Manual Scan
**POST** `/api/auto-discovery/trigger`

Manually triggers an auto-discovery cycle:

```bash
curl -X POST http://localhost:5003/api/auto-discovery/trigger
```

Response:
```json
{
  "success": true,
  "message": "Auto-discovery cycle triggered"
}
```

## Workflow

```
┌─────────────────────┐
│  ABS Recently Played│
│  (Last 7 Days)      │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Filter: Progress   │
│  1% <= P < 100%     │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Filter: Not        │
│  Finished/Completed │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Check Database     │
│  Already Mapped?    │
└──────────┬──────────┘
           │ No
           ▼
┌─────────────────────┐
│  Download Ebook     │
│  from ABS           │
└──────────┬──────────┘
           │ Success
           ▼
┌─────────────────────┐
│  Create Sync Job    │
│  (Status: pending)  │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Job Queue          │
│  Processes          │
└─────────────────────┘
```

## Logs

The daemon logs its activity with the `🔍` emoji prefix:

```
2026-04-18 10:00:00 - INFO - 🔍 Auto-discovery daemon enabled (interval: 1h, lookback: 7 days)
2026-04-18 10:00:05 - INFO - 🔍 Running auto-discovery cycle...
2026-04-18 10:00:06 - INFO - 📊 Found 15 recently played items
2026-04-18 10:00:06 - INFO - 🆕 Found 3 unmapped items (out of 15 recent)
2026-04-18 10:00:07 - INFO - [abc-123] 📥 Downloading ebook: book.epub
2026-04-18 10:00:09 - INFO - [abc-123] ✅ Downloaded ebook (2.3 MB): book.epub
2026-04-18 10:00:09 - INFO - [abc-123] ✅ Created sync job for 'Book Title'
2026-04-18 10:00:15 - INFO - 🎉 Auto-discovery completed: 3 new book(s) queued for sync
```

## Benefits

1. **Zero Configuration**: Works automatically once enabled
2. **Smart Detection**: Only syncs books you're actively reading
3. **No Manual Mapping**: Eliminates the need to manually link each audiobook
4. **Efficient**: Downloads ebooks once and caches them
5. **Respectful**: Rate-limited to avoid overwhelming the server

## Troubleshooting

### No items discovered
- Check that audiobooks have ebook files attached in ABS
- Verify the time window with `AUTO_DISCOVERY_LOOKBACK_DAYS`
- Ensure items have sufficient progress (>1% and <100%)
- Verify books are not marked as finished/completed in ABS
- Check that items were played recently (within lookback period)

### Ebook download fails
- Verify ABS permissions and API token
- Check that the `/api/items/{item_id}/ebook` endpoint is accessible
- Review logs for specific HTTP error codes

### Daemon not running
- Check `AUTO_DISCOVERY_ENABLED` is set to `true`
- Review startup logs for initialization errors
- Verify ABS client is configured correctly

## Performance

- **Memory**: Minimal (~10-20 MB for daemon)
- **Network**: Downloads only when new ebooks are found
- **CPU**: Negligible - runs only hourly by default
- **Storage**: Ebooks cached in `/data/epub_cache`

## Integration with Existing Features

The auto-discovery daemon works seamlessly with:

- **Job Queue**: Uses the same queue system as manual mappings
- **Transcription**: Automatically triggers transcription for new books
- **Sync Clients**: New books sync with all configured clients (KoSync, Hardcover, etc.)
- **Suggestions**: Won't create duplicates - checks database first

