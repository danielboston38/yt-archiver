# yt-archiver

A tool that fetches and archives video metadata from YouTube channels into a local searchable database. Find any episode by keyword, title, or date — then click the link and watch it on YouTube. Includes a local web UI for browsing and searching in your browser.

## Features

- Archive all video metadata from any YouTube channel (title, description, upload date, duration, view count, tags)
- Full-text search across titles and descriptions
- Fast incremental syncing — only fetches new videos on subsequent runs
- Automatic detection of multi-part series (e.g. "Restoration Part 1", "Part 2", …)
- Local web UI with live search (`yt-archiver serve`)
- No API key required
- Stores everything locally in a SQLite database

## Requirements

- Python 3.10+
- pip or pipx

## Installation

### From source (recommended while in development)

```bash
git clone https://github.com/danielboston38/yt-archiver.git
cd yt-archiver
pip install -e .
```

This installs `yt-archiver` as a real command on your system — no more `python main.py`.

### With pipx (isolated install, no venv management)

```bash
pipx install git+https://github.com/danielboston38/yt-archiver.git
```

## Usage

### Add a channel and fetch all metadata

```bash
yt-archiver add "https://www.youtube.com/@adriansdigitalbasement"
```

This does the initial fetch of all videos on the channel. It may take a few minutes depending on channel size.

### Start the web UI

```bash
yt-archiver serve
```

Opens a local web server at `http://127.0.0.1:8000`. Browse channels, search videos, explore series — all in your browser. Use `--open` to auto-open the browser, or `--port` to change the port.

### Search for videos (CLI)

```bash
yt-archiver search "mac classic"
yt-archiver search "IBM AT repair"
yt-archiver search 2022
yt-archiver search "6502" 2021
```

A bare 4-digit year filters by upload date. Combine with a keyword to narrow by both.

### List recent videos

```bash
yt-archiver list
yt-archiver list --sort view_count
yt-archiver list --sort title
yt-archiver list --limit 100 --offset 50
```

Available sort options: `upload_date`, `view_count`, `title`, `duration`

### Show full details for a video

```bash
yt-archiver info <video_id>
yt-archiver info "https://www.youtube.com/watch?v=<video_id>"
```

### Sync new videos

```bash
# Sync all archived channels
yt-archiver sync

# Sync a specific channel
yt-archiver sync "https://www.youtube.com/@adriansdigitalbasement"
```

### Show all archived channels

```bash
yt-archiver channels
```

### Browse multi-part series

```bash
# List all detected series
yt-archiver series

# Show all parts of a specific series
yt-archiver series "Mac IIci Restoration"
```

Series are detected automatically from video titles using the pattern `Part N` (e.g. "Part 1", "Part 2"). If a channel uses different conventions (e.g. "Ep. 1", "#1"), extend `_SERIES_RE` in `yt_archiver/db.py`.

### Manually tag a series

```bash
yt-archiver tag-series "Plexus P20" --match "plexus p20"
```

Finds videos whose titles contain the match text and tags them as a series, ordered by upload date.

## Data

The database is stored at `~/.local/share/yt-archiver/archive.db` by default. If an `archive.db` exists in the current directory it will be used instead (backwards compatibility). Override with the `YT_ARCHIVER_DB` environment variable:

```bash
YT_ARCHIVER_DB=/my/custom/path/archive.db yt-archiver serve
```

## License

GPL-3.0 — see [LICENSE](LICENSE)
