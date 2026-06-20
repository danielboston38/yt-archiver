# tubevault

A tool that fetches and archives video metadata from YouTube channels into a local searchable database. Find any episode by keyword, title, or date — then click the link and watch it on YouTube. Includes a local web UI for browsing and searching in your browser.

## Features

- Archive all video metadata from any YouTube channel (title, description, upload date, duration, view count, tags)
- Full-text search across titles and descriptions
- Fast incremental syncing — only fetches new videos on subsequent runs
- Automatic detection of multi-part series (e.g. "Restoration Part 1", "Part 2", …)
- Local web UI with live search (`tubevault serve`)
- No API key required
- Stores everything locally in a SQLite database

## Requirements

- Python 3.10+
- pip or pipx

## Installation

> **Note:** This project was renamed from `yt-archiver` to `tubevault` (the name `yt-archiver` was already taken on PyPI). The GitHub repo URL still uses the old name, but the package, the command, and the import are all `tubevault`.

### From PyPI

```bash
pip install tubevault
```

### With pipx (isolated install, no venv management)

```bash
pipx install tubevault
```

### From source

```bash
git clone https://github.com/danielboston38/yt-archiver.git
cd yt-archiver
pip3 install -e .
```

> **macOS note:** the python.org installer only creates `pip3`, not a bare `pip` — so use `pip3` (or `python3 -m pip`) for the commands above. The PyPI/pipx installs are recommended for most users.

## Usage

### Add a channel and fetch all metadata

```bash
tubevault add "https://www.youtube.com/@adriansdigitalbasement"
```

This does the initial fetch of all videos on the channel. It may take a few minutes depending on channel size.

### Start the web UI

```bash
tubevault serve
```

Opens a local web server at `http://127.0.0.1:8000`. Browse channels, search videos, explore series — all in your browser. Use `--open` to auto-open the browser, or `--port` to change the port.

### Search for videos (CLI)

```bash
tubevault search "mac classic"
tubevault search "IBM AT repair"
tubevault search 2022
tubevault search "6502" 2021
```

A bare 4-digit year filters by upload date. Combine with a keyword to narrow by both.

### List recent videos

```bash
tubevault list
tubevault list --sort view_count
tubevault list --sort title
tubevault list --limit 100 --offset 50
```

Available sort options: `upload_date`, `view_count`, `title`, `duration`

### Show full details for a video

```bash
tubevault info <video_id>
tubevault info "https://www.youtube.com/watch?v=<video_id>"
```

### Sync new videos

```bash
# Sync all archived channels
tubevault sync

# Sync a specific channel
tubevault sync "https://www.youtube.com/@adriansdigitalbasement"
```

### Show all archived channels

```bash
tubevault channels
```

### Browse multi-part series

```bash
# List all detected series
tubevault series

# Show all parts of a specific series
tubevault series "Mac IIci Restoration"
```

Series are detected automatically from video titles using the pattern `Part N` (e.g. "Part 1", "Part 2"). If a channel uses different conventions (e.g. "Ep. 1", "#1"), extend `_SERIES_RE` in `tubevault/db.py`.

### Manually tag a series

```bash
tubevault tag-series "Plexus P20" --match "plexus p20"
```

Finds videos whose titles contain the match text and tags them as a series, ordered by upload date.

## Data

The database is stored at `~/.local/share/tubevault/archive.db` by default. If an `archive.db` exists in the current directory it will be used instead (backwards compatibility). Override with the `TUBEVAULT_DB` environment variable:

```bash
TUBEVAULT_DB=/my/custom/path/archive.db tubevault serve
```

## License

GPL-3.0 — see [LICENSE](LICENSE)
