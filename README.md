# yt-archiver

A command-line tool that fetches and archives video metadata from YouTube channels into a local searchable database. Find any episode by keyword, title, or date — then just click the link and watch it on YouTube.

## Features

- Archive all video metadata from any YouTube channel (title, description, upload date, duration, view count, tags)
- Full-text search across titles and descriptions
- Fast incremental syncing — only fetches new videos on subsequent runs
- No API key required
- Stores everything locally in a SQLite database

## Requirements

- Python 3.10+
- pip

## Installation

```bash
git clone https://github.com/danielboston38/yt-archiver.git
cd yt-archiver
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Usage

### Add a channel and fetch all metadata

```bash
python main.py add "https://www.youtube.com/@adriansdigitalbasement"
```

This does the initial fetch of all videos on the channel. It may take a few minutes depending on the size of the channel.

### Search for videos

```bash
python main.py search "mac classic"
python main.py search "IBM AT repair"
python main.py search "6502"
```

### List recent videos

```bash
python main.py list
python main.py list --sort view_count
python main.py list --sort title
python main.py list --limit 100 --offset 50
```

Available sort options: `upload_date`, `view_count`, `title`, `duration`

### Show full details for a video

```bash
python main.py info <video_id>
python main.py info "https://www.youtube.com/watch?v=<video_id>"
```

Displays the full title, description, tags, duration, views, and URL.

### Sync new videos

```bash
# Sync all archived channels
python main.py sync

# Sync a specific channel
python main.py sync "https://www.youtube.com/@adriansdigitalbasement"
```

Run this periodically to pick up new uploads without re-fetching everything.

### Show all archived channels

```bash
python main.py channels
```

## Data

All data is stored in `archive.db` (SQLite) in the project directory. The database is excluded from version control via `.gitignore`. To start fresh, simply delete `archive.db`.

## License

GPL-3.0 — see [LICENSE](LICENSE)
