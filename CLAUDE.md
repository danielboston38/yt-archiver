# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development setup

```bash
pip3 install -e .       # install in editable mode; exposes `tubevault` CLI
tubevault serve       # start web UI at http://127.0.0.1:8000
python main.py          # dev shim — equivalent to `tubevault`
```

There are no tests or linters configured yet.

## Architecture

The package lives entirely in `tubevault/`. Entry point is `tubevault/cli.py:main`, registered as the `tubevault` console script.

**Data flow:**
1. `fetcher.py` — wraps `yt-dlp`. `fetch_all_videos` does a flat playlist extraction first (fast, gets IDs), then calls `_fetch_video_metadata` per video (slower, gets full metadata). YouTube channels return tabs (Videos, Shorts, Live) as nested playlists that must be flattened one level.
2. `db.py` — SQLite via stdlib `sqlite3`. FTS5 virtual table (`videos_fts`) is kept in sync via `AFTER INSERT` / `AFTER UPDATE` triggers on the `videos` table. The `sort` column in `list_videos` is allowlisted to prevent injection.
3. `server.py` — FastAPI + Jinja2. `/api/search` returns JSON for the live-search JS; all other routes return HTML. Formatting helpers (`_fmt_duration`, `_fmt_count`) are registered as Jinja2 filters.
4. `cli.py` — argparse dispatcher; uses `rich` for all terminal output.

**Database path resolution** (in `db.py:_get_db_path`):
- `TUBEVAULT_DB` env var → override path (legacy `YT_ARCHIVER_DB` still honored as a fallback)
- `./archive.db` exists → use it (backwards compat)
- Otherwise → `~/.local/share/tubevault/archive.db` (via `platformdirs`)

**Series detection:** `_SERIES_RE` in `db.py` matches titles like "Restoration Part 1". Runs automatically on `upsert_video` and during `_migrate_db` for existing rows. To extend for other conventions (e.g. "Ep. 1"), modify `_SERIES_RE`. Manual tagging is done with `tag-series` / `db.tag_series`.

**Schema migrations** use `ALTER TABLE … ADD COLUMN` wrapped in try/except (ignores "column already exists"). No migration framework.
