import re
import sqlite3
import json
from pathlib import Path
from datetime import datetime

# Matches "Title Part 2", "Title - Part 2", "Title, Part 2" (case-insensitive)
_SERIES_RE = re.compile(r'^(.*?)\s*[,\-–]?\s*[Pp]art\.?\s*(\d+)\s*$')


def _detect_series(title: str):
    """Return (series_name, part_number) if title matches a series pattern, else (None, None)."""
    m = _SERIES_RE.match(title.strip())
    if m:
        return m.group(1).strip(), int(m.group(2))
    return None, None

DB_PATH = Path(__file__).parent / "archive.db"


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _migrate_db(conn):
    """Add columns introduced after the initial schema."""
    for col, typedef in [("series_name", "TEXT"), ("series_part", "INTEGER")]:
        try:
            conn.execute(f"ALTER TABLE videos ADD COLUMN {col} {typedef}")
        except sqlite3.OperationalError:
            pass  # column already exists

    # Backfill series detection for any videos not yet processed
    rows = conn.execute(
        "SELECT rowid, title FROM videos WHERE series_name IS NULL"
    ).fetchall()
    for row in rows:
        name, part = _detect_series(row["title"])
        if name:
            conn.execute(
                "UPDATE videos SET series_name=?, series_part=? WHERE rowid=?",
                (name, part, row["rowid"])
            )


def init_db():
    with get_connection() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS channels (
                id          TEXT PRIMARY KEY,
                name        TEXT NOT NULL,
                url         TEXT NOT NULL,
                last_synced TEXT
            );

            CREATE TABLE IF NOT EXISTS videos (
                id          TEXT PRIMARY KEY,
                channel_id  TEXT NOT NULL,
                title       TEXT NOT NULL,
                description TEXT,
                upload_date TEXT,
                duration    INTEGER,
                view_count  INTEGER,
                like_count  INTEGER,
                tags        TEXT,
                url         TEXT NOT NULL,
                thumbnail   TEXT,
                FOREIGN KEY (channel_id) REFERENCES channels(id)
            );

            CREATE VIRTUAL TABLE IF NOT EXISTS videos_fts
                USING fts5(
                    title,
                    description,
                    tags,
                    content=videos,
                    content_rowid=rowid
                );

            CREATE TRIGGER IF NOT EXISTS videos_ai AFTER INSERT ON videos BEGIN
                INSERT INTO videos_fts(rowid, title, description, tags)
                VALUES (new.rowid, new.title, new.description, new.tags);
            END;

            CREATE TRIGGER IF NOT EXISTS videos_au AFTER UPDATE ON videos BEGIN
                INSERT INTO videos_fts(videos_fts, rowid, title, description, tags)
                VALUES ('delete', old.rowid, old.title, old.description, old.tags);
                INSERT INTO videos_fts(rowid, title, description, tags)
                VALUES (new.rowid, new.title, new.description, new.tags);
            END;
        """)
        _migrate_db(conn)


def upsert_channel(channel_id: str, name: str, url: str):
    with get_connection() as conn:
        conn.execute("""
            INSERT INTO channels (id, name, url)
            VALUES (?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET name=excluded.name, url=excluded.url
        """, (channel_id, name, url))


def mark_channel_synced(channel_id: str):
    with get_connection() as conn:
        conn.execute(
            "UPDATE channels SET last_synced=? WHERE id=?",
            (datetime.utcnow().isoformat(), channel_id)
        )


def upsert_video(video: dict):
    tags_json = json.dumps(video.get("tags") or [])
    series_name, series_part = _detect_series(video["title"])
    with get_connection() as conn:
        conn.execute("""
            INSERT INTO videos
                (id, channel_id, title, description, upload_date, duration,
                 view_count, like_count, tags, url, thumbnail, series_name, series_part)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                title=excluded.title,
                description=excluded.description,
                view_count=excluded.view_count,
                like_count=excluded.like_count,
                tags=excluded.tags,
                series_name=COALESCE(excluded.series_name, series_name),
                series_part=COALESCE(excluded.series_part, series_part)
        """, (
            video["id"],
            video["channel_id"],
            video["title"],
            video.get("description", ""),
            video.get("upload_date"),
            video.get("duration"),
            video.get("view_count"),
            video.get("like_count"),
            tags_json,
            video["url"],
            video.get("thumbnail"),
            series_name,
            series_part,
        ))


def get_channel(channel_id: str):
    with get_connection() as conn:
        return conn.execute(
            "SELECT * FROM channels WHERE id=?", (channel_id,)
        ).fetchone()


def get_all_channels():
    with get_connection() as conn:
        return conn.execute("SELECT * FROM channels").fetchall()


def get_video_count(channel_id: str = None):
    with get_connection() as conn:
        if channel_id:
            row = conn.execute(
                "SELECT COUNT(*) FROM videos WHERE channel_id=?", (channel_id,)
            ).fetchone()
        else:
            row = conn.execute("SELECT COUNT(*) FROM videos").fetchone()
        return row[0]


def search_videos(query: str = None, channel_id: str = None, limit: int = 20, year: str = None):
    """Full-text search across title, description, and tags. Optionally filter by year."""
    with get_connection() as conn:
        # Year-only: simple date filter, no FTS
        if not query and year:
            if channel_id:
                return conn.execute("""
                    SELECT * FROM videos
                    WHERE upload_date LIKE ? AND channel_id = ?
                    ORDER BY upload_date ASC
                    LIMIT ?
                """, (f"{year}%", channel_id, limit)).fetchall()
            else:
                return conn.execute("""
                    SELECT * FROM videos
                    WHERE upload_date LIKE ?
                    ORDER BY upload_date ASC
                    LIMIT ?
                """, (f"{year}%", limit)).fetchall()

        # FTS with optional year filter
        year_clause = f"AND v.upload_date LIKE '{year}%'" if year else ""
        if channel_id:
            return conn.execute(f"""
                SELECT v.*, rank
                FROM videos_fts
                JOIN videos v ON v.rowid = videos_fts.rowid
                WHERE videos_fts MATCH ? AND v.channel_id = ? {year_clause}
                ORDER BY rank
                LIMIT ?
            """, (query, channel_id, limit)).fetchall()
        else:
            return conn.execute(f"""
                SELECT v.*, rank
                FROM videos_fts
                JOIN videos v ON v.rowid = videos_fts.rowid
                WHERE videos_fts MATCH ? {year_clause}
                ORDER BY rank
                LIMIT ?
            """, (query, limit)).fetchall()


def list_videos(channel_id: str = None, limit: int = 50, offset: int = 0,
                sort: str = "upload_date"):
    allowed_sorts = {"upload_date", "view_count", "title", "duration"}
    if sort not in allowed_sorts:
        sort = "upload_date"
    with get_connection() as conn:
        if channel_id:
            return conn.execute(f"""
                SELECT * FROM videos WHERE channel_id=?
                ORDER BY {sort} DESC
                LIMIT ? OFFSET ?
            """, (channel_id, limit, offset)).fetchall()
        else:
            return conn.execute(f"""
                SELECT * FROM videos
                ORDER BY {sort} DESC
                LIMIT ? OFFSET ?
            """, (limit, offset)).fetchall()


def get_series(channel_id: str = None):
    """Return all detected series grouped by (channel_id, series_name)."""
    with get_connection() as conn:
        if channel_id:
            return conn.execute("""
                SELECT series_name, channel_id, COUNT(*) as part_count,
                       MIN(series_part) as first_part, MAX(series_part) as last_part,
                       MIN(upload_date) as first_date
                FROM videos
                WHERE series_name IS NOT NULL AND channel_id = ?
                GROUP BY channel_id, series_name
                ORDER BY first_date DESC
            """, (channel_id,)).fetchall()
        else:
            return conn.execute("""
                SELECT series_name, channel_id, COUNT(*) as part_count,
                       MIN(series_part) as first_part, MAX(series_part) as last_part,
                       MIN(upload_date) as first_date
                FROM videos
                WHERE series_name IS NOT NULL
                GROUP BY channel_id, series_name
                ORDER BY first_date DESC
            """).fetchall()


def get_series_videos(series_name: str, channel_id: str = None):
    """Return all videos in a series ordered by part number."""
    with get_connection() as conn:
        if channel_id:
            return conn.execute("""
                SELECT * FROM videos
                WHERE series_name = ? AND channel_id = ?
                ORDER BY series_part ASC
            """, (series_name, channel_id)).fetchall()
        else:
            return conn.execute("""
                SELECT * FROM videos
                WHERE series_name = ?
                ORDER BY series_part ASC
            """, (series_name,)).fetchall()


def find_videos_by_title_match(match: str, channel_id: str = None):
    """Return videos whose title contains `match` (case-insensitive), ordered by upload_date."""
    pattern = f"%{match}%"
    with get_connection() as conn:
        if channel_id:
            return conn.execute(
                "SELECT * FROM videos WHERE title LIKE ? AND channel_id = ? ORDER BY upload_date ASC",
                (pattern, channel_id),
            ).fetchall()
        else:
            return conn.execute(
                "SELECT * FROM videos WHERE title LIKE ? ORDER BY upload_date ASC",
                (pattern,),
            ).fetchall()


def tag_series(series_name: str, video_ids: list):
    """Assign series_name and sequential part numbers (oldest first) to the given videos."""
    with get_connection() as conn:
        for part, vid_id in enumerate(video_ids, start=1):
            conn.execute(
                "UPDATE videos SET series_name=?, series_part=? WHERE id=?",
                (series_name, part, vid_id),
            )


def get_existing_video_ids(channel_id: str) -> set:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT id FROM videos WHERE channel_id=?", (channel_id,)
        ).fetchall()
        return {row["id"] for row in rows}
