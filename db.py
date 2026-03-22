import sqlite3
import json
from pathlib import Path
from datetime import datetime

DB_PATH = Path(__file__).parent / "archive.db"


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


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
                    video_id UNINDEXED,
                    title,
                    description,
                    tags,
                    content=videos,
                    content_rowid=rowid
                );

            CREATE TRIGGER IF NOT EXISTS videos_ai AFTER INSERT ON videos BEGIN
                INSERT INTO videos_fts(rowid, video_id, title, description, tags)
                VALUES (new.rowid, new.id, new.title, new.description, new.tags);
            END;

            CREATE TRIGGER IF NOT EXISTS videos_au AFTER UPDATE ON videos BEGIN
                INSERT INTO videos_fts(videos_fts, rowid, video_id, title, description, tags)
                VALUES ('delete', old.rowid, old.id, old.title, old.description, old.tags);
                INSERT INTO videos_fts(rowid, video_id, title, description, tags)
                VALUES (new.rowid, new.id, new.title, new.description, new.tags);
            END;
        """)


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
    with get_connection() as conn:
        conn.execute("""
            INSERT INTO videos
                (id, channel_id, title, description, upload_date, duration,
                 view_count, like_count, tags, url, thumbnail)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                title=excluded.title,
                description=excluded.description,
                view_count=excluded.view_count,
                like_count=excluded.like_count,
                tags=excluded.tags
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


def search_videos(query: str, channel_id: str = None, limit: int = 20):
    """Full-text search across title, description, and tags."""
    with get_connection() as conn:
        if channel_id:
            rows = conn.execute("""
                SELECT v.*, rank
                FROM videos_fts
                JOIN videos v ON v.id = videos_fts.video_id
                WHERE videos_fts MATCH ? AND v.channel_id = ?
                ORDER BY rank
                LIMIT ?
            """, (query, channel_id, limit)).fetchall()
        else:
            rows = conn.execute("""
                SELECT v.*, rank
                FROM videos_fts
                JOIN videos v ON v.id = videos_fts.video_id
                WHERE videos_fts MATCH ?
                ORDER BY rank
                LIMIT ?
            """, (query, limit)).fetchall()
        return rows


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


def get_existing_video_ids(channel_id: str) -> set:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT id FROM videos WHERE channel_id=?", (channel_id,)
        ).fetchall()
        return {row["id"] for row in rows}
