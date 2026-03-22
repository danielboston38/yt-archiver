import yt_dlp
from typing import Generator


def _make_ydl_opts(quiet: bool = True) -> dict:
    return {
        "quiet": quiet,
        "no_warnings": quiet,
        "extract_flat": "in_playlist",  # fast: metadata only, no page per video
        "ignoreerrors": True,
    }


def fetch_channel_info(channel_url: str) -> dict:
    """Return basic channel metadata (id, name)."""
    opts = _make_ydl_opts()
    opts["playlistend"] = 1  # only need one entry to get channel info

    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(channel_url, download=False)
        if not info:
            raise ValueError(f"Could not fetch info for {channel_url}")
        return {
            "id": info.get("channel_id") or info.get("uploader_id") or info["id"],
            "name": info.get("channel") or info.get("uploader") or info.get("title"),
            "url": channel_url,
        }


def fetch_all_videos(
    channel_url: str,
    channel_id: str,
    existing_ids: set = None,
    progress_callback=None,
) -> Generator[dict, None, None]:
    """
    Yield video metadata dicts for all videos on the channel.
    Skips video IDs already in `existing_ids`.
    Calls progress_callback(fetched, total) when total becomes known.
    """
    if existing_ids is None:
        existing_ids = set()

    opts = _make_ydl_opts(quiet=True)

    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(channel_url, download=False)
        if not info:
            return

        entries = info.get("entries") or []
        total = len(entries)

        for i, entry in enumerate(entries):
            if not entry:
                continue

            video_id = entry.get("id")
            if not video_id or video_id in existing_ids:
                if progress_callback:
                    progress_callback(i + 1, total)
                continue

            # For extract_flat we get partial data — fetch full metadata per video
            # only for new videos to keep syncs fast.
            full = _fetch_video_metadata(ydl, video_id)
            if full:
                full["channel_id"] = channel_id
                yield full

            if progress_callback:
                progress_callback(i + 1, total)


def _fetch_video_metadata(ydl: yt_dlp.YoutubeDL, video_id: str) -> dict | None:
    """Fetch full metadata for a single video."""
    url = f"https://www.youtube.com/watch?v={video_id}"
    try:
        info = ydl.extract_info(url, download=False)
    except Exception:
        return None

    if not info:
        return None

    upload_date = info.get("upload_date")  # "YYYYMMDD"
    if upload_date and len(upload_date) == 8:
        upload_date = f"{upload_date[:4]}-{upload_date[4:6]}-{upload_date[6:]}"

    return {
        "id": info["id"],
        "title": info.get("title", ""),
        "description": info.get("description", ""),
        "upload_date": upload_date,
        "duration": info.get("duration"),
        "view_count": info.get("view_count"),
        "like_count": info.get("like_count"),
        "tags": info.get("tags") or [],
        "url": f"https://www.youtube.com/watch?v={info['id']}",
        "thumbnail": info.get("thumbnail"),
    }
