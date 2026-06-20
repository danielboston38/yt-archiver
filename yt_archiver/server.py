import json
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from yt_archiver import db

_BASE = Path(__file__).parent

app = FastAPI(title="yt-archiver")
app.mount("/static", StaticFiles(directory=_BASE / "static"), name="static")
templates = Jinja2Templates(directory=_BASE / "templates")


def _fmt_duration(seconds):
    if seconds is None:
        return "—"
    h, rem = divmod(int(seconds), 3600)
    m, s = divmod(rem, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def _fmt_count(n):
    if n is None:
        return "—"
    n = int(n)
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n/1_000:.1f}K"
    return str(n)


templates.env.filters["duration"] = _fmt_duration
templates.env.filters["count"] = _fmt_count


@app.get("/", response_class=HTMLResponse)
async def channels_page(request: Request):
    db.init_db()
    channels = []
    for ch in db.get_all_channels():
        channels.append({
            "id": ch["id"],
            "name": ch["name"],
            "url": ch["url"],
            "last_synced": (ch["last_synced"] or "")[:10] or "never",
            "video_count": db.get_video_count(ch["id"]),
        })
    return templates.TemplateResponse(request, "channels.html", {
        "channels": channels,
    })


@app.get("/channels/{channel_id}", response_class=HTMLResponse)
async def channel_videos(
    request: Request, channel_id: str,
    sort: str = "upload_date", limit: int = 50, offset: int = 0,
):
    db.init_db()
    channel = db.get_channel(channel_id)
    if not channel:
        return HTMLResponse("Channel not found", status_code=404)
    rows = db.list_videos(channel_id=channel_id, sort=sort, limit=limit, offset=offset)
    total = db.get_video_count(channel_id)
    return templates.TemplateResponse(request, "videos.html", {
        "page_title": channel["name"],
        "videos": [dict(r) for r in rows],
        "total": total,
        "sort": sort,
        "limit": limit,
        "offset": offset,
        "channel_id": channel_id,
        "query": "",
    })


@app.get("/videos", response_class=HTMLResponse)
async def all_videos(
    request: Request,
    sort: str = "upload_date", limit: int = 50, offset: int = 0,
):
    db.init_db()
    rows = db.list_videos(sort=sort, limit=limit, offset=offset)
    total = db.get_video_count()
    return templates.TemplateResponse(request, "videos.html", {
        "page_title": "All Videos",
        "videos": [dict(r) for r in rows],
        "total": total,
        "sort": sort,
        "limit": limit,
        "offset": offset,
        "channel_id": None,
        "query": "",
    })


@app.get("/video/{video_id}", response_class=HTMLResponse)
async def video_detail(request: Request, video_id: str):
    db.init_db()
    with db.get_connection() as conn:
        row = conn.execute("SELECT * FROM videos WHERE id=?", (video_id,)).fetchone()
    if not row:
        return HTMLResponse("Video not found", status_code=404)
    video = dict(row)
    video["tags"] = json.loads(video.get("tags") or "[]")
    series_videos = []
    if video.get("series_name"):
        series_videos = [dict(r) for r in db.get_series_videos(video["series_name"])]
    return templates.TemplateResponse(request, "video.html", {
        "video": video,
        "series_videos": series_videos,
        "query": "",
    })


@app.get("/search", response_class=HTMLResponse)
async def search_page(request: Request, q: str = "", year: str = ""):
    db.init_db()
    query = q.strip() or None
    year_filter = year.strip() or None
    videos = []
    if query or year_filter:
        rows = db.search_videos(query=query, year=year_filter, limit=50)
        videos = [dict(r) for r in rows]
    return templates.TemplateResponse(request, "search.html", {
        "query": q,
        "year": year,
        "videos": videos,
    })


@app.get("/api/search")
async def api_search(q: str = "", year: str = "", limit: int = 30):
    db.init_db()
    query = q.strip() or None
    year_filter = year.strip() or None
    if not query and not year_filter:
        return []
    rows = db.search_videos(query=query, year=year_filter, limit=limit)
    return [dict(r) for r in rows]


@app.get("/series", response_class=HTMLResponse)
async def series_list(request: Request):
    db.init_db()
    series = [dict(s) for s in db.get_series()]
    return templates.TemplateResponse(request, "series.html", {
        "series_list": series,
        "series_name": None,
        "videos": [],
        "query": "",
    })


@app.get("/series/{series_name:path}", response_class=HTMLResponse)
async def series_detail(request: Request, series_name: str):
    db.init_db()
    videos = [dict(v) for v in db.get_series_videos(series_name)]
    return templates.TemplateResponse(request, "series.html", {
        "series_list": [],
        "series_name": series_name,
        "videos": videos,
        "query": "",
    })
