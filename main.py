#!/usr/bin/env python3
"""
yt-archiver — YouTube channel metadata archiver & search tool.

Usage:
  python main.py add <channel_url>       Add a channel and do initial fetch
  python main.py sync [channel_url]      Sync new videos (all channels if no URL given)
  python main.py search <query>          Full-text search across all archived channels
  python main.py list [--sort <field>]   List videos (most recent first)
  python main.py channels                Show all archived channels
  python main.py info <video_id_or_url>  Show full details for a single video
  python main.py series                  List detected multi-part series
  python main.py series <name>           Show all parts of a specific series
  python main.py tag-series <name> --match <text>  Manually tag videos as a series
"""

import argparse
import json
import sys

from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich import print as rprint
from rich.panel import Panel
from rich.text import Text

import db
import fetcher

console = Console()


# ── helpers ──────────────────────────────────────────────────────────────────

def format_duration(seconds: int | None) -> str:
    if seconds is None:
        return "—"
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m}:{s:02d}"


def format_count(n: int | None) -> str:
    if n is None:
        return "—"
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n/1_000:.1f}K"
    return str(n)


def video_table(rows, title: str = "") -> Table:
    table = Table(title=title, show_lines=False, highlight=True)
    table.add_column("Date", style="dim", width=11)
    table.add_column("Title", style="bold", no_wrap=False, max_width=55)
    table.add_column("Duration", justify="right", width=9)
    table.add_column("Views", justify="right", width=8)
    table.add_column("URL", style="cyan", no_wrap=True)

    for row in rows:
        table.add_row(
            row["upload_date"] or "—",
            row["title"],
            format_duration(row["duration"]),
            format_count(row["view_count"]),
            row["url"],
        )
    return table


# ── commands ─────────────────────────────────────────────────────────────────

def cmd_add(channel_url: str):
    db.init_db()

    console.print(f"[bold]Fetching channel info for[/bold] {channel_url} …")
    try:
        channel = fetcher.fetch_channel_info(channel_url)
    except ValueError as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)

    db.upsert_channel(channel["id"], channel["name"], channel["url"])
    console.print(f"[green]Channel:[/green] {channel['name']} ({channel['id']})")

    _sync_channel(channel["id"], channel["url"])


def cmd_sync(channel_url: str | None):
    db.init_db()

    if channel_url:
        try:
            channel = fetcher.fetch_channel_info(channel_url)
        except ValueError as e:
            console.print(f"[red]Error:[/red] {e}")
            sys.exit(1)
        db.upsert_channel(channel["id"], channel["name"], channel["url"])
        channels = [db.get_channel(channel["id"])]
    else:
        channels = db.get_all_channels()
        if not channels:
            console.print("[yellow]No channels archived yet. Use[/yellow] [bold]add[/bold] [yellow]first.[/yellow]")
            sys.exit(0)

    for ch in channels:
        console.print(f"\n[bold]Syncing:[/bold] {ch['name']}")
        _sync_channel(ch["id"], ch["url"])


def _sync_channel(channel_id: str, channel_url: str):
    existing = db.get_existing_video_ids(channel_id)
    new_count = 0
    errors = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console,
        transient=True,
    ) as progress:
        task = progress.add_task("Scanning channel…", total=None)

        def on_progress(fetched, total):
            progress.update(task, total=total, completed=fetched,
                            description=f"Processing {fetched}/{total} videos…")

        for video in fetcher.fetch_all_videos(
            channel_url, channel_id, existing_ids=existing,
            progress_callback=on_progress
        ):
            try:
                db.upsert_video(video)
                new_count += 1
            except Exception as e:
                errors += 1

    db.mark_channel_synced(channel_id)
    total_stored = db.get_video_count(channel_id)
    console.print(
        f"[green]Done.[/green] {new_count} new video(s) added. "
        f"{total_stored} total in database."
        + (f" [red]{errors} error(s).[/red]" if errors else "")
    )


def cmd_search(terms: list, limit: int = 20):
    import re
    db.init_db()

    year = None
    query_parts = []
    for term in terms:
        if re.match(r'^\d{4}$', term) and 2000 <= int(term) <= 2099:
            year = term
        else:
            query_parts.append(term)
    query = " ".join(query_parts) or None

    if not query and not year:
        console.print("[yellow]Provide a search query, a year, or both.[/yellow]")
        return

    rows = db.search_videos(query=query, year=year, limit=limit)

    if not rows:
        label = " ".join(filter(None, [f'"{query}"' if query else None, year]))
        console.print(f"[yellow]No results for[/yellow] [bold]{label}[/bold]")
        return

    label = " ".join(filter(None, [f'"{query}"' if query else None, year]))
    console.print(video_table(rows, title=f"Search: {label} — {len(rows)} result(s)"))


def cmd_list(sort: str = "upload_date", limit: int = 50, offset: int = 0):
    db.init_db()
    rows = db.list_videos(sort=sort, limit=limit, offset=offset)
    if not rows:
        console.print("[yellow]No videos archived yet.[/yellow]")
        return
    total = db.get_video_count()
    console.print(video_table(rows, title=f"Videos ({offset+1}–{offset+len(rows)} of {total})"))


def cmd_channels():
    db.init_db()
    channels = db.get_all_channels()
    if not channels:
        console.print("[yellow]No channels archived.[/yellow]")
        return

    table = Table(title="Archived Channels", highlight=True)
    table.add_column("Name", style="bold")
    table.add_column("ID", style="dim")
    table.add_column("Videos", justify="right")
    table.add_column("Last Synced", style="dim")
    table.add_column("URL", style="cyan")

    for ch in channels:
        count = db.get_video_count(ch["id"])
        table.add_row(
            ch["name"],
            ch["id"],
            str(count),
            ch["last_synced"] or "never",
            ch["url"],
        )
    console.print(table)


def cmd_series(name: str = None):
    db.init_db()
    if name:
        rows = db.get_series_videos(name)
        if not rows:
            console.print(f"[yellow]No series found matching[/yellow] [bold]{name!r}[/bold]")
            return
        console.print(video_table(rows, title=f'Series: "{name}" — {len(rows)} part(s)'))
    else:
        series_list = db.get_series()
        if not series_list:
            console.print("[yellow]No multi-part series detected yet.[/yellow]")
            return
        table = Table(title="Detected Series", highlight=True)
        table.add_column("Series", style="bold", no_wrap=False, max_width=50)
        table.add_column("Parts", justify="right", width=7)
        table.add_column("First Upload", style="dim", width=12)
        for s in series_list:
            parts_str = (
                f"{s['part_count']}"
                if s['first_part'] == s['last_part']
                else f"{s['part_count']} (pt {s['first_part']}–{s['last_part']})"
            )
            table.add_row(s["series_name"], parts_str, s["first_date"] or "—")
        console.print(table)


def cmd_tag_series(series_name: str, match: str, channel_id: str = None):
    db.init_db()
    rows = db.find_videos_by_title_match(match, channel_id=channel_id)
    if not rows:
        console.print(f"[yellow]No videos found matching[/yellow] [bold]{match!r}[/bold]")
        return

    console.print(video_table(
        rows,
        title=f'Videos matching "{match}" — {len(rows)} result(s)',
    ))
    console.print(f'\nThese will be tagged as series [bold]{series_name!r}[/bold], '
                  f'ordered by upload date (oldest = part 1).')
    confirm = console.input("Proceed? [y/N] ").strip().lower()
    if confirm != "y":
        console.print("[yellow]Cancelled.[/yellow]")
        return

    db.tag_series(series_name, [row["id"] for row in rows])
    console.print(f"[green]Tagged {len(rows)} video(s) as series[/green] [bold]{series_name!r}[/bold].")


def cmd_info(video_ref: str):
    db.init_db()
    # Accept full URL or bare video ID
    video_id = video_ref.split("v=")[-1].split("&")[0] if "youtube.com" in video_ref else video_ref

    with db.get_connection() as conn:
        row = conn.execute("SELECT * FROM videos WHERE id=?", (video_id,)).fetchone()

    if not row:
        console.print(f"[red]Video not found:[/red] {video_id}")
        sys.exit(1)

    tags = json.loads(row["tags"] or "[]")
    text = Text()
    text.append(f"{row['title']}\n", style="bold white")
    text.append(f"\nDate:     {row['upload_date'] or '—'}\n", style="dim")
    text.append(f"Duration: {format_duration(row['duration'])}\n", style="dim")
    text.append(f"Views:    {format_count(row['view_count'])}\n", style="dim")
    text.append(f"Likes:    {format_count(row['like_count'])}\n", style="dim")
    text.append(f"URL:      {row['url']}\n\n", style="cyan")
    if tags:
        text.append("Tags: " + ", ".join(tags) + "\n\n", style="dim")
    text.append(row["description"] or "(no description)")
    console.print(Panel(text, title="[bold]Video Info[/bold]", expand=False))


# ── entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="YouTube channel archiver & search",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_add = sub.add_parser("add", help="Add a channel and fetch all metadata")
    p_add.add_argument("channel_url")

    p_sync = sub.add_parser("sync", help="Sync new videos (all channels if no URL given)")
    p_sync.add_argument("channel_url", nargs="?", default=None)

    p_search = sub.add_parser("search", help="Full-text search — query, year, or both")
    p_search.add_argument("terms", nargs="+", help="Search query and/or a 4-digit year")
    p_search.add_argument("--limit", type=int, default=20)

    p_list = sub.add_parser("list", help="List archived videos")
    p_list.add_argument("--sort", choices=["upload_date", "view_count", "title", "duration"],
                        default="upload_date")
    p_list.add_argument("--limit", type=int, default=50)
    p_list.add_argument("--offset", type=int, default=0)

    sub.add_parser("channels", help="Show all archived channels")

    p_info = sub.add_parser("info", help="Show full details for a video")
    p_info.add_argument("video_ref", help="Video ID or YouTube URL")

    p_series = sub.add_parser("series", help="List detected multi-part series")
    p_series.add_argument("name", nargs="?", default=None,
                          help="Series name to show all parts (omit to list all series)")

    p_tag = sub.add_parser("tag-series", help="Manually tag videos as a series by title match")
    p_tag.add_argument("series_name", help="Name to assign to the series")
    p_tag.add_argument("--match", required=True, help="Title substring to match")
    p_tag.add_argument("--channel", default=None, metavar="CHANNEL_ID",
                       help="Restrict search to a specific channel ID")

    args = parser.parse_args()

    if args.command == "add":
        cmd_add(args.channel_url)
    elif args.command == "sync":
        cmd_sync(args.channel_url)
    elif args.command == "search":
        cmd_search(args.terms, limit=args.limit)
    elif args.command == "list":
        cmd_list(sort=args.sort, limit=args.limit, offset=args.offset)
    elif args.command == "channels":
        cmd_channels()
    elif args.command == "info":
        cmd_info(args.video_ref)
    elif args.command == "series":
        cmd_series(args.name)
    elif args.command == "tag-series":
        cmd_tag_series(args.series_name, args.match, channel_id=args.channel)


if __name__ == "__main__":
    main()
