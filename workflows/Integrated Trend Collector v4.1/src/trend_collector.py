from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

import requests
import yaml
from google.oauth2 import service_account
from googleapiclient.discovery import build


SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


class TrendCollectorError(Exception):
    pass


@dataclass
class VideoCandidate:
    video_id: str
    title: str
    description: str
    published_at: str
    channel_id: str
    channel_title: str


def load_settings(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def ensure_dirs(settings: dict[str, Any], base_dir: Path) -> tuple[Path, Path]:
    state_dir = base_dir / settings["state_dir"]
    logs_dir = base_dir / settings["logs_dir"]
    state_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    return state_dir, logs_dir


def load_service_account_info() -> dict[str, Any]:
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not raw:
        raise TrendCollectorError("GOOGLE_SERVICE_ACCOUNT_JSON is required")
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise TrendCollectorError("GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON") from exc


def build_sheets_service() -> Any:
    credentials = service_account.Credentials.from_service_account_info(
        load_service_account_info(), scopes=SCOPES
    )
    return build("sheets", "v4", credentials=credentials, cache_discovery=False)


def read_channels(settings: dict[str, Any]) -> list[dict[str, str]]:
    service = build_sheets_service()
    response = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=settings["google_sheet_id"], range=settings["channel_sheet_range"])
        .execute()
    )
    values = response.get("values", [])
    if not values:
        return []

    header = [cell.strip() for cell in values[0]]
    channels: list[dict[str, str]] = []
    for row in values[1:]:
        record = {header[idx]: row[idx] for idx in range(min(len(header), len(row)))}
        channel_id = (
            record.get("youtube_channel_id")
            or record.get("channel_id")
            or record.get("channelId")
            or ""
        ).strip()
        if not channel_id:
            continue
        channels.append(
            {
                "youtube_channel_id": channel_id,
                "channel_name": (
                    record.get("channel_name")
                    or record.get("channelTitle")
                    or record.get("title")
                    or ""
                ).strip(),
            }
        )
    return channels


def iso_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_iso8601_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def load_processed_state(state_file: Path, retention_days: int) -> dict[str, dict[str, str]]:
    if not state_file.exists():
        return {}

    with state_file.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)

    items = payload.get("items", {})
    cutoff = datetime.now(UTC) - timedelta(days=retention_days)
    kept: dict[str, dict[str, str]] = {}
    for video_id, entry in items.items():
        processed_at = entry.get("processed_at")
        if not processed_at:
            continue
        if parse_iso8601_datetime(processed_at) >= cutoff:
            kept[video_id] = entry
    return kept


def save_processed_state(state_file: Path, items: dict[str, dict[str, str]]) -> None:
    payload = {
        "updated_at": iso_now(),
        "items": dict(sorted(items.items(), key=lambda pair: pair[0])),
    }
    with state_file.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
        fh.write("\n")


def fetch_channel_feed(channel_id: str) -> list[VideoCandidate]:
    url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    root = ET.fromstring(response.text)
    namespace = {
        "atom": "http://www.w3.org/2005/Atom",
        "yt": "http://www.youtube.com/xml/schemas/2015",
        "media": "http://search.yahoo.com/mrss/",
    }
    entries: list[VideoCandidate] = []
    for entry in root.findall("atom:entry", namespace):
        video_id = entry.findtext("yt:videoId", default="", namespaces=namespace).strip()
        title = entry.findtext("atom:title", default="", namespaces=namespace).strip()
        published_at = entry.findtext("atom:published", default="", namespaces=namespace).strip()
        description = entry.findtext(
            "media:group/media:description", default="", namespaces=namespace
        ).strip()
        channel_title = entry.findtext("author/atom:name", default="", namespaces=namespace).strip()
        if not video_id:
            continue
        entries.append(
            VideoCandidate(
                video_id=video_id,
                title=title,
                description=description,
                published_at=published_at,
                channel_id=channel_id,
                channel_title=channel_title,
            )
        )
    return entries


def dedupe_candidates(candidates: list[VideoCandidate]) -> list[VideoCandidate]:
    unique: dict[str, VideoCandidate] = {}
    for candidate in sorted(candidates, key=lambda item: item.published_at, reverse=True):
        unique.setdefault(candidate.video_id, candidate)
    return list(unique.values())


def fetch_video_details(video_ids: list[str], api_key: str) -> dict[str, dict[str, Any]]:
    if not video_ids:
        return {}
    results: dict[str, dict[str, Any]] = {}
    for start in range(0, len(video_ids), 50):
        batch = video_ids[start : start + 50]
        response = requests.get(
            "https://www.googleapis.com/youtube/v3/videos",
            params={
                "part": "statistics,snippet,contentDetails",
                "id": ",".join(batch),
                "key": api_key,
            },
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        for item in payload.get("items", []):
            results[item["id"]] = item
    return results


def parse_duration_seconds(duration: str) -> int:
    match = re.fullmatch(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", duration)
    if not match:
        return 0
    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)
    return hours * 3600 + minutes * 60 + seconds


def should_keep_video(video: dict[str, Any], settings: dict[str, Any]) -> bool:
    snippet = video.get("snippet", {})
    statistics = video.get("statistics", {})
    content_details = video.get("contentDetails", {})

    title = (snippet.get("title") or "").lower()
    description = (snippet.get("description") or "").lower()
    if "#shorts" in title or "#shorts" in description:
        return False

    duration_seconds = parse_duration_seconds(content_details.get("duration", ""))
    if duration_seconds < int(settings["min_duration_sec"]):
        return False

    view_count = int(statistics.get("viewCount", 0))
    if view_count < int(settings["min_view_count"]):
        return False

    return True


def extract_json_object(text: str) -> dict[str, Any]:
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or start >= end:
        raise TrendCollectorError("Gemini response did not contain JSON")
    try:
        return json.loads(text[start : end + 1])
    except json.JSONDecodeError as exc:
        raise TrendCollectorError("Gemini response JSON parsing failed") from exc


def analyze_video(video_url: str, api_key: str) -> dict[str, Any]:
    prompt = {
        "contents": [
            {
                "parts": [
                    {
                        "text": (
                            "You are summarizing a YouTube video in Japanese. "
                            "Return only JSON with this exact schema: "
                            "{\"video_title\": string, "
                            "\"video_summary\": string, "
                            "\"technologies\": string[], "
                            "\"conclusion\": string, "
                            "\"reasons\": string[], "
                            "\"examples\": string[], "
                            "\"learnings\": string[]}. "
                            "Keep technologies empty when none are mentioned. "
                            f"Analyze this video URL: {video_url}"
                        )
                    }
                ]
            }
        ],
        "generationConfig": {"temperature": 0.2, "responseMimeType": "application/json"},
    }
    response = requests.post(
        (
            "https://generativelanguage.googleapis.com/v1beta/models/"
            "gemini-2.0-flash:generateContent"
        ),
        params={"key": api_key},
        json=prompt,
        timeout=120,
    )
    response.raise_for_status()
    payload = response.json()
    candidates = payload.get("candidates", [])
    if not candidates:
        raise TrendCollectorError("Gemini returned no candidates")
    parts = candidates[0].get("content", {}).get("parts", [])
    text = "".join(part.get("text", "") for part in parts)
    data = extract_json_object(text)
    for key in [
        "video_title",
        "video_summary",
        "technologies",
        "conclusion",
        "reasons",
        "examples",
        "learnings",
    ]:
        if key not in data:
            raise TrendCollectorError(f"Gemini response missing field: {key}")
    return data


def notion_headers(api_key: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28",
    }


def rich_text(content: str, bold: bool = False) -> list[dict[str, Any]]:
    return [
        {
            "type": "text",
            "text": {"content": content[:2000]},
            "annotations": {
                "bold": bold,
                "italic": False,
                "strikethrough": False,
                "underline": False,
                "code": False,
                "color": "default",
            },
        }
    ]


def make_callout(emoji: str, content: str, color: str) -> dict[str, Any]:
    return {
        "object": "block",
        "type": "callout",
        "callout": {
            "rich_text": rich_text(content),
            "icon": {"type": "emoji", "emoji": emoji},
            "color": color,
        },
    }


def make_heading(content: str) -> dict[str, Any]:
    return {
        "object": "block",
        "type": "heading_2",
        "heading_2": {"rich_text": rich_text(content, bold=True), "color": "default"},
    }


def make_bullet(content: str) -> dict[str, Any]:
    return {
        "object": "block",
        "type": "bulleted_list_item",
        "bulleted_list_item": {"rich_text": rich_text(content)},
    }


def make_numbered(content: str) -> dict[str, Any]:
    return {
        "object": "block",
        "type": "numbered_list_item",
        "numbered_list_item": {"rich_text": rich_text(content)},
    }


def build_notion_payload(
    settings: dict[str, Any], video: dict[str, Any], analysis: dict[str, Any]
) -> dict[str, Any]:
    snippet = video["snippet"]
    statistics = video["statistics"]
    video_url = f"https://www.youtube.com/watch?v={video['id']}"
    thumb_url = (
        snippet.get("thumbnails", {}).get("high", {}).get("url")
        or snippet.get("thumbnails", {}).get("default", {}).get("url")
        or ""
    )
    blocks: list[dict[str, Any]] = [
        make_callout("📺", analysis["video_summary"], "blue_background"),
        make_callout(
            "📊",
            (
                f"👁 再生数: {int(statistics.get('viewCount', 0)):,}  "
                f"👍 いいね数: {int(statistics.get('likeCount', 0)):,}  "
                f"📺 {snippet.get('channelTitle', '')}"
            ),
            "gray_background",
        ),
        {"object": "block", "type": "divider", "divider": {}},
        make_heading("💡 結論"),
        {
            "object": "block",
            "type": "quote",
            "quote": {"rich_text": rich_text(analysis["conclusion"], bold=True), "color": "default"},
        },
        {"object": "block", "type": "divider", "divider": {}},
        make_heading("🔍 理由"),
    ]
    blocks.extend(make_bullet(reason) for reason in analysis["reasons"])
    blocks.append(make_heading("🧪 具体例"))
    blocks.extend(make_bullet(example) for example in analysis["examples"])
    blocks.append(make_heading("🎯 この動画から得られる学び"))
    blocks.extend(make_numbered(learning) for learning in analysis["learnings"])
    blocks.append({"object": "block", "type": "divider", "divider": {}})
    blocks.append(make_callout("🔗", f"元動画: {video_url}", "default"))

    return {
        "parent": {"database_id": settings["notion_database_id"]},
        "cover": {"type": "external", "external": {"url": thumb_url}} if thumb_url else None,
        "properties": {
            "タイトル": {"title": [{"text": {"content": analysis["video_title"][:2000]}}]},
            "いいね数": {"number": int(statistics.get("likeCount", 0))},
            "再生数": {"number": int(statistics.get("viewCount", 0))},
            "セーブ数": {"number": int(statistics.get("favoriteCount", 0))},
            "コメント数": {"number": int(statistics.get("commentCount", 0))},
            "チャンネルID": {
                "rich_text": [{"text": {"content": snippet.get("channelId", "")[:2000]}}]
            },
            "チャンネル名": {
                "rich_text": [{"text": {"content": snippet.get("channelTitle", "")[:2000]}}]
            },
            "URL": {"url": video_url},
            "published_at": {"date": {"start": snippet.get("publishedAt")}},
            "使用技術": {
                "multi_select": [
                    {"name": tech[:100]} for tech in analysis.get("technologies", []) if tech.strip()
                ]
            },
            "追加日": {"date": {"start": datetime.now(UTC).date().isoformat()}},
        },
        "children": blocks,
    }


def create_notion_page(payload: dict[str, Any], api_key: str) -> str:
    clean_payload = {key: value for key, value in payload.items() if value is not None}
    response = requests.post(
        "https://api.notion.com/v1/pages",
        headers=notion_headers(api_key),
        json=clean_payload,
        timeout=60,
    )
    response.raise_for_status()
    return response.json()["id"]


def build_run_log(now: datetime, settings: dict[str, Any]) -> dict[str, Any]:
    return {
        "run_started_at": now.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "config": {
            "min_duration_sec": settings["min_duration_sec"],
            "min_view_count": settings["min_view_count"],
            "max_candidates_per_run": settings["max_candidates_per_run"],
            "processed_retention_days": settings["processed_retention_days"],
        },
        "summary": {
            "channels": 0,
            "rss_candidates": 0,
            "new_candidates": 0,
            "eligible_videos": 0,
            "processed": 0,
            "failed": 0,
        },
        "errors": [],
    }


def write_run_log(log_path: Path, payload: dict[str, Any]) -> None:
    with log_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
        fh.write("\n")


def collect_candidates(
    channels: list[dict[str, str]], state: dict[str, dict[str, str]]
) -> tuple[list[VideoCandidate], int]:
    candidates: list[VideoCandidate] = []
    for channel in channels:
        channel_id = channel["youtube_channel_id"]
        candidates.extend(fetch_channel_feed(channel_id))
    rss_total = len(candidates)
    deduped = dedupe_candidates(candidates)
    return [item for item in deduped if item.video_id not in state], rss_total


def run(settings_path: Path) -> int:
    settings = load_settings(settings_path)
    base_dir = settings_path.parent.parent
    state_dir, logs_dir = ensure_dirs(settings, base_dir)
    state_file = state_dir / "processed_videos.json"

    now = datetime.now(UTC)
    run_log = build_run_log(now, settings)
    log_path = logs_dir / f"run_{now.strftime('%Y%m%d_%H%M%S')}.json"

    try:
        state = load_processed_state(state_file, int(settings["processed_retention_days"]))
        channels = read_channels(settings)
        run_log["summary"]["channels"] = len(channels)

        candidates, rss_total = collect_candidates(channels, state)
        run_log["summary"]["rss_candidates"] = rss_total
        run_log["summary"]["new_candidates"] = len(candidates)

        youtube_api_key = os.environ.get("YOUTUBE_API_KEY")
        gemini_api_key = os.environ.get("GEMINI_API_KEY")
        notion_api_key = os.environ.get("NOTION_API_KEY")
        if not youtube_api_key or not gemini_api_key or not notion_api_key:
            raise TrendCollectorError(
                "YOUTUBE_API_KEY, GEMINI_API_KEY, and NOTION_API_KEY are required"
            )

        details = fetch_video_details([item.video_id for item in candidates], youtube_api_key)
        eligible = [details[item.video_id] for item in candidates if item.video_id in details]
        eligible = [video for video in eligible if should_keep_video(video, settings)]
        eligible.sort(
            key=lambda item: item.get("snippet", {}).get("publishedAt", ""),
            reverse=True,
        )
        eligible = eligible[: int(settings["max_candidates_per_run"])]
        run_log["summary"]["eligible_videos"] = len(eligible)

        failed_items: list[dict[str, Any]] = []
        for video in eligible:
            video_url = f"https://www.youtube.com/watch?v={video['id']}"
            try:
                analysis = analyze_video(video_url, gemini_api_key)
                notion_payload = build_notion_payload(settings, video, analysis)
                notion_page_id = create_notion_page(notion_payload, notion_api_key)
                state[video["id"]] = {
                    "video_id": video["id"],
                    "processed_at": iso_now(),
                    "channel_id": video.get("snippet", {}).get("channelId", ""),
                    "notion_page_id": notion_page_id,
                }
                run_log["summary"]["processed"] += 1
            except Exception as exc:  # noqa: BLE001
                failed_items.append(
                    {
                        "video_id": video["id"],
                        "title": video.get("snippet", {}).get("title", ""),
                        "error": str(exc),
                    }
                )
                run_log["summary"]["failed"] += 1
                run_log["errors"].append(str(exc))

        if failed_items:
            run_log["failed_items"] = failed_items

        save_processed_state(state_file, state)
        run_log["run_finished_at"] = iso_now()
        write_run_log(log_path, run_log)
        return 0
    except Exception as exc:  # noqa: BLE001
        run_log["summary"]["failed"] += 1
        run_log["errors"].append(str(exc))
        run_log["run_finished_at"] = iso_now()
        write_run_log(log_path, run_log)
        print(f"error: {exc}", file=sys.stderr)
        return 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--settings",
        default="config/settings.yaml",
        help="Path to YAML settings file",
    )
    args = parser.parse_args()
    return run(Path(args.settings).resolve())


if __name__ == "__main__":
    raise SystemExit(main())
