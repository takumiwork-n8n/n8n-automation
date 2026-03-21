import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.trend_collector import extract_json_object, load_processed_state, parse_duration_seconds, should_keep_video


def test_parse_duration_seconds() -> None:
    assert parse_duration_seconds("PT3M10S") == 190
    assert parse_duration_seconds("PT1H2M3S") == 3723
    assert parse_duration_seconds("PT45S") == 45


def test_should_keep_video_filters_short_and_low_views() -> None:
    settings = {"min_duration_sec": 180, "min_view_count": 1000}
    low_view_video = {
        "snippet": {"title": "Video", "description": "desc"},
        "contentDetails": {"duration": "PT4M"},
        "statistics": {"viewCount": "999"},
    }
    shorts_video = {
        "snippet": {"title": "#shorts video", "description": "desc"},
        "contentDetails": {"duration": "PT4M"},
        "statistics": {"viewCount": "5000"},
    }
    valid_video = {
        "snippet": {"title": "Long video", "description": "desc"},
        "contentDetails": {"duration": "PT4M"},
        "statistics": {"viewCount": "5000"},
    }
    assert not should_keep_video(low_view_video, settings)
    assert not should_keep_video(shorts_video, settings)
    assert should_keep_video(valid_video, settings)


def test_extract_json_object() -> None:
    payload = extract_json_object('prefix {"video_title":"a","video_summary":"b","technologies":[],"conclusion":"c","reasons":[],"examples":[],"learnings":[]} suffix')
    assert payload["video_title"] == "a"


def test_load_processed_state_prunes_old_entries(tmp_path: Path) -> None:
    state_file = tmp_path / "processed_videos.json"
    state_file.write_text(
        json.dumps(
            {
                "items": {
                    "old": {"processed_at": "2020-01-01T00:00:00Z"},
                    "new": {"processed_at": "2099-01-01T00:00:00Z"},
                }
            }
        ),
        encoding="utf-8",
    )
    kept = load_processed_state(state_file, retention_days=90)
    assert "old" not in kept
    assert "new" in kept
