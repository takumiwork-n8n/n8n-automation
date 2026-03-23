import json
import sys
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.trend_collector import (
    VideoCandidate,
    build_analysis_prompt,
    collect_candidates,
    extract_video_id_from_url,
    extract_json_object,
    fetch_video_details,
    load_processed_state,
    parse_duration_seconds,
    read_channels,
    should_keep_video,
)


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


def test_should_keep_video_non_numeric_view_count() -> None:
    settings = {"min_duration_sec": 180, "min_view_count": 1000}
    video = {
        "snippet": {"title": "Video", "description": "desc"},
        "contentDetails": {"duration": "PT4M"},
        "statistics": {"viewCount": "N/A"},
    }
    assert not should_keep_video(video, settings)


def test_extract_video_id_from_url_multiple_formats() -> None:
    assert extract_video_id_from_url("https://www.youtube.com/watch?v=abc123&x=1") == "abc123"
    assert extract_video_id_from_url("https://youtu.be/xyz789") == "xyz789"
    assert extract_video_id_from_url("https://www.youtube.com/shorts/def456?feature=share") == "def456"
    assert extract_video_id_from_url("https://example.com") == ""


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


def test_load_processed_state_skips_invalid_timestamp(tmp_path: Path) -> None:
    state_file = tmp_path / "processed_videos.json"
    state_file.write_text(
        json.dumps(
            {
                "items": {
                    "bad": {"processed_at": "not-a-date"},
                    "good": {"processed_at": "2099-01-01T00:00:00Z"},
                }
            }
        ),
        encoding="utf-8",
    )
    kept = load_processed_state(state_file, retention_days=3650)
    assert "bad" not in kept
    assert "good" in kept


def test_read_channels_without_header_reads_first_row() -> None:
    settings = {"google_sheet_id": "sid", "channel_sheet_range": "'All_info_input'!A:B"}
    values = [["UC111"], ["UC222"], ["UC333"]]
    mock_service = Mock()
    (
        mock_service.spreadsheets.return_value.values.return_value.get.return_value.execute.return_value
    ) = {"values": values}
    with patch("src.trend_collector.build_sheets_service", return_value=mock_service):
        out = read_channels(settings)
    assert out == [
        {"youtube_channel_id": "UC111", "channel_name": ""},
        {"youtube_channel_id": "UC222", "channel_name": ""},
        {"youtube_channel_id": "UC333", "channel_name": ""},
    ]


def test_fetch_video_details_batches_requests() -> None:
    ids = [f"id{i}" for i in range(55)]

    def fake_get(_url: str, params: dict, timeout: int) -> Mock:
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"items": [{"id": video_id} for video_id in params["id"].split(",")]}
        return response

    with patch("src.trend_collector.requests.get", side_effect=fake_get) as mock_get:
        result = fetch_video_details(ids, "test-key")

    assert len(result) == 55
    assert mock_get.call_count == 2


def test_build_analysis_prompt_anchors_to_video_metadata() -> None:
    video = {
        "id": "abc123",
        "snippet": {
            "title": "Test title",
            "channelTitle": "Test channel",
            "description": "Test description",
        },
    }
    prompt = build_analysis_prompt(video)
    assert "Source title: Test title" in prompt
    assert "Source channel: Test channel" in prompt
    assert "https://www.youtube.com/watch?v=abc123" in prompt


def test_collect_candidates_skips_failed_state_items() -> None:
    state = {
        "abc123": {
            "video_id": "abc123",
            "processed_at": "2026-03-23T00:00:00Z",
            "status": "failed",
        }
    }
    channels = [{"youtube_channel_id": "ch1", "channel_name": "Channel"}]
    fake_feed = [
        VideoCandidate(
            video_id="abc123",
            title="t1",
            description="d1",
            published_at="2026-03-23T00:00:00Z",
            channel_id="ch1",
            channel_title="Channel",
        ),
        VideoCandidate(
            video_id="xyz999",
            title="t2",
            description="d2",
            published_at="2026-03-23T00:00:00Z",
            channel_id="ch1",
            channel_title="Channel",
        ),
    ]

    with patch("src.trend_collector.fetch_channel_feed", return_value=fake_feed):
        candidates, total = collect_candidates(channels, state)

    assert total == 2
    assert len(candidates) == 1
    assert candidates[0].video_id == "xyz999"


def test_collect_candidates_continue_on_error_and_log() -> None:
    state: dict[str, dict[str, str]] = {}
    channels = [{"youtube_channel_id": "bad", "channel_name": ""}, {"youtube_channel_id": "good", "channel_name": ""}]
    ok_feed = [
        VideoCandidate(
            video_id="ok1",
            title="t1",
            description="d1",
            published_at="2026-03-23T00:00:00Z",
            channel_id="good",
            channel_title="Good",
        )
    ]

    def side_effect(channel_id: str) -> list[VideoCandidate]:
        if channel_id == "bad":
            raise RuntimeError("boom")
        return ok_feed

    errors: list[str] = []
    with patch("src.trend_collector.fetch_channel_feed", side_effect=side_effect):
        candidates, total = collect_candidates(channels, state, errors=errors)

    assert total == 1
    assert len(candidates) == 1
    assert candidates[0].video_id == "ok1"
    assert errors
    assert "channel_id=bad" in errors[0]
