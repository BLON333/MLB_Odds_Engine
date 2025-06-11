import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils import format_segment_header


def test_mainline_suppresses_tag():
    assert format_segment_header("mainline").endswith("Mainline*")
    assert "\U0001F3F7" not in format_segment_header("mainline")


def test_team_total_keeps_tag():
    out = format_segment_header("team_total")
    assert "\U0001F3AF" in out  # emoji
    assert "\U0001F3F7" in out  # tag emoji
    assert out.endswith("team_total")
