import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.format_utils import format_market_odds_and_roles
from cli.log_betting_evals import build_discord_embed


def _base_row():
    return {
        "game_id": "2025-06-09-MIL@CIN-T1305",
        "market": "h2h",
        "side": "TeamA",
        "consensus_prob": 0.55,
        "market_prob": 0.6,
        "full_stake": 1.2,
        "entry_type": "first",
        "market_odds": -110,
        "ev_percent": 6.0,
        "blended_prob": 0.6,
        "blended_fv": -105,
        "stake": 1.2,
        "sim_prob": 0.6,
    }


def test_format_market_odds_basic():
    odds_block, roles = format_market_odds_and_roles(
        "Pinnacle",
        {"Pinnacle": -105, "BetOnline": 210},
        {"pinnacle": 9.3, "betonline": 4.8},
        {"pinnacle": "<@&1>", "betonline": "<@&2>"},
    )
    lines = odds_block.splitlines()
    assert lines[0] == "• BetOnline: +210"
    assert lines[1] == "• **Pinnacle: -105 <@&1>**"
    assert roles == "📣 <@&1>"
    assert "<@&2>" not in odds_block


def test_top_up_includes_note_and_icon():
    row = _base_row()
    row.update(
        {
            "entry_type": "top-up",
            "stake": 0.5,
            "full_stake": 1.5,
            "segment_label": "alt_line",
            "_raw_sportsbook": {"pinnacle": -105},
            "best_book": "pinnacle",
        }
    )
    message = build_discord_embed(row)
    lines = [ln.strip() for ln in message.splitlines()]
    assert lines[0].startswith("🔁")
    assert any("🔁 Top-Up:" in ln for ln in lines)


def test_segment_header_no_redundant_tag():
    row = _base_row()
    row.update({"segment_label": "alt_line"})
    msg = build_discord_embed(row)
    lines = [ln.strip() for ln in msg.splitlines()]
    header_line = lines[2]
    assert "🏷" not in header_line


def test_no_extra_roles_line_when_none_qualify():
    row = _base_row()
    row.update(
        {
            "_raw_sportsbook": {"pinnacle": -200},
            "best_book": "pinnacle",
        }
    )
    msg = build_discord_embed(row)
    lines = [ln.strip() for ln in msg.splitlines()]
    assert "📣" not in lines[-1]
