import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from cli.log_betting_evals import send_discord_notification


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


def test_role_tagging_filters(monkeypatch):
    row = _base_row()
    row.update(
        {
            "_raw_sportsbook": {
                "fanduel": -110,
                "draftkings": -105,
                "betmgm": 210,
            },
            "best_book": "fanduel",
        }
    )

    payload = {}

    def fake_post(url, json):
        payload.update(json)

        class Resp:
            status_code = 200
            text = "ok"

        return Resp()

    monkeypatch.setattr(
        "cli.log_betting_evals.OFFICIAL_PLAYS_WEBHOOK_URL",
        "http://example.com",
    )
    monkeypatch.setattr("cli.log_betting_evals.requests.post", fake_post)

    send_discord_notification(row)

    content = payload.get("content", "")
    lines = [ln.strip() for ln in content.splitlines()]
    odds_idx = lines.index("📉 **Market Odds**:")
    odds_lines = lines[odds_idx + 1 : odds_idx + 4]

    # Fanduel and DraftKings should be tagged inline
    assert any("fanduel: -110 <@&1366767456470831164>" in l for l in odds_lines)
    assert any(
        "draftkings: -105 <@&1366767510246133821>" in l for l in odds_lines
    )
    # betmgm is outside the range so should have no tag
    assert all("<@&1366767548502245457>" not in l for l in odds_lines)
