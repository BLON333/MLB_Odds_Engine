import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils import merge_book_sources_for


def test_merge_f5_totals_with_alternate():
    offers = {
        "totals_1st_5_innings_source": {
            "Over 5.5": {"fanduel": 100},
        },
        "alternate_totals_1st_5_innings_source": {
            "Over 5.5": {"draftkings": 105},
        },
    }

    merged = merge_book_sources_for("totals_1st_5_innings", offers)
    assert merged["Over 5.5"]["fanduel"] == 100
    assert merged["Over 5.5"]["draftkings"] == 105


def test_merge_unknown_market_ignores_alternate():
    offers = {
        "weird_market_source": {"Label": {"fanduel": 999}},
        "alternate_weird_market_source": {"Label": {"fanduel": 1000}},
    }

    merged = merge_book_sources_for("weird_market", offers)
    assert merged == {"Label": {"fanduel": 999}}
