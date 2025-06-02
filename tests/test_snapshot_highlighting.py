import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd

from core.snapshot_core import _style_dataframe


def _get_bg_color(ctx, row, col):
    for prop, val in ctx[(row, col)]:
        if prop == "background-color":
            return val
    return None


def test_snapshot_highlighting():
    df = pd.DataFrame({
        "Market Class": ["A", "B", "C"],
        "EV": ["10%", "-5%", "0%"],
        "ev_movement": ["better", "worse", "same"],
        "Mkt %": ["50%", "52%", "48%"],
        "mkt_movement": ["same", "better", "worse"],
        "Odds": ["+100", "-110", "+150"],
        "odds_movement": ["worse", "same", "better"],
    })

    styled = _style_dataframe(df)
    html = styled.to_html()

    assert "#d4edda" in html
    assert "#f8d7da" in html

    ctx = styled._compute().ctx

    # EV column colors
    assert _get_bg_color(ctx, 0, 1) == "#d4edda"  # better
    assert _get_bg_color(ctx, 1, 1) == "#f8d7da"  # worse
    assert _get_bg_color(ctx, 2, 1) is None       # same

    # Mkt % column colors
    assert _get_bg_color(ctx, 1, 3) == "#d4edda"  # better
    assert _get_bg_color(ctx, 2, 3) == "#f8d7da"  # worse

    # Odds column colors
    assert _get_bg_color(ctx, 0, 5) == "#f8d7da"  # worse
    assert _get_bg_color(ctx, 2, 5) == "#d4edda"  # better
