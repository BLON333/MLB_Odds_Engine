import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd

from core.dispatch_fv_drop_snapshot import (
    is_market_prob_increasing,
    filter_by_books,
)


def test_is_market_prob_increasing():
    assert is_market_prob_increasing("60.1% \u2192 62.3%")
    assert not is_market_prob_increasing("60.1%")
    assert not is_market_prob_increasing("60.1% \u2192 59.3%")
    assert not is_market_prob_increasing(None)
    assert not is_market_prob_increasing("bad data")


def test_filter_by_books():
    df = pd.DataFrame({"Book": ["a", "b", "c"], "val": [1, 2, 3]})
    subset = filter_by_books(df, ["a", "c"])
    assert subset["Book"].tolist() == ["a", "c"]
    subset_empty = filter_by_books(df, [])
    assert subset_empty.equals(df)
