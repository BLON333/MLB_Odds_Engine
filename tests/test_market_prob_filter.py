import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.dispatch_fv_drop_snapshot import is_market_prob_increasing


def test_is_market_prob_increasing():
    assert is_market_prob_increasing("60.1% \u2192 62.3%")
    assert not is_market_prob_increasing("60.1%")
    assert not is_market_prob_increasing("60.1% \u2192 59.3%")
    assert not is_market_prob_increasing(None)
    assert not is_market_prob_increasing("bad data")
