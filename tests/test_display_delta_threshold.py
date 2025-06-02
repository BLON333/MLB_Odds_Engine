import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import core.snapshot_core as sc
from core import market_movement_tracker as mmt


def test_display_delta_respects_threshold(monkeypatch):
    # Set a high threshold so movement is considered 'same'
    monkeypatch.setitem(mmt.MOVEMENT_THRESHOLDS, 'market_prob', 0.1)
    entry = {'market_prob': 0.51}
    prior = {'market_prob': 0.46}
    movement = mmt.detect_market_movement(entry, prior)
    entry.update(movement)
    entry['prev_market_prob'] = prior['market_prob']
    sc.annotate_display_deltas(entry, prior)
    assert entry['mkt_movement'] == 'same'
    assert entry['mkt_prob_display'] == '51.0%'

    # Lower threshold to trigger movement
    monkeypatch.setitem(mmt.MOVEMENT_THRESHOLDS, 'market_prob', 0.01)
    movement = mmt.detect_market_movement(entry, prior)
    entry.update(movement)
    sc.annotate_display_deltas(entry, prior)
    assert entry['mkt_movement'] == 'better'
    assert entry['mkt_prob_display'].startswith('46.0%')
    assert '→' in entry['mkt_prob_display']
