import os
from core.data_loader import load_all_stats
from core.game_asset_builder import build_game_assets

# ✅ Step 1: Load stats
print("🔄 Loading batter and pitcher stats...")
batter_stats, pitcher_stats = load_all_stats()

# ✅ Step 2: Choose any game ID for today or tomorrow
game_id = "2025-05-13-STL@PHI"
print(f"🎯 Running build_game_assets for: {game_id}")

# ✅ Step 3: Run build_game_assets (this will auto-suggest and merge aliases)
assets = build_game_assets(game_id, batter_stats, pitcher_stats)

# ✅ Step 4: Show summary
if assets and "lineups" in assets:
    print("\n✅ Test succeeded — lineups and alias logic ran.")
else:
    print("\n❌ Something went wrong. No assets returned.")
