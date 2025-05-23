# === Imports ===
import pandas as pd
import numpy as np

# === Load Bet History ===
# Make sure 'your_bet_log.csv' has these columns:
# ['market_type', 'hours_to_game', 'model_prob', 'market_prob', 'actual_result', 'odds']

bets = pd.read_csv('logs/market_evals.csv')
bets.columns = bets.columns.str.strip() 

# === Core Functions ===

def blend_prob(model_prob, market_prob, model_weight):
    """Blend model and market probabilities based on dynamic model weight."""
    return model_prob * model_weight + market_prob * (1 - model_weight)

def simulate_bankroll(bets, decay_function, kelly_fraction=0.5, initial_bankroll=40000):
    bankroll = initial_bankroll

    for idx, row in bets.iterrows():
        model_weight = decay_function(row['hours_to_start'], row['market_type'])  # <-- changed
        blended = blend_prob(row['model_implied_prob'], row['market_implied_prob'], model_weight)  # <-- changed

        edge = (blended * (row['odds'] - 1)) - (1 - blended)
        full_kelly = edge / (row['odds'] - 1)

        bet_fraction = max(full_kelly * kelly_fraction, 0)
        stake = bankroll * bet_fraction

        if row['bet_result'] == 1:  # <-- changed
            bankroll += stake * (row['odds'] - 1)
        elif row['bet_result'] == 0:  # <-- changed
            bankroll -= stake
        else:
            pass  # Push = no change

    return bankroll


# === Flexible Decay Generator ===

def flexible_decay_generator(initial_weight=0.6, late_weight=0.01, half_life_hours=24):
    """Return a decay function parameterized by initial weight, late weight, and half-life."""
    def decay_func(hours, market_type):
        if hours >= half_life_hours * 2:
            return initial_weight
        elif hours <= 0.5:
            return late_weight
        else:
            decay_factor = np.exp(-np.log(2) * (half_life_hours - hours) / half_life_hours)
            weight = late_weight + (initial_weight - late_weight) * decay_factor
            return max(min(weight, 1), 0)
    return decay_func

# === Grid Search Settings ===

# Define grids of parameters to search over
initial_weights = [0.5, 0.6, 0.7, 0.8, 0.9]
late_weights = [0.005, 0.01, 0.02]
half_life_hours_list = [8, 12, 16, 24, 32]

results = []

# === Run Grid Search ===

for iw in initial_weights:
    for lw in late_weights:
        for hlh in half_life_hours_list:
            decay = flexible_decay_generator(initial_weight=iw, late_weight=lw, half_life_hours=hlh)
            final_bankroll = simulate_bankroll(bets, decay)
            roi = (final_bankroll - 40000) / 40000
            results.append({
                'Initial Weight': iw,
                'Late Weight': lw,
                'Half Life (hrs)': hlh,
                'Final Bankroll': round(final_bankroll, 2),
                'ROI (%)': round(roi * 100, 2)
            })

# === Results Display ===

results_df = pd.DataFrame(results)
results_df = results_df.sort_values('Final Bankroll', ascending=False)

print("\n=== Top Blending Strategies ===")
print(results_df.head(20))  # Print Top 20 results

# Save all results to CSV
results_df.to_csv('grid_search_results.csv', index=False)

print("\nFull results saved to 'grid_search_results.csv'")
