import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from scipy.special import logit
import json

df = pd.read_csv("logs/sim_vs_market_win.csv")  # Your stored outcomes
# Columns expected: sim_win_pct, market_odds

# Convert market odds to implied probability
df["market_win_pct"] = np.where(
    df["market_odds"] < 0,
    -df["market_odds"] / (-df["market_odds"] + 100),
    100 / (df["market_odds"] + 100)
)

logit_sim = logit(df["sim_win_pct"].clip(0.0001, 0.9999))
logit_market = logit(df["market_win_pct"].clip(0.0001, 0.9999))

model = LinearRegression().fit(logit_sim.values.reshape(-1, 1), logit_market.values)
a, b = float(model.intercept_), float(model.coef_[0])

print(f"Fitted model: logit(p_calibrated) = {a:.4f} + {b:.4f} * logit(p_sim)")

with open("logs/logit_calibration_fitted.json", "w") as f:
    json.dump({"a": round(a, 4), "b": round(b, 4)}, f, indent=2)
