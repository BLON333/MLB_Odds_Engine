import math
from collections import Counter
import numpy as np

def calculate_mean(pmf):
    return sum(x * p for x, p in pmf.items())

def calculate_std_dev(pmf):
    mean = calculate_mean(pmf)
    variance = sum(((x - mean) ** 2) * p for x, p in pmf.items())
    return math.sqrt(variance)

def calculate_tail_probability(pmf, threshold, direction="over"):
    def safe_key(x):
        try:
            return float(x)
        except:
            return None

    if direction == "over":
        return sum(p for x, p in pmf.items() if safe_key(x) is not None and safe_key(x) > threshold)
    elif direction == "under":
        return sum(p for x, p in pmf.items() if safe_key(x) is not None and safe_key(x) < threshold)
    elif direction == "exact":
        return pmf.get(int(threshold), 0.0)
    else:
        raise ValueError("Direction must be 'over', 'under', or 'exact'")




def calculate_fair_odds(probability):
    """
    Convert probability into decimal fair odds (e.g., 0.55 → 1.82).
    """
    if probability <= 0:
        return float("inf")  # Avoid divide by zero
    return 1 / probability

def summarize_pmf(values):
    from collections import Counter
    counts = Counter(values)
    total = sum(counts.values())
    pmf = {}
    for k, v in counts.items():
        try:
            key = float(k)
        except Exception:
            continue
        pmf[key] = float(v) / total
    return pmf

