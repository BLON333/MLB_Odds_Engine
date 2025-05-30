import numpy as np


def scale_distribution(raw_vals, target_mean=None, target_sd=None):
    """Return values scaled to target mean and standard deviation."""
    arr = np.array(raw_vals, dtype=float)
    raw_mean = arr.mean()
    raw_sd = arr.std()
    scaled = arr
    if target_sd is not None and raw_sd > 0:
        scaled = (scaled - raw_mean) * (target_sd / raw_sd) + raw_mean
    if target_mean is not None:
        scaled = scaled + (target_mean - raw_mean)
    return scaled.tolist()

