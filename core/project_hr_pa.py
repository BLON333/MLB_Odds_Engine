
from core.config import DEBUG_MODE, VERBOSE_MODE
import math

DEFAULT_SHRINK_BASE = 300
REFERENCE_TBF = 650

def dynamic_shrink_n(TBF):
    try:
        TBF = float(TBF)
    except (TypeError, ValueError):
        return DEFAULT_SHRINK_BASE
    if TBF <= 0:
        return DEFAULT_SHRINK_BASE * (REFERENCE_TBF / 1)
    return DEFAULT_SHRINK_BASE * (REFERENCE_TBF / TBF)

def infer_league_avg_hr_pa(role: str = "SP", stuff_plus: float = 100.0):
    if role.upper() == "RP":
        base = 0.020
        if stuff_plus >= 120:
            return base * 0.85
        elif stuff_plus >= 110:
            return base * 0.90
        elif stuff_plus <= 95:
            return base * 1.10
        else:
            return base
    else:
        base = 0.030
        if stuff_plus >= 110:
            return base * 0.90
        elif stuff_plus >= 100:
            return base * 0.95
        elif stuff_plus <= 90:
            return base * 1.10
        else:
            return base

def safe_float(value, fallback):
    try:
        val = float(value)
        return fallback if math.isnan(val) else val
    except (ValueError, TypeError):
        return fallback

def project_hr_pa(pitcher_data):
    if not isinstance(pitcher_data, dict):
        print(f"❌ project_hr_pa received non-dict: {type(pitcher_data)} → {pitcher_data}")
        raise TypeError("Expected dict for pitcher_data")

    for key in ["exit_velocity_avg", "launch_angle_avg", "barrel_batted_rate"]:
        val = pitcher_data.get(key)
        if isinstance(val, list):
            print(f"❌ Field '{key}' in project_hr_pa is a list → {val}")
            raise TypeError(f"Invalid list found in field '{key}'")

    HR = pitcher_data.get("HR", 0)
    TBF = pitcher_data.get("TBF", 1)
    IP = pitcher_data.get("IP", 1)
    empirical_hr_pa = HR / TBF if TBF else 0.0

    pitcher_data["barrel_batted_rate"] = safe_float(pitcher_data.get("barrel_batted_rate"), 0.06)
    pitcher_data["exit_velocity"] = safe_float(pitcher_data.get("exit_velocity_avg"), 88.5)
    pitcher_data["launch_angle"] = safe_float(pitcher_data.get("launch_angle_avg"), 12.5)

    barrel_rate = pitcher_data["barrel_batted_rate"]
    sweet_spot_pct = pitcher_data.get("sweet_spot_pct", 0.32)
    xSLG = pitcher_data.get("xSLG", 0.0)
    xwOBAcon = pitcher_data.get("xwOBAcon", 0.0)
    xSLG_diff = pitcher_data.get("xSLG_diff", 0.0)
    wOBAdiff = pitcher_data.get("wOBAdiff", 0.0)

    normalized_ev = (pitcher_data["exit_velocity"] - 85) / 1000
    normalized_la = (pitcher_data["launch_angle"] - 12) / 100

    K_pct = pitcher_data.get("K_pct", 0.0)
    BB_pct = pitcher_data.get("BB_pct", 0.0)

    Stuff_plus = pitcher_data.get("stuff_plus", 100.0)
    Location_plus = pitcher_data.get("location_plus", 100.0) / 100.0

    norm_stuff = Stuff_plus / 100.0
    FIP = pitcher_data.get("FIP", 4.0)
    norm_FIP = 1 - FIP / 10.0

    role = pitcher_data.get("role", "SP")
    league_avg_hr_pa = pitcher_data.get("league_avg_hr_pa", infer_league_avg_hr_pa(role, Stuff_plus))

    base = 0.0011
    model_estimate_hr_pa = (
        base
        + 0.020 * barrel_rate
        + 0.018 * normalized_ev
        + 0.018 * normalized_la
        + 0.010 * xSLG
        + 0.006 * xwOBAcon
        + 0.010 * sweet_spot_pct
        + 0.005 * xSLG_diff
        + 0.005 * wOBAdiff
        + 0.008 * (1 - K_pct)
        + 0.005 * (1 - BB_pct)
        + 0.004 * norm_stuff
        + 0.010 * Location_plus
        + 0.005 * norm_FIP
    )

    model_estimate_hr_pa *= 1.15

    if TBF >= 900:
        model_estimate_hr_pa *= 0.87
    elif TBF >= 700:
        model_estimate_hr_pa *= 0.93
    elif TBF >= 500:
        model_estimate_hr_pa *= 0.97  # slightly less shrinkage

    model_estimate_hr_pa = min(max(model_estimate_hr_pa, 0.005), 0.07)

    shrink_n = dynamic_shrink_n(TBF)
    smoothed_hr_pa = (TBF * model_estimate_hr_pa + shrink_n * league_avg_hr_pa) / (TBF + shrink_n)
    hr_per_9 = smoothed_hr_pa * (TBF / IP) * 9 if IP else 0.0

    return {
        "pitcher_id": pitcher_data.get("pitcher_id", None),
        "role": role,
        "hr_pa_projected": smoothed_hr_pa,
        "empirical_hr_pa": empirical_hr_pa,
        "model_estimate_hr_pa": model_estimate_hr_pa,
        "hr_per_9": hr_per_9,
        "league_avg_hr_pa": league_avg_hr_pa
    }
