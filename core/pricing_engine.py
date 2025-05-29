import numpy as np
from scipy.special import logit, expit

class MLBPricingEngine:
    def __init__(self, calibration):
        self.run_scaling_factor = calibration.get("run_scaling_factor", 1.0)
        self.stddev_scaling_factor = calibration.get("stddev_scaling_factor", 1.0)
        self.run_diff_scaling_factor = calibration.get("run_diff_scaling_factor", 1.0)

        team_scaling = calibration.get("team_total_scaling", {})
        self.home_mean_factor = team_scaling.get("home_mean_factor", 1.0)
        self.home_std_factor = team_scaling.get("home_std_factor", 1.0)
        self.away_mean_factor = team_scaling.get("away_mean_factor", 1.0)
        self.away_std_factor = team_scaling.get("away_std_factor", 1.0)

        # Segment-level scaling factors for derivatives
        self.segment_scaling = calibration.get("segment_scaling", {})

        logit_params = calibration.get("logit_win_pct_calibration", {})
        self.logit_a = logit_params.get("a")
        self.logit_b = logit_params.get("b")

    def apply_total_scaling(self, raw_totals):
        mean_total = np.mean(raw_totals)
        std_scaled = [(r - mean_total) * self.stddev_scaling_factor + mean_total for r in raw_totals]
        final_scaled = [r * self.run_scaling_factor for r in std_scaled]
        return final_scaled

    def apply_runline_scaling(self, raw_diffs):
        mean_diff = np.mean(raw_diffs)
        return [(d - mean_diff) * self.run_diff_scaling_factor + mean_diff for d in raw_diffs]

    def calc_prob(self, samples, comparator):
        return np.mean([1 if comparator(x) else 0 for x in samples])

    def calc_total_probs(self, scaled_totals, line):
        p_push = self.calc_prob(scaled_totals, lambda x: round(x) == line)
        p_over = self.calc_prob(scaled_totals, lambda x: x > line)
        p_under = 1 - p_over - p_push
        return {"over": p_over, "under": p_under, "push": p_push}

    def calc_runline_prob(self, run_diffs, threshold):
        return self.calc_prob(run_diffs, lambda x: x > threshold)

    def price_moneyline(self, sim_win_pct):
        if self.logit_a is not None and self.logit_b is not None:
            sim_win_pct = float(expit(self.logit_a + self.logit_b * logit(sim_win_pct)))
        dec_odds = 1 / sim_win_pct
        if sim_win_pct >= 0.5:
            return -100 * (sim_win_pct / (1 - sim_win_pct))
        else:
            return 100 * ((1 - sim_win_pct) / sim_win_pct)

    def implied_prob(self, odds):
        if odds < 0:
            return abs(odds) / (abs(odds) + 100)
        else:
            return 100 / (odds + 100)

    def expected_value(self, p, odds):
        payout = odds / 100 if odds > 0 else 100 / abs(odds)
        return p * payout - (1 - p)

    def summarize_alt_totals(self, totals, lines):
        scaled_totals = self.apply_total_scaling(totals)
        return {
            line: self.calc_total_probs(scaled_totals, line)
            for line in lines
        }

    def summarize_alt_runlines(self, run_diffs, lines):
        scaled_diffs = self.apply_runline_scaling(run_diffs)
        return {
            line: self.calc_runline_prob(scaled_diffs, line)
            for line in lines
        }

    def apply_team_total_scaling(self, scores, is_home=True):
        mean_factor = self.home_mean_factor if is_home else self.away_mean_factor
        std_factor = self.home_std_factor if is_home else self.away_std_factor

        mean_score = np.mean(scores)
        std_scaled = [(s - mean_score) * std_factor + mean_score for s in scores]
        return [s * mean_factor for s in std_scaled]
