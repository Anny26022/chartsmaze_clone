"""Cross-sectional aggregation with metric-specific denominators."""

from collections import defaultdict
import math

import pandas as pd


COUNT_FIELDS = (
    "eligible_with_candle",
    "valid_return",
    "advances",
    "declines",
    "unchanged",
    "up_4",
    "down_4",
    "up_4_5",
    "down_4_5",
    "valid_monthly_extrema",
    "new_monthly_high",
    "new_monthly_low",
    "valid_quarterly_extrema",
    "new_quarterly_high",
    "new_quarterly_low",
    "valid_yearly_extrema",
    "new_52w_high",
    "new_52w_low",
    "valid_volume_20",
    "volume_above_20",
    "volume_below_or_equal_20",
    "valid_return_21",
    "up_25_month",
    "down_25_month",
    "up_50_month",
    "down_50_month",
    "valid_return_34",
    "up_13_34d",
    "down_13_34d",
    "valid_return_63",
    "up_25_quarter",
    "down_25_quarter",
)


def _blank_record(date):
    record = {"date": date}
    record.update({field: 0 for field in COUNT_FIELDS})
    for ma_type in ("sma", "ema"):
        for period in (10, 20, 50, 200):
            record[f"valid_{ma_type}_{period}"] = 0
            record[f"above_{ma_type}_{period}"] = 0
            record[f"below_{ma_type}_{period}"] = 0
            record[f"equal_{ma_type}_{period}"] = 0
    return record


def _present(value):
    return value is not None and not pd.isna(value)


class BreadthAccumulator:
    def __init__(self, methodology):
        self.methodology = methodology
        self._records = defaultdict(dict)

    def _record(self, date):
        if not self._records[date]:
            self._records[date] = _blank_record(date)
        return self._records[date]

    def update(self, history):
        for row in history.itertuples(index=False):
            record = self._record(row.Date)
            record["eligible_with_candle"] += 1

            daily_return = row.Daily_Return
            if _present(daily_return):
                record["valid_return"] += 1
                if daily_return > 0:
                    record["advances"] += 1
                elif daily_return < 0:
                    record["declines"] += 1
                else:
                    record["unchanged"] += 1
                if daily_return >= self.methodology.advance_threshold:
                    record["up_4"] += 1
                if daily_return < -self.methodology.advance_threshold:
                    record["down_4"] += 1
                if daily_return >= self.methodology.extreme_advance_threshold:
                    record["up_4_5"] += 1
                if daily_return < -self.methodology.extreme_advance_threshold:
                    record["down_4_5"] += 1

            for ma_type in ("SMA", "EMA"):
                for period in self.methodology.ma_periods:
                    ma_value = getattr(row, f"{ma_type}_{period}")
                    if not _present(ma_value):
                        continue
                    prefix = ma_type.lower()
                    record[f"valid_{prefix}_{period}"] += 1
                    if row.Close > ma_value:
                        record[f"above_{prefix}_{period}"] += 1
                    elif row.Close < ma_value:
                        record[f"below_{prefix}_{period}"] += 1
                    else:
                        record[f"equal_{prefix}_{period}"] += 1

            for label, valid_field, high_field, low_field in (
                ("Monthly", "valid_monthly_extrema", "new_monthly_high", "new_monthly_low"),
                ("Quarterly", "valid_quarterly_extrema", "new_quarterly_high", "new_quarterly_low"),
                ("Yearly", "valid_yearly_extrema", "new_52w_high", "new_52w_low"),
            ):
                reference_high = getattr(row, f"{label}_Reference_High")
                reference_low = getattr(row, f"{label}_Reference_Low")
                if _present(reference_high) and _present(reference_low):
                    record[valid_field] += 1
                    record[high_field] += int(getattr(row, f"New_{label}_High"))
                    record[low_field] += int(getattr(row, f"New_{label}_Low"))

            if _present(row.Volume_SMA_20) and _present(row.Volume):
                record["valid_volume_20"] += 1
                if row.Volume > row.Volume_SMA_20:
                    record["volume_above_20"] += 1
                else:
                    record["volume_below_or_equal_20"] += 1

            for value, valid_field, rules in (
                (
                    row.Return_21,
                    "valid_return_21",
                    (("up_25_month", 25, "gte"), ("down_25_month", -25, "lte"),
                     ("up_50_month", 50, "gte"), ("down_50_month", -50, "lte")),
                ),
                (
                    row.Return_34,
                    "valid_return_34",
                    (("up_13_34d", 13, "gte"), ("down_13_34d", -13, "lte")),
                ),
                (
                    row.Return_63,
                    "valid_return_63",
                    (("up_25_quarter", 25, "gte"), ("down_25_quarter", -25, "lte")),
                ),
            ):
                if not _present(value):
                    continue
                record[valid_field] += 1
                for field, threshold, operator in rules:
                    if (operator == "gte" and value >= threshold) or (operator == "lte" and value <= threshold):
                        record[field] += 1

    def records(self):
        return [self._records[date] for date in sorted(self._records)]


def percentage(numerator, denominator):
    if denominator <= 0:
        return None
    return 100.0 * numerator / denominator


def scaled_ratio(numerator, denominator):
    if denominator <= 0:
        return None
    return 100.0 * numerator / denominator


def percentage_change(current, previous):
    if current is None or previous in (None, 0) or not math.isfinite(previous):
        return None
    return 100.0 * (current / previous - 1)
