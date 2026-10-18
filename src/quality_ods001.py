"""Pure rules for bounded snapshots of ten parsed ODS001 business fields."""

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import timedelta
from statistics import median


@dataclass
class QualityResult:
    check_id: str
    severity: str
    evaluated_count: int
    anomaly_count: int
    examples: list
    check_version: int = 1

    def to_dict(self):
        return asdict(self)


def evaluate_quality(records, *, as_of, check_continuity=True):
    if as_of.utcoffset() is None:
        raise ValueError("as_of requires a UTC offset")
    records = list(records)
    results = []

    def key(row):
        return {"timestamp_utc": row[0].isoformat(), "resolution_code": row[1]}

    def add(name, severity, evaluated, anomalies):
        results.append(
            QualityResult(name, severity, evaluated, len(anomalies), anomalies[:5])
        )

    counts = Counter((r[0], r[1]) for r in records)
    add(
        "technical.unique_key",
        "ERROR",
        len(records),
        [key(r) for r in records if counts[(r[0], r[1])] > 1],
    )
    add(
        "technical.quarter_hour",
        "ERROR",
        len(records),
        [
            key(r)
            for r in records
            if r[1] != "PT15M" or r[0].minute % 15 or r[0].second or r[0].microsecond
        ],
    )
    if check_continuity:
        times = sorted({r[0] for r in records})
        add(
            "technical.continuity",
            "WARNING",
            max(0, len(times) - 1),
            [
                {"previous": a.isoformat(), "next": b.isoformat()}
                for a, b in zip(times, times[1:])
                if b - a != timedelta(minutes=15)
            ],
        )
    cutoff = as_of - timedelta(hours=24)
    for i in range(2, 10):
        historical = [r for r in records if r[0] <= cutoff]
        pending = [r for r in records if r[0] > cutoff]
        add(
            f"completeness.{i}.historical",
            "WARNING" if i in (2, 9) else "ERROR",
            len(historical),
            [key(r) for r in historical if r[i] is None],
        )
        add(
            f"completeness.{i}.pending",
            "WARNING",
            len(pending),
            [key(r) for r in pending if r[i] is None],
        )
        available = [r for r in records if r[i] is not None]
        add(
            f"business.{i}.negative",
            "WARNING",
            len(available),
            [{**key(r), "value": r[i]} for r in available if r[i] < 0],
        )
        values = [r[i] for r in available]
        center = median(values) if values else 0
        mad = median([abs(v - center) for v in values]) if values else 0
        eligible = available if len(values) >= 20 and mad > 0 else []
        add(
            f"business.{i}.outlier",
            "WARNING",
            len(eligible),
            [
                {**key(r), "value": r[i]}
                for r in eligible
                if abs(r[i] - center) > 6 * 1.4826 * mad
            ],
        )
    for forecast, low, high in ((3, 4, 5), (6, 7, 8)):
        eligible = [
            r for r in records if all(r[i] is not None for i in (forecast, low, high))
        ]
        add(
            f"business.{forecast}.interval",
            "WARNING",
            len(eligible),
            [
                {**key(r), "forecast": r[forecast], "p10": r[low], "p90": r[high]}
                for r in eligible
                if not r[low] <= r[forecast] <= r[high]
            ],
        )
    return results


def has_errors(results):
    return any(r.severity == "ERROR" and r.anomaly_count for r in results)
