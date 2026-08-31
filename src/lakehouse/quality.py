"""Data quality gates: named checks that enforce contracts over DataFrames.

A Check is a callable that inspects a DataFrame and returns a Violation if the
contract is broken, or None if it passes. ``enforce`` runs every check in one
pass and raises QualityError listing all failures together — callers see the
full picture at once instead of fixing violations one at a time.

Usage::

    from lakehouse.quality import enforce, not_empty, null_rate, value_range

    enforce(silver_df, [
        not_empty(),
        null_rate("vendor_id", max_rate=0.05),
        value_range("total_amount", lo=0.01),
        value_range("passenger_count", lo=1, hi=8),
    ])
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame

# A Check is any callable that takes a DataFrame and returns a Violation or None.
Check = Callable[["DataFrame"], "Violation | None"]


@dataclass(frozen=True)
class Violation:
    check: str
    detail: str

    def __str__(self) -> str:
        return f"{self.check}: {self.detail}"


class QualityError(Exception):
    """Raised when one or more quality checks fail."""

    def __init__(self, violations: list[Violation]) -> None:
        self.violations = violations
        bullet = "\n  - ".join(str(v) for v in violations)
        super().__init__(f"{len(violations)} quality violation(s):\n  - {bullet}")


def enforce(df: DataFrame, checks: list[Check]) -> None:
    """Run every check and raise QualityError if any fail.

    All checks run before the exception is raised so callers see every problem
    in one shot.
    """
    violations = [v for check in checks if (v := check(df)) is not None]
    if violations:
        raise QualityError(violations)


# ---------------------------------------------------------------------------
# Built-in check factories
# ---------------------------------------------------------------------------


def not_empty() -> Check:
    """Fail if the DataFrame has zero rows."""

    def _check(df: DataFrame) -> Violation | None:
        if df.count() == 0:
            return Violation("not_empty", "DataFrame has zero rows")
        return None

    _check.__name__ = "not_empty"
    return _check


def null_rate(column: str, max_rate: float = 0.0) -> Check:
    """Fail if the null fraction of *column* exceeds *max_rate* (0.0 – 1.0)."""

    def _check(df: DataFrame) -> Violation | None:
        from pyspark.sql import functions as F

        total = df.count()
        if total == 0:
            return None
        nulls = df.filter(F.col(column).isNull()).count()
        rate = nulls / total
        if rate > max_rate:
            return Violation(
                f"null_rate({column})",
                f"{rate:.2%} null (threshold {max_rate:.2%}, {nulls}/{total} rows)",
            )
        return None

    _check.__name__ = f"null_rate({column})"
    return _check


def value_range(column: str, lo: float | None = None, hi: float | None = None) -> Check:
    """Fail if any value in *column* falls outside [lo, hi].

    Either bound may be omitted for a one-sided constraint.
    """

    def _check(df: DataFrame) -> Violation | None:
        from pyspark.sql import functions as F

        cond = F.lit(True)
        if lo is not None:
            cond = cond & (F.col(column) >= lo)
        if hi is not None:
            cond = cond & (F.col(column) <= hi)

        bad = df.filter(~cond).count()
        if bad:
            bounds = f"[{lo if lo is not None else '-∞'}, {hi if hi is not None else '+∞'}]"
            return Violation(f"value_range({column})", f"{bad} row(s) outside {bounds}")
        return None

    _check.__name__ = f"value_range({column})"
    return _check


def no_future_timestamps(column: str) -> Check:
    """Fail if any value in *column* is later than the current timestamp."""

    def _check(df: DataFrame) -> Violation | None:
        from pyspark.sql import functions as F

        bad = df.filter(F.col(column) > F.current_timestamp()).count()
        if bad:
            return Violation(f"no_future_timestamps({column})", f"{bad} row(s) in the future")
        return None

    _check.__name__ = f"no_future_timestamps({column})"
    return _check
