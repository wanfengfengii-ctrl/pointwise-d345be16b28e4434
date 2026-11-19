from collections.abc import Iterable
from dataclasses import dataclass

from ledger.history import Observation


@dataclass(frozen=True)
class SectionTotals:
    section: str
    records: int
    missing: int
    reading_sum: int


def summarize_sections(scope: tuple[str, ...],
                       rows: Iterable[Observation]) -> tuple[SectionTotals, ...]:
    counts = dict.fromkeys(scope, 0)
    missing = dict.fromkeys(scope, 0)
    sums = dict.fromkeys(scope, 0)
    for row in rows:
        counts[row.section] += 1
        if row.reading is None:
            missing[row.section] += 1
        else:
            sums[row.section] += row.reading
    return tuple(SectionTotals(section, counts[section], missing[section], sums[section])
                 for section in scope)
