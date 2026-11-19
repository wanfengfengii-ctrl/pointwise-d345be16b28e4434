import csv
import io

from audit.details import AuditReport
from ledger.history import reading_text


def _export(position: int, columns: tuple[str, ...], rows) -> str:
    output = io.StringIO(newline='')
    writer = csv.writer(output, lineterminator='\r\n')
    writer.writerow(('position', position))
    writer.writerow(columns)
    writer.writerows(rows)
    return output.getvalue()


def export_details(report: AuditReport) -> str:
    return _export(report.position, ('section', 'observation_id', 'reading_um', 'source_position'),
                   ((row.section, row.observation_id, reading_text(row.reading), row.source_position)
                    for row in report.rows))


def export_totals(report: AuditReport) -> str:
    return _export(report.position, ('section', 'records', 'missing', 'reading_sum_um'),
                   ((total.section, total.records, total.missing, reading_text(total.reading_sum))
                    for total in report.totals))
