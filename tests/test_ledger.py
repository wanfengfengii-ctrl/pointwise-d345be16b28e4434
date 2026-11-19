from ledger.history import LedgerHistory
from ledger.records import RecordRejected, record
from ledger.section_query import SourcePositionRejected, query_section_at
from tests.support import EAST, WorkdayCase, values


class LedgerTests(WorkdayCase):
    def test_unknown_correction_has_no_position_or_fact(self):
        record(self.source, 'register', 'E', 'east', 5)
        with self.assertRaises(RecordRejected):
            record(self.source, 'correct', 'Z', 'east', 12)
        self.assertEqual(LedgerHistory(self.source).head_position(), 1)
        self.assertEqual(values(query_section_at(self.source, 1, EAST).rows), (('E', 'east', 5, 1),))
        self.install()
        self.assertEqual(values(self.report().rows), (('E', 'east', 5, 1),))
        self.assertEqual(self.issue(), ())

    def test_invalid_readings_preserve_existing_fact_and_position(self):
        record(self.source, 'register', 'E', 'east', 5)
        for invalid in (True, False, 5.0, '5', b'5'):
            with self.subTest(reading=invalid):
                with self.assertRaises(RecordRejected):
                    record(self.source, 'correct', 'E', 'east', invalid)
                with self.assertRaises(RecordRejected):
                    record(self.source, 'register', 'G', 'east', invalid)
        with self.assertRaises(RecordRejected):
            record(self.source, 'register', 'G', 'unknown', 5)
        self.assertEqual(LedgerHistory(self.source).head_position(), 1)
        self.assertEqual(values(query_section_at(self.source, 1, EAST).rows), (('E', 'east', 5, 1),))
        self.assertEqual(record(self.source, 'register', 'G', 'east', -5), 2)

    def test_withdrawal_preserves_identity_and_earlier_section(self):
        self.assertEqual(record(self.source, 'register', 'E', 'east', 5), 1)
        self.assertEqual(record(self.source, 'withdraw', 'E'), 2)
        for operation, fields in (('register', ('east', 5)), ('correct', ('east', 5)), ('withdraw', ())):
            with self.subTest(operation=operation):
                with self.assertRaises(RecordRejected):
                    record(self.source, operation, 'E', *fields)
        self.assertEqual(LedgerHistory(self.source).head_position(), 2)
        self.assertEqual(values(query_section_at(self.source, 1, EAST).rows), (('E', 'east', 5, 1),))
        self.assertEqual(query_section_at(self.source, 2, EAST).rows, ())

    def test_history_position_zero_future_and_wrong_type(self):
        record(self.source, 'register', 'E', 'east', 5)
        self.assertEqual(query_section_at(self.source, 0, EAST).rows, ())
        for position in (-1, 2, True, 1.0):
            with self.subTest(position=position):
                with self.assertRaises(SourcePositionRejected):
                    query_section_at(self.source, position, EAST)
