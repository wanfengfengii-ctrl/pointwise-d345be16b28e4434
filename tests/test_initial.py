import csv
import io

from audit.details import ReplicaNotReady
from audit.export import export_details, export_totals
from delivery.runner import advance_replication
from issuance.subscriptions import SubscriptionRejected, SubscriptionStore, subscribe
from ledger.records import record
from ledger.section_query import query_section_at
from tests.support import EAST, REPLICA, SUBSCRIPTION, WorkdayCase, totals, values


class InitialTests(WorkdayCase):
    def test_initial_details_and_totals(self):
        self.seed_initial()
        self.install()
        report = self.report()
        self.assertEqual((report.position, report.scope), (3, EAST))
        self.assertEqual(values(report.rows), (('E', 'east', 5, 1), ('F', 'east', None, 2)))
        self.assertEqual(totals(report), (('east', 2, 1, 5),))
        self.assertEqual(list(csv.reader(io.StringIO(export_totals(report)))), [
            ['position', '3'], ['section', 'records', 'missing', 'reading_sum_um'],
            ['east', '2', '1', '5'],
        ])
        self.source.close()
        self.assertEqual(self.report(), report)

    def test_equal_readings_keep_distinct_observation_ids(self):
        self.seed_initial()
        record(self.source, 'register', 'G', 'east', 5)
        self.install()
        report = self.report()
        self.assertEqual(values(report.rows), (
            ('E', 'east', 5, 1), ('F', 'east', None, 2), ('G', 'east', 5, 4),
        ))
        self.assertEqual(totals(report), (('east', 3, 1, 10),))

    def test_zero_is_distinct_from_missing(self):
        self.seed_initial()
        record(self.source, 'register', 'Z', 'east', 0)
        self.install()
        report = self.report()
        self.assertEqual(totals(report), (('east', 3, 1, 5),))
        exported = list(csv.reader(io.StringIO(export_details(report))))
        self.assertEqual(exported, [
            ['position', '4'], ['section', 'observation_id', 'reading_um', 'source_position'],
            ['east', 'E', '5', '1'], ['east', 'F', '', '2'], ['east', 'Z', '0', '4'],
        ])

    def test_install_retry_keeps_issued_position_and_rows(self):
        self.seed_initial()
        initial = self.subscribe()
        record(self.source, 'correct', 'E', 'east', 7)
        (correction,) = self.issue()
        self.assertEqual(self.send(correction).status, 'waiting')
        with self.assertRaises(ReplicaNotReady):
            self.report()
        self.assertEqual(self.send(initial).status, 'applied')
        self.assertEqual(self.report().position, 3)
        self.assertEqual(values(self.report().rows), (('E', 'east', 5, 1), ('F', 'east', None, 2)))
        self.assertEqual(self.send(correction).status, 'applied')
        self.assertEqual(self.send(initial).status, 'applied')
        self.assertEqual(self.report().position, 4)
        self.assertEqual(values(self.report().rows), (('E', 'east', 7, 4), ('F', 'east', None, 2)))
        self.assertEqual(self.pending_ids(), ())

    def test_subscribe_conflict_unknown_section_and_uninstalled_read(self):
        self.seed_initial()
        with self.assertRaises(ReplicaNotReady):
            self.report()
        initial = self.subscribe()
        with self.assertRaises(SubscriptionRejected):
            subscribe(self.source, SUBSCRIPTION, 'D2', EAST)
        with self.assertRaises(SubscriptionRejected):
            subscribe(self.source, 'other', REPLICA, EAST)
        with self.assertRaises(SubscriptionRejected):
            subscribe(self.source, 'other', 'D2', ['unknown'])
        with self.assertRaises(SubscriptionRejected):
            SubscriptionStore(self.source).get('other')
        self.assertEqual(self.pending_ids(), (initial.envelope_id,))
        self.assertEqual(SubscriptionStore(self.source).get(SUBSCRIPTION).cursor, 3)

    def test_reopen_existing_preserves_edits_cursor_and_outbox(self):
        self.seed_initial()
        self.install()
        record(self.source, 'correct', 'E', 'east', 7)
        record(self.source, 'withdraw', 'F')
        pieces = self.issue()
        pending = tuple(piece.envelope_id for piece in pieces)
        self.reopen()
        self.reopen()
        self.assertEqual(self.report().position, 3)
        self.assertEqual(self.pending_ids(), pending)
        self.assertEqual(SubscriptionStore(self.source).get(SUBSCRIPTION).cursor, 5)
        self.assertEqual(values(query_section_at(self.source, 5, EAST).rows), (('E', 'east', 7, 4),))
        advance_replication(self.source, self.receiver, SUBSCRIPTION, REPLICA)
        self.assertEqual(self.report().position, 5)
        self.assertEqual(values(self.report().rows), (('E', 'east', 7, 4),))
        self.assertEqual(self.pending_ids(), ())

    def test_empty_segment_and_all_missing_segment_have_zero_sum(self):
        record(self.source, 'register', 'F', 'east', None)
        self.install(('west', 'east', 'east'))
        self.assertEqual(self.report().scope, ('east', 'west'))
        self.assertEqual(totals(self.report()), (('east', 1, 1, 0), ('west', 0, 0, 0)))

    def test_empty_install_at_zero_can_advance_without_rows(self):
        self.install(())
        self.assertEqual((self.report().position, self.report().scope), (0, ()))
        self.assertEqual(totals(self.report()), ())
        record(self.source, 'register', 'E', 'east', 5)
        (progress,) = self.issue()
        self.assertEqual(progress.payload, {'operation': 'progress'})
        self.assertEqual(self.send(progress).status, 'applied')
        self.assertEqual((self.report().position, self.report().rows), (1, ()))
