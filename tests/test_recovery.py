import sqlite3
from contextlib import closing

from audit.details import read
from audit.receipts import ReceiptNotFound, ReceiptPending, receipt
from delivery.resend import ReplyLost
from delivery.runner import advance_replication, forward_pending_scope_request
from ledger.records import record
from replica.receive import open_replica
from replica.scope_request import ScopeRequestRejected, change_scope
from tests.support import EAST, REPLICA, SUBSCRIPTION, WEST, WorkdayCase, reject_receiver_commit, values


class RecoveryTests(WorkdayCase):
    def test_activation_commit_failure_keeps_old_book_and_no_receipt(self):
        self.install_through_seven()
        with self.assertRaises(ReceiptNotFound):
            receipt(self.receiver, SUBSCRIPTION, 'R')
        pending = change_scope(self.receiver, SUBSCRIPTION, 'R', WEST)
        cutover = forward_pending_scope_request(self.source, self.receiver, SUBSCRIPTION)
        before = self.report()
        with reject_receiver_commit(self.receiver) as denied:
            with self.assertRaises(sqlite3.DatabaseError):
                self.send(cutover)
        self.assertEqual(denied, ['COMMIT'])
        with closing(open_replica(self.replica_path)) as observer:
            self.assertEqual(read(observer, REPLICA), before)
            with self.assertRaises(ReceiptPending):
                receipt(observer, SUBSCRIPTION, 'R')
        self.assertEqual(self.pending_ids(), (cutover.envelope_id,))
        self.reopen()
        self.assertEqual(change_scope(self.receiver, SUBSCRIPTION, 'R', WEST), pending)
        self.assertEqual(forward_pending_scope_request(self.source, self.receiver, SUBSCRIPTION), cutover)
        self.assertEqual(self.send(cutover).status, 'applied')
        result = receipt(self.receiver, SUBSCRIPTION, 'R')
        self.assertEqual((result.request_id, result.position), ('R', 7))
        self.assertEqual((self.report().position, values(self.report().rows)), (7, (('W', 'west', 17, 3),)))
        self.assertEqual(self.pending_ids(), ())

    def test_lost_success_reply_preserves_original_receipt_after_progress(self):
        self.install_through_seven()
        change_scope(self.receiver, SUBSCRIPTION, 'R', WEST)
        cutover = forward_pending_scope_request(self.source, self.receiver, SUBSCRIPTION)
        with self.assertRaises(ReplyLost):
            self.send(cutover, lose_reply=True)
        self.assertIn(cutover.envelope_id, self.pending_ids())
        original = receipt(self.receiver, SUBSCRIPTION, 'R')
        self.assertEqual((original.request_id, original.position), ('R', 7))
        self.assertEqual(self.report().scope, WEST)
        self.assertEqual(record(self.source, 'correct', 'W', 'west', 19), 8)
        (piece8,) = self.issue()
        self.assertEqual(self.send(piece8).status, 'applied')
        self.reopen()
        self.assertEqual(change_scope(self.receiver, SUBSCRIPTION, 'R', ['west', 'west']), original)
        self.assertEqual(receipt(self.receiver, SUBSCRIPTION, 'R'), original)
        with self.assertRaises(ScopeRequestRejected):
            change_scope(self.receiver, SUBSCRIPTION, 'R', EAST)
        self.assertEqual(self.send(cutover).status, 'applied')
        report = self.report()
        self.assertEqual((report.position, report.scope, values(report.rows)), (8, WEST, (('W', 'west', 19, 8),)))
        self.assertEqual(self.pending_ids(), ())

    def test_reopen_resumes_unforwarded_persistent_request(self):
        self.install_through_seven()
        change_scope(self.receiver, SUBSCRIPTION, 'R', WEST)
        self.reopen()
        self.assertEqual(self.report().scope, EAST)
        self.assertEqual(record(self.source, 'correct', 'W', 'west', 19), 8)
        results = advance_replication(self.source, self.receiver, SUBSCRIPTION, REPLICA)
        self.assertEqual([(result.position, result.status) for result in results], [(7, 'applied'), (8, 'applied')])
        self.assertEqual(receipt(self.receiver, SUBSCRIPTION, 'R').position, 7)
        self.assertEqual((self.report().position, values(self.report().rows)), (8, (('W', 'west', 19, 8),)))
