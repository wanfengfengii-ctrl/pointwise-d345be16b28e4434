import sqlite3
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path

from audit.details import read
from delivery.outbox import Outbox
from delivery.resend import deliver
from issuance.increment import issue_available_increments
from issuance.subscriptions import open_source, subscribe
from ledger.records import record
from replica.receive import open_replica


SUBSCRIPTION = 'night'
REPLICA = 'D'
EAST = ('east',)
WEST = ('west',)


def values(rows):
    return tuple((row.observation_id, row.section, row.reading, row.source_position) for row in rows)


def totals(report):
    return tuple((row.section, row.records, row.missing, row.reading_sum) for row in report.totals)


@contextmanager
def reject_receiver_commit(connection):
    denied = []

    def authorize(action, operation, argument, database, trigger):
        if action == sqlite3.SQLITE_TRANSACTION and operation == 'COMMIT':
            denied.append(operation)
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    connection.set_authorizer(authorize)
    try:
        yield denied
    finally:
        connection.set_authorizer(None)


class WorkdayCase(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.source_path = Path(directory.name) / 'source.sqlite'
        self.replica_path = Path(directory.name) / 'replica.sqlite'
        self.source = open_source(self.source_path)
        self.receiver = None
        self.addCleanup(self.close_databases)
        self.receiver = open_replica(self.replica_path)

    def close_databases(self):
        self.source.close()
        if self.receiver is not None:
            self.receiver.close()

    def reopen(self):
        self.close_databases()
        self.source = open_source(self.source_path)
        self.receiver = open_replica(self.replica_path)

    def seed_initial(self):
        self.assertEqual(record(self.source, 'register', 'E', 'east', 5), 1)
        self.assertEqual(record(self.source, 'register', 'F', 'east', None), 2)
        self.assertEqual(record(self.source, 'register', 'W', 'west', 17), 3)

    def subscribe(self, scope=EAST):
        return subscribe(self.source, SUBSCRIPTION, REPLICA, scope)

    def install(self, scope=EAST):
        piece = self.subscribe(scope)
        self.assertEqual(self.send(piece).status, 'applied')
        return piece

    def seed_position_six(self):
        self.seed_initial()
        self.assertEqual(record(self.source, 'correct', 'E', 'east', 7), 4)
        self.assertEqual(record(self.source, 'correct', 'F', 'east', None), 5)
        self.assertEqual(record(self.source, 'correct', 'E', 'east', 6), 6)

    def install_through_seven(self):
        self.seed_initial()
        self.install()
        self.assertEqual(record(self.source, 'correct', 'E', 'east', 7), 4)
        self.assertEqual(record(self.source, 'withdraw', 'F'), 5)
        self.assertEqual(record(self.source, 'correct', 'E', 'east', 6), 6)
        self.assertEqual(record(self.source, 'correct', 'E', 'east', 5), 7)
        for piece in self.issue():
            self.assertEqual(self.send(piece).status, 'applied')
        self.assertEqual(values(self.report().rows), (('E', 'east', 5, 7),))

    def issue(self):
        return issue_available_increments(self.source, SUBSCRIPTION)

    def send(self, piece, **options):
        return deliver(self.source, self.receiver, piece.envelope_id, REPLICA, **options)

    def report(self):
        return read(self.receiver, REPLICA)

    def pending_ids(self):
        return tuple(piece.envelope_id for piece in Outbox(self.source).pending_for(SUBSCRIPTION))
