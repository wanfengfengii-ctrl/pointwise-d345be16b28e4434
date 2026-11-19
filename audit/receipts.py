import sqlite3

from replica.scope_request import ScopeReceipt, ScopeRequestStore


class ReceiptNotFound(LookupError):
    pass


class ReceiptPending(LookupError):
    pass


def receipt(receiver: sqlite3.Connection, subscription_id: str, request_id: str) -> ScopeReceipt:
    request = ScopeRequestStore(receiver).get(subscription_id, request_id)
    if request is None:
        raise ReceiptNotFound(f'unknown scope request: {request_id}')
    original = request.receipt
    if original is None:
        raise ReceiptPending(f'scope request awaits activation: {request_id}')
    return original
