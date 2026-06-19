"""Shared, reusable database clients.

Creating a new ``MongoClient`` per operation is expensive — each one performs
replica-set topology discovery (a network handshake) before the first query.
Reusing a single, process-wide client removes that per-call cost entirely.

MongoClient is thread-safe and is *designed* to be shared across a process
(it manages its own internal connection pool), so a singleton here is both
faster and the officially recommended usage. This changes no behavior — the
same queries run against the same database; only the client is reused.
"""

import threading

from pymongo import MongoClient

from hybriddb.config import paths

_mongo_client: MongoClient | None = None
_lock = threading.Lock()


def get_mongo_client() -> MongoClient:
    """Return the process-wide shared MongoClient (created lazily, once)."""
    global _mongo_client
    if _mongo_client is None:
        with _lock:
            if _mongo_client is None:
                _mongo_client = MongoClient(
                    paths.MONGO_URI, serverSelectionTimeoutMS=2000
                )
    return _mongo_client


def get_mongo_db(name: str | None = None):
    """Return a database handle from the shared client (default: configured DB)."""
    return get_mongo_client()[name or paths.MONGO_DB_NAME]
