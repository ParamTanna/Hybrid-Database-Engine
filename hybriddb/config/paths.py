"""Single source of truth for filesystem paths and backend connection settings.

Every module imports path/connection constants from here instead of hardcoding
relative literals, so the package is runnable from any working directory and the
data directory / database hosts can be relocated via environment variables.

Resolution order for every setting: environment variable -> .env file (if
python-dotenv is installed) -> sane default defined below.
"""

import os
from pathlib import Path

# Optional .env support. If python-dotenv isn't installed we silently fall back
# to real environment variables and the defaults below.
try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:  # pragma: no cover - dotenv is optional
    pass


# paths.py lives at <root>/hybriddb/config/paths.py -> parents[2] is <root>.
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# All runtime data files live under one directory (override with HYBRIDDB_DATA_DIR).
DATA_DIR = Path(os.getenv("HYBRIDDB_DATA_DIR", str(PROJECT_ROOT / "data")))
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Generated reports / charts.
REPORTS_DIR = Path(os.getenv("HYBRIDDB_REPORTS_DIR", str(PROJECT_ROOT / "reports")))
CHARTS_DIR = REPORTS_DIR / "charts"


def _data(name: str) -> str:
    return str(DATA_DIR / name)


def _report(name: str) -> str:
    return str(REPORTS_DIR / name)


# ----------------------------------------------------------------------------
# Data files (string paths, passed straight to open()).
# ----------------------------------------------------------------------------
SCHEMA_FILE = _data("schema.json")
METADATA_FILE = _data("metadata_store.json")
BUFFER_FILE = _data("buffer.json")
USERS_FILE = _data("users.json")
SESSION_FILE = _data("session_store.json")
HISTORY_FILE = _data("query_history.json")
AUDIT_FILE = _data("record_audit_store.json")
RESULTS_FILE = _data("benchmark_results.json")
METADATA_BACKUP = _data("metadata_store_benchmark_backup.json")

# ----------------------------------------------------------------------------
# Generated reports.
# ----------------------------------------------------------------------------
BENCHMARK_REPORT = _report("benchmark_report.md")
COMPARATIVE_REPORT = _report("comparative_analysis.md")

# ----------------------------------------------------------------------------
# PostgreSQL connection settings.
# ----------------------------------------------------------------------------
PG_HOST = os.getenv("PGHOST", "localhost")
PG_PORT = int(os.getenv("PGPORT", "5432"))
PG_DB = os.getenv("PGDATABASE", "hybrid_db")
PG_USER = os.getenv("PGUSER", "postgres")
PG_PASSWORD = os.getenv("PGPASSWORD", "postgres")

# ----------------------------------------------------------------------------
# MongoDB connection settings. Default URI targets a single-node replica set so
# multi-document transactions are available (see docker-compose.yml).
# ----------------------------------------------------------------------------
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27018/?replicaSet=rs0")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "hybrid_db")
BUFFER_COLL = os.getenv("MONGO_BUFFER_COLL", "buffer")

# ----------------------------------------------------------------------------
# Ingestion stream (simulation server).
# ----------------------------------------------------------------------------
STREAM_BASE = os.getenv("STREAM_BASE", "http://127.0.0.1:8000/record")
DEFAULT_COUNT = int(os.getenv("INGEST_DEFAULT_COUNT", "100"))

# ----------------------------------------------------------------------------
# Type-conflict policy: what to do when an incoming value cannot be coerced to
# its field's declared schema type.
#   "adaptive" -> widen the field to a schemaless Mongo field, preserving the
#                 value (schema-on-read; the value is kept, types may mix).
#   "strict"   -> reject the write with a clear error (schema is a hard contract).
# Safe/representational mismatches (e.g. 12345 -> "12345", "42" -> 42) are always
# coerced regardless of this setting.
# ----------------------------------------------------------------------------
TYPE_CONFLICT_POLICY = os.getenv("HYBRIDDB_TYPE_CONFLICT_POLICY", "adaptive").lower()


def ensure_dirs() -> None:
    """Create the data and report directories if they do not yet exist."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)
