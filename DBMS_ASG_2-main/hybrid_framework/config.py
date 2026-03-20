import os
from pathlib import Path

# --- Database ---
SQL_URL = os.environ.get("HYBRID_SQL_URL", "sqlite:///./data/hybrid.db")
MONGO_URI = os.environ.get("HYBRID_MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.environ.get("HYBRID_MONGO_DB", "hybrid_db")

# --- File paths ---
DATA_DIR = Path("./data")
METADATA_FILE = DATA_DIR / "metadata.json"
BUFFER_FILE = DATA_DIR / "buffer.json"

# --- Classification thresholds (from Assignment 1) ---
FREQUENCY_THRESHOLD_SQL = 0.5
TYPE_STABILITY_THRESHOLD = 1.0

# --- Buffer ---
BUFFER_BATCH_SIZE = 50          # re-evaluate after this many new pending records
MIN_FIELD_OBSERVATIONS = 30     # minimum global observations before classifying a field

# --- Normalization engine ---
FD_SAMPLE_SIZE = 200            # max records to sample for functional dependency detection
FD_THRESHOLD = 0.90             # if B is determined by A in this fraction+ of A values, treat as FD
MAX_INLINE_FIELDS = 5           # nested object with <= this many sub-fields gets flattened into parent table

# --- MongoDB strategy ---
MONGO_EMBED_MAX_ARRAY_LENGTH = 10   # avg array length above this → reference

# --- Join key (JOIN_KEY is overridden from schema.global_record_key when registered) ---
DEFAULT_JOIN_KEY = "sys_ingested_at"
JOIN_KEY = DEFAULT_JOIN_KEY
# legacy_timestamp | uuid_v4 | from_payload
GLOBAL_RECORD_KEY_POLICY = "legacy_timestamp"
# Inferred from schema (scalar field other than global_record_key); None = omit from child/mongo extras
SECONDARY_JOIN_KEY: str | None = None


def _global_record_key_field(schema: dict) -> str | None:
    grk = schema.get("global_record_key")
    if isinstance(grk, dict):
        f = grk.get("field")
        return f if isinstance(f, str) else None
    if isinstance(grk, str):
        return grk
    return None


def infer_secondary_correlation_field(schema: dict) -> str | None:
    """
    Pick a second correlation attribute for child SQL rows / Mongo sidecars.
    User supplies only global_record_key; this chooses another scalar from schema:
    prefer str+not_null, then unique+not_null, then any not_null scalar.
    Ties break by earlier declaration order in schema['fields'] (not alphabetically).
    """
    fields = schema.get("fields")
    if not isinstance(fields, dict) or not fields:
        return None

    grk = _global_record_key_field(schema)
    order_index = {n: i for i, n in enumerate(fields.keys())}
    candidates: list[tuple[str, dict]] = []
    for name, spec in fields.items():
        if not isinstance(spec, dict):
            continue
        if name == grk:
            continue
        t = spec.get("type")
        if t in ("array", "object"):
            continue
        candidates.append((name, spec))

    if not candidates:
        return None

    def rank_key(item: tuple[str, dict]) -> tuple[int, int, int, int]:
        name, spec = item
        t = spec.get("type")
        nn = bool(spec.get("not_null", False))
        uq = bool(spec.get("unique", False))
        s_str_nn = 1 if t == "str" and nn else 0
        s_uq_nn = 1 if uq and nn else 0
        s_nn = 1 if nn else 0
        # Earlier schema position wins on tie: negate index so larger tuple = better, earlier field
        idx = order_index.get(name, 9999)
        return (s_str_nn, s_uq_nn, s_nn, -idx)

    candidates.sort(key=rank_key, reverse=True)
    return candidates[0][0]


def apply_join_key_from_schema(schema: dict) -> None:
    """Set JOIN_KEY, ingest policy, and inferred SECONDARY_JOIN_KEY from schema."""
    global JOIN_KEY, GLOBAL_RECORD_KEY_POLICY, SECONDARY_JOIN_KEY

    grk = schema.get("global_record_key")
    if not grk:
        JOIN_KEY = DEFAULT_JOIN_KEY
        GLOBAL_RECORD_KEY_POLICY = "legacy_timestamp"
    else:
        field: str | None
        policy: str
        if isinstance(grk, dict):
            field = grk.get("field")
            policy = str(grk.get("policy", "uuid_v4"))
        elif isinstance(grk, str):
            field = grk
            policy = "uuid_v4"
        else:
            field = None
            policy = "uuid_v4"

        if not field or not isinstance(field, str):
            JOIN_KEY = DEFAULT_JOIN_KEY
            GLOBAL_RECORD_KEY_POLICY = "legacy_timestamp"
        else:
            if policy not in ("uuid_v4", "from_payload"):
                policy = "uuid_v4"
            JOIN_KEY = field
            GLOBAL_RECORD_KEY_POLICY = policy

    SECONDARY_JOIN_KEY = infer_secondary_correlation_field(schema)
