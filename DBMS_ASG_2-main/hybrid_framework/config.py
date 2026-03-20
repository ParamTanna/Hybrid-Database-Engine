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
FD_THRESHOLD = 0.95             # if B is determined by A in 95%+ of cases, it's an FD
MAX_INLINE_FIELDS = 5           # nested object with <= this many sub-fields gets flattened into parent table

# --- MongoDB strategy ---
MONGO_EMBED_MAX_ARRAY_LENGTH = 10   # avg array length above this → reference

# --- Join key ---
JOIN_KEY = "sys_ingested_at"
SECONDARY_JOIN_KEY = "username"
