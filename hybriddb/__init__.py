"""Hybrid database framework.

Adaptive ingestion + autonomous SQL/MongoDB placement, metadata-driven CRUD,
a logical dashboard, ACID-coordinated cross-backend transactions, and
benchmarking — packaged as a reproducible software framework.
"""

import sys as _sys

# Many modules print Unicode (→, ✓, box-drawing chars). On Windows the default
# console code page (cp1252) raises UnicodeEncodeError on those. Force UTF-8 on
# the standard streams so output works everywhere. Safe no-op if unsupported.
for _stream in (_sys.stdout, _sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

__version__ = "1.0.0"
