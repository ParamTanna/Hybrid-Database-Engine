import json
import os
import time
import uuid
from types import SimpleNamespace
from typing import Any

from flask import Flask, g, make_response, redirect, render_template, request, Response
from flask_socketio import SocketIO
from jinja2 import DictLoader

from hybriddb.config import paths
from hybriddb.testing.reliability_test_runner import (
  test_api_contract_rolled_back_flag,
  test_atomic_delete_mongo_fail_restores_sql,
  test_atomic_insert_mongo_fail_no_residue,
  test_atomic_insert_mongo_commit_fail_converges,
  test_atomic_update_mongo_fail_snapshot_restore,
  test_consistency_bulk_delete_mixed_keys_aborts_all,
  test_consistency_cross_backend_after_insert,
  test_consistency_duplicate_insert_rejected,
  test_consistency_unknown_update_data_rejected,
  test_consistency_unknown_where_rejected,
  test_durability_reopen_reader,
  test_isolation_duplicate_insert_race_repeated,
  test_isolation_parallel_unique_inserts,
  test_isolation_reader_never_sees_torn_update,
  test_isolation_update_delete_race,
)
from hybriddb.crud.read_operation import execute_read
from hybriddb.core.transaction_coordinator import TransactionCoordinator
from hybriddb.utils.strict_json import loads_strict_json
import time
from hybriddb.storage.query_history_store import (
    get_session,
    start_session,
    delete_session,
    delete_sessions_for_user,
    get_history,
    log_query,
    clear_history,
    update_session_activity,
    register_user,
    authenticate_user,
    update_user,
    delete_user,
    get_user,
    get_all_users,
)

tc = TransactionCoordinator()

METADATA_FILE = paths.METADATA_FILE

INTERNAL_KEYS = {
    "_id",
    "unknown_top",
    "discarded",
    "received_at",
    "storage_backend",
    "storage_detail",
    "key_management",
    "confidence",
}

SCHEMA_WARNING_TEXT = (
    "Schema not registered. Please run the main pipeline first "
    "(python main.py -> option 1 then 2)."
)

app = Flask(__name__)

socketio = SocketIO(app)


def _developer_mode_enabled() -> bool:
    user = _get_current_user()
    return bool(user and user.get("role") == "developer")


def _client_session_id() -> str:
    if hasattr(g, "client_session_id") and g.client_session_id:
        return g.client_session_id
    client_id = request.cookies.get("client_session_id")
    if not client_id:
        client_id = str(uuid.uuid4())
    g.client_session_id = client_id
    return client_id


def _get_current_user() -> dict | None:
    """Get current authenticated user from session."""
    username = request.cookies.get("username")
    if not username:
        return None
    return get_user(username)


def _require_auth() -> dict | None:
    """Require authentication, redirect to login if not authenticated."""
    user = _get_current_user()
    if not user:
        return redirect("/login")
    return user


def _require_developer() -> dict | None:
    """Require developer role, redirect if not authenticated or not a developer."""
    user = _require_auth()
    if not isinstance(user, dict):
        return user  # redirect Response from _require_auth
    if user.get("role") != "developer":
        return redirect("/login")
    return user


@app.after_request
def _ensure_client_session_cookie(response):
    if request.cookies.get("client_session_id"):
        return response
    client_id = getattr(g, "client_session_id", None)
    if client_id:
        response.set_cookie("client_session_id", client_id, max_age=30 * 24 * 60 * 60)
    return response


def _load_meta() -> dict | None:
    """Load metadata_store.json. Return None if missing."""
    if not os.path.exists(METADATA_FILE):
        return None
    try:
        with open(METADATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _strip_internal(obj: Any):
    if isinstance(obj, dict):
        clean: dict[str, Any] = {}
        for k, v in obj.items():
            if k in INTERNAL_KEYS:
                continue
            clean[k] = _strip_internal(v)
        return clean
    if isinstance(obj, list):
        return [_strip_internal(item) for item in obj]
    return obj


def _schema_ready() -> bool:
    return os.path.exists(METADATA_FILE)


def _logical_entities(meta: dict | None) -> list[dict]:
    if not meta:
        return []

    fields = meta.get("fields", {})
    rows: list[dict] = []
    for name, fmeta in fields.items():
        if fmeta.get("level") != 0:
            continue

        children_info = []
        for child_name in fmeta.get("children", []):
            child_meta = fields.get(child_name, {})
            children_info.append(
                {
                    "name": child_name,
                    "type": child_meta.get("type", "unknown"),
                }
            )

        rows.append(
            {
                "field_name": name,
                "type": fmeta.get("type", "unknown"),
                "required": "Yes" if fmeta.get("not_null") else "No",
                "has_children": "Yes" if children_info else "No",
                "children": children_info,
            }
        )

    rows.sort(key=lambda item: item["field_name"])
    return rows


def _mongo_unreachable() -> bool:
    try:
        from pymongo import MongoClient
        client = MongoClient(serverSelectionTimeoutMS=1200)
        client.admin.command("ping")
        client.close()
        return False
    except Exception:
        return True


def _buf_count() -> int:
    try:
        from buffer_store import count as buffer_count_func
        return buffer_count_func()
    except Exception:
        return 0


def _staging_count() -> int:
    try:
        from buffer_store import staging_count as staging_count_func
        return staging_count_func()
    except Exception:
        return 0


def _base_context() -> dict:
    _client_session_id()
    meta = _load_meta()
    ready = meta is not None
    dev_mode = _developer_mode_enabled()
    user = _get_current_user()

    total_records = 0
    total_fields = 0
    if meta:
        total_records = int(meta.get("total_records", 0))
        total_fields = sum(
            1
            for field_data in meta.get("fields", {}).values()
            if field_data.get("level") == 0
        )

    return {
        "meta": meta,
        "schema_ready": ready,
        "schema_status": "Registered" if meta else "Not Registered",
        "system_ready": "Yes" if ready else "No",
        "developer_mode": dev_mode,
        "current_path": request.path,
        "total_records": total_records,
        "total_fields": total_fields,
        "schema_warning": (SCHEMA_WARNING_TEXT if not ready else ""),
        "user": user,
    }


def _routing_map_rows(meta: dict | None) -> list[dict[str, Any]]:
    if not meta:
        return []

    fields = meta.get("fields", {})
    rows: list[dict[str, Any]] = []

    for fname, fmeta in fields.items():
        if fmeta.get("level") != 0:
            continue

        rows.append(
            {
                "name": fname,
                "type": fmeta.get("type", "unknown"),
                "backend": fmeta.get("storage_backend") or "Buffer",
                "detail": fmeta.get("storage_detail") or "Buffer",
                "required": "Yes" if fmeta.get("not_null") else "No",
            }
        )

        for child_name in fmeta.get("children", []):
            child = fields.get(child_name, {})
            rows.append(
                {
                    "name": f"{fname} -> {child_name}",
                    "type": child.get("type", "unknown"),
                    "backend": child.get("storage_backend") or "Buffer",
                    "detail": child.get("storage_detail") or "Buffer",
                    "required": "Yes" if child.get("not_null") else "No",
                }
            )

    return rows


def _data_distribution(meta: dict | None) -> dict:
    if not meta:
        return {}

    fields = meta.get("fields", {})
    distribution = {
        "High Frequency": 0,
        "Medium Frequency": 0,
        "Low Frequency": 0,
        "Buffer": 0,
    }
    total = 0

    for fname, fmeta in fields.items():
        if fmeta.get("level") != 0:
            continue
        total += 1
        freq = fmeta.get("frequency", 0)
        backend = fmeta.get("storage_backend", "Buffer")

        if backend == "Buffer":
            distribution["Buffer"] += 1
        elif freq >= 0.7:
            distribution["High Frequency"] += 1
        elif freq >= 0.4:
            distribution["Medium Frequency"] += 1
        else:
            distribution["Low Frequency"] += 1

    return {
        "counts": distribution,
        "total": total,
    }


def _safe_json_text(raw: str, fallback: str) -> str:
    try:
        parsed = loads_strict_json(raw)
    except Exception:
        return fallback
    return json.dumps(_strip_internal(parsed), indent=2, ensure_ascii=True)


def _parse_positive_int(raw: str, default: int) -> int:
    try:
        value = int(raw)
    except Exception:
        return default
    return value if value > 0 else default


def _reliability_group_map() -> dict[str, dict[str, Any]]:
    return {
        "A": {
            "title": "Failure Recovery",
            "tests": [
                ("Insert rollback leaves no residue", test_atomic_insert_mongo_fail_no_residue),
                ("Delete rollback restores prior SQL state", test_atomic_delete_mongo_fail_restores_sql),
                ("Update rollback restores prior record", test_atomic_update_mongo_fail_snapshot_restore),
                ("Mongo commit-fail converges (no divergence)", test_atomic_insert_mongo_commit_fail_converges),
                ("Rollback status flag contract", test_api_contract_rolled_back_flag),
            ],
        },
        "C": {
            "title": "Data Integrity",
            "tests": [
                ("Cross-backend agreement after insert", test_consistency_cross_backend_after_insert),
                ("Duplicate insert rejected", test_consistency_duplicate_insert_rejected),
                ("Unknown update data rejected", test_consistency_unknown_update_data_rejected),
                ("Unknown where-field rejected", test_consistency_unknown_where_rejected),
                ("Mixed-key bulk delete aborts", test_consistency_bulk_delete_mixed_keys_aborts_all),
            ],
        },
        "I": {
            "title": "Concurrent Safety",
            "tests": [
                ("Duplicate-insert race repeated", test_isolation_duplicate_insert_race_repeated),
                ("Update/delete race handling", test_isolation_update_delete_race),
                ("Parallel unique inserts", test_isolation_parallel_unique_inserts),
                ("Reader never sees torn update", test_isolation_reader_never_sees_torn_update),
            ],
        },
        "D": {
            "title": "Persistence",
            "tests": [
                ("Data survives reader restart", test_durability_reopen_reader),
            ],
        },
    }


def _reliability_sections(group_map: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    ordered = []
    for key in ("A", "C", "I", "D"):
        info = group_map[key]
        ordered.append(
            {
                "code": key,
                "title": info["title"],
                "tests": [name for name, _ in info["tests"]],
            }
        )
    return ordered


TEMPLATES = {
    "base.html": """
<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>{{ page_title }}</title>
  <style>
    :root {
      --text: #1f1f1f;
      --ok: #2e7d32;
      --bad: #c62828;
      --panel: #f5f5f5;
      --line: #d9d9d9;
      --link: #0f5e9c;
    }
    body {
      margin: 0;
      background: #ffffff;
      color: var(--text);
      font-family: -apple-system, BlinkMacSystemFont, \"Segoe UI\", Helvetica, Arial, sans-serif;
      line-height: 1.5;
    }
    .shell {
      max-width: 900px;
      margin: 0 auto;
      padding: 18px;
    }
    .nav {
      border-bottom: 1px solid var(--line);
      padding-bottom: 10px;
      margin-bottom: 16px;
      display: flex;
      align-items: center;
      gap: 12px;
      flex-wrap: wrap;
    }
    .nav a {
      color: var(--link);
      text-decoration: none;
      font-weight: 600;
      padding: 6px 10px;
      border-radius: 6px;
    }
    .nav a.active {
      background: rgba(15, 94, 156, 0.1);
      color: #0b4d8c;
    }
    .nav a:hover {
      background: rgba(15, 94, 156, 0.08);
    }
    .dev-toggle {
      margin-left: auto;
      display: inline-flex;
      align-items: center;
      gap: 6px;
      font-size: 0.92rem;
    }
    .dev-toggle input {
      width: auto;
      margin: 0;
    }
    .banner {
      border: 1px solid #ef9a9a;
      background: #ffebee;
      color: #8a1c1c;
      padding: 10px;
      margin-bottom: 14px;
    }
    .panel {
      border: 1px solid var(--line);
      padding: 12px;
      margin-bottom: 14px;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      margin-top: 8px;
    }
    th, td {
      border: 1px solid var(--line);
      padding: 8px;
      text-align: left;
      vertical-align: top;
    }
    textarea, input[type=\"text\"] {
      width: 100%;
      box-sizing: border-box;
      border: 1px solid #bdbdbd;
      padding: 8px;
      font: inherit;
      margin-top: 4px;
      margin-bottom: 10px;
    }
    textarea {
      min-height: 130px;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    }
    button {
      border: 1px solid #9e9e9e;
      background: #ffffff;
      padding: 8px 14px;
      cursor: pointer;
    }
    .status-success {
      color: var(--ok);
      font-weight: 700;
    }
    .status-failed {
      color: var(--bad);
      font-weight: 700;
    }
    pre.json {
      background: var(--panel);
      padding: 10px;
      border: 1px solid #e0e0e0;
      white-space: pre-wrap;
      word-break: break-word;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    }
    .hint {
      font-size: 0.88rem;
      color: #666;
    }
    .scroll-table {
      overflow-x: auto;
      margin-top: 10px;
    }
  </style>
</head>
<body>
  <div class=\"shell\">
    {% if user %}
    <div class=\"nav\">
      <a href=\"/\" class=\"{{ 'active' if current_path == '/' else '' }}\">Home</a>
      <a href=\"/sessions\" class=\"{{ 'active' if current_path == '/sessions' else '' }}\">Sessions</a>
      <a href=\"/query-history\" class=\"{{ 'active' if current_path == '/query-history' else '' }}\">History</a>
      <a href=\"/entities\" class=\"{{ 'active' if current_path == '/entities' else '' }}\">Entities</a>
      <a href=\"/records\" class=\"{{ 'active' if current_path == '/records' else '' }}\">Records</a>
      <a href=\"/query\" class=\"{{ 'active' if current_path == '/query' else '' }}\">Query</a>
      <a href=\"/insert\" class=\"{{ 'active' if current_path == '/insert' else '' }}\">Insert</a>
      <a href=\"/update\" class=\"{{ 'active' if current_path == '/update' else '' }}\">Update</a>
      <a href=\"/delete\" class=\"{{ 'active' if current_path == '/delete' else '' }}\">Delete</a>
      {% if developer_mode %}
        <a href=\"/users\" class=\"{{ 'active' if current_path == '/users' else '' }}\">Users</a>
        <a href=\"/routing-map\" class=\"{{ 'active' if current_path == '/routing-map' else '' }}\">Routing Map</a>
      {% endif %}
      <a href=\"/reliability-test\" class=\"{{ 'active' if current_path == '/reliability-test' else '' }}\">Reliability Tests</a>
      <div style=\"margin-left: auto; display: flex; align-items: center; gap: 12px;\">
        <span style=\"font-size: 0.9rem; color: #666;\">
          {{ user.username }} ({{ user.role }})
        </span>
        <form method=\"post\" action=\"/logout\" style=\"margin: 0;\">
          <button type=\"submit\" style=\"background: #d32f2f; color: white; border: none; padding: 6px 12px; border-radius: 4px; cursor: pointer; font-size: 0.9rem;\">Logout</button>
        </form>
      </div>
    </div>
    {% endif %}

    {% if schema_warning %}
      <div class=\"banner\">{{ schema_warning }}</div>
    {% endif %}

    {% block content %}{% endblock %}
  </div>
  <script src="https://cdn.socket.io/4.0.0/socket.io.min.js"></script>
  <script>
    const socket = io();
    socket.on('connect', () => console.log('SocketIO connected'));
    socket.on('connect_error', (err) => console.error('SocketIO connection error:', err));
    socket.on('dashboard_update', function() {
      console.log('Received dashboard_update event');
      location.reload();
    });
  </script>
</body>
</html>
""",
    "home.html": """
{% extends \"base.html\" %}
{% block content %}
  <h1>Hybrid Database &mdash; Logical Dashboard</h1>

  <div class=\"panel\">
    <h2>Session</h2>
    <table>
      <tr><th>Schema Status</th><td>{{ schema_status }}</td></tr>
      <tr><th>Total Records Ingested</th><td>{{ total_records }}</td></tr>
      <tr><th>Total Logical Fields</th><td>{{ total_fields }}</td></tr>
      <tr><th>System Ready</th><td>{{ system_ready }}</td></tr>
      <tr>
        <th>PostgreSQL</th>
        <td class=\"{{ 'status-success' if schema_ready else 'status-failed' }}\">
          {{ '✅ Connected' if schema_ready else '❌ Not initialized' }}
        </td>
      </tr>
      <tr>
        <th>MongoDB</th>
        <td class=\"{{ 'status-success' if mongo_ok else 'status-failed' }}\">
          {{ '✅ Connected' if mongo_ok else '❌ Unavailable' }}
        </td>
      </tr>
    </table>
    {% if mongo_warning %}
      <p class=\"hint\" style=\"color:#c62828;\">Warning: Some data may be unavailable</p>
    {% endif %}
  </div>

  <div class=\"panel\">
    <h2>Query Statistics</h2>
    {% if total_queries == 0 %}
      <p class=\"hint\">No queries run yet.
      Use the Query Runner to get started.</p>
    {% else %}
      <table>
        <tr><th>Total Queries Run</th><td>{{ total_queries }}</td></tr>
        <tr><th>Last Query At</th><td>{{ last_query_time }}</td></tr>
        <tr><th>Most Used Operation</th><td>{{ most_used_op }}</td></tr>
        <tr>
          <th>Success Rate</th>
          <td class=\"{{ 'status-success' if success_rate >= 80 else 'status-failed' }}\">
            {{ success_rate }}%
          </td>
        </tr>
      </table>
    {% endif %}
    <p class=\"hint\" style=\"margin-top:8px;\">
      <a href=\"/query-history\">View full query history →</a>
    {% if total_queries > 0 %}
      &nbsp;&nbsp;
      <a href=\"/sessions\">View session details →</a>
    {% endif %}
    </p>
  </div>

  {% if distribution and distribution.total > 0 %}
  <div class=\"panel\">
    <h2>Field Distribution</h2>
    <p class=\"hint\">How {{ distribution.total }} logical fields are classified by access frequency</p>
    <table>
      {% for label, count in distribution.counts.items() %}
        <tr>
          <td style=\"width:160px;\"><strong>{{ label }}</strong></td>
          <td style=\"width:200px;\">
            <div style=\"background:#e0e0e0; border-radius:4px; height:16px; width:100%;\">
              <div style=\"
                background: {{ '#2e7d32' if loop.index == 1 else '#0f5e9c' if loop.index == 2 else '#f57c00' if loop.index == 3 else '#9e9e9e' }};
                border-radius:4px;
                height:16px;
                width:{{ ((count / distribution.total) * 100) | int }}%;
              \"></div>
            </div>
          </td>
          <td style=\"padding-left:8px;\">{{ count }} field{{ 's' if count != 1 else '' }}</td>
        </tr>
      {% endfor %}
    </table>
  </div>
  {% endif %}
{% endblock %}
""",
    "entities.html": """
{% extends \"base.html\" %}
{% block content %}
  <h1>Logical Entities</h1>
  <p class=\"hint\">Schema overview — to view actual records, go to <a href=\"/records\">Records</a></p>
  <table>
    <tr>
      <th>Field Name</th>
      <th>Type</th>
      <th>Required</th>
      <th>Has Sub-fields</th>
    </tr>
    {% for row in entities %}
      <tr>
        <td>
          <strong>{{ row.field_name }}</strong>
          {% if row.children %}
            <ul>
              {% for child in row.children %}
                <li>{{ child.name }} ({{ child.type }})</li>
              {% endfor %}
            </ul>
          {% endif %}
        </td>
        <td>{{ row.type }}</td>
        <td>{{ row.required }}</td>
        <td>{{ row.has_children }}</td>
      </tr>
    {% endfor %}
  </table>
{% endblock %}
""",
    "records.html": """
{% extends \"base.html\" %}
{% block content %}
  <h1>All Records</h1>

  <div style=\"display:flex; gap:10px; align-items:center; margin-bottom:12px;\">
    <form method=\"get\" action=\"/records\" style=\"flex:1; display:flex; gap:8px;\">
      <input
        type=\"text\"
        name=\"search\"
        value=\"{{ search }}\"
        placeholder=\"Search all fields...\"
        style=\"flex:1; max-width:400px;\"
      />
      <button type=\"submit\">Search</button>
      {% if search %}
        <a href=\"/records?page=1\" style=\"color:var(--link);\">Clear</a>
      {% endif %}
    </form>
    <a href=\"/export/query/json\" style=\"color:var(--link);\">⬇ Export JSON</a>
    <a href=\"/export/entities/all/csv\" style=\"color:var(--link);\">⬇ Export CSV</a>
  </div>

  {% if error %}
    <div class=\"banner\">{{ error }}</div>
  {% else %}
    <p class=\"hint\">
      Showing {{ records|length }} of {{ total_count }} record(s)
      — Page {{ page }} of {{ total_pages }}
    </p>

    {% if not records %}
      <p class=\"hint\">No records found.</p>
    {% else %}
      <div class=\"scroll-table\">
        <table id=\"records-table\">
          <tr>
            {% for col in columns %}
              <th>{{ col }}</th>
            {% endfor %}
            <th>Action</th>
          </tr>
          {% for rec in records %}
            <tr>
              {% for col in columns %}
                {% set value = rec.get(col) %}
                <td>
                  {% if value is none %}
                  {% elif value is mapping %}
                    { ... }
                  {% elif value is sequence and value is not string %}
                    {{ value | length }} items
                  {% else %}
                    {% if col == global_key %}
                      <a href=\"/records/inspect?key={{ value }}\" style=\"color:var(--link);\">
                        {{ value }}
                      </a>
                    {% else %}
                      {{ value }}
                    {% endif %}
                  {% endif %}
                </td>
              {% endfor %}
              <td>
                <a href=\"/records/inspect?key={{ rec.get(global_key) }}\" style=\"color:var(--link);\">
                  Inspect
                </a>
              </td>
            </tr>
          {% endfor %}
        </table>
      </div>

      <div style=\"display:flex; gap:12px; margin-top:12px; align-items:center;\">
        {% if has_prev %}
          <a href=\"/records?page={{ page - 1 }}{% if search %}&search={{ search }}{% endif %}\">← Previous</a>
        {% else %}
          <span style=\"color:#999;\">← Previous</span>
        {% endif %}
        <span class=\"hint\">Page {{ page }} / {{ total_pages }}</span>
        {% if has_next %}
          <a href=\"/records?page={{ page + 1 }}{% if search %}&search={{ search }}{% endif %}\">Next →</a>
        {% else %}
          <span style=\"color:#999;\">Next →</span>
        {% endif %}
      </div>
    {% endif %}
  {% endif %}
{% endblock %}
""",
    "query.html": """
{% extends \"base.html\" %}
{% block content %}
  <h1>Query Runner</h1>
  <form method=\"post\" enctype=\"application/x-www-form-urlencoded\">
    <fieldset {% if not schema_ready %}disabled{% endif %}>
      <label for=\"query_json\">JSON Query</label>
      <textarea id=\"query_json\" name=\"query_json\" placeholder='{"operation": "read", "fields": ["*"], "where": {"customer_id": 12345}}'>{{ query_json }}</textarea>
      <button type=\"submit\">Submit</button>
    </fieldset>
  </form>

  {% if message %}
    <p class=\"{{ 'status-success' if status == 'SUCCESS' else 'status-failed' }}\">{{ status }}: {{ message }}</p>
  {% endif %}

  {% if submitted_query %}
    <h2>Submitted Query</h2>
    <pre class=\"json\">{{ submitted_query }}</pre>
  {% endif %}

  {% if result_json %}
    <h2>Logical Result</h2>
    <pre class=\"json\">{{ result_json }}</pre>
  {% endif %}
{% endblock %}
""",
    "insert.html": """
{% extends \"base.html\" %}
{% block content %}
  <h1>Insert</h1>
  <form method=\"post\" enctype=\"application/x-www-form-urlencoded\">
    <fieldset {% if not schema_ready %}disabled{% endif %}>
      <label for=\"data_json\">Data JSON</label>
      <textarea id=\"data_json\" name=\"data_json\" placeholder='{"customer_id": 99999, "name": "Alice", "email": "alice@example.com"}'>{{ data_json }}</textarea>
      <button type=\"submit\">Submit</button>
    </fieldset>
  </form>

  {% if message %}
    <p class=\"{{ 'status-success' if status == 'SUCCESS' else 'status-failed' }}\">{{ status }}: {{ message }}</p>
  {% endif %}

  {% if developer_mode and routing_json %}
    <h2>Routing Details</h2>
    <pre class=\"json\">{{ routing_json }}</pre>
  {% elif routing_json %}
    <p class=\"hint\">Routing details hidden. Enable Developer mode to view.</p>
  {% endif %}
{% endblock %}
""",
    "update.html": """
{% extends \"base.html\" %}
{% block content %}
  <h1>Update</h1>
  <form method=\"post\" enctype=\"application/x-www-form-urlencoded\">
    <fieldset {% if not schema_ready %}disabled{% endif %}>
      <label for=\"entity\">Entity (optional)</label>
      <input id=\"entity\" type=\"text\" name=\"entity\" value=\"{{ entity }}\" placeholder=\"orders\" />

      <label for=\"where_json\">Where</label>
      <textarea id=\"where_json\" name=\"where_json\" placeholder='{"customer_id": 99999}'>{{ where_json }}</textarea>

      <label for=\"data_json\">Data</label>
      <textarea id=\"data_json\" name=\"data_json\" placeholder='{"name": "Alice Updated"}'>{{ data_json }}</textarea>

      <button type=\"submit\">Submit</button>
    </fieldset>
  </form>

  {% if message %}
    <p class=\"{{ 'status-success' if status == 'SUCCESS' else 'status-failed' }}\">{{ status }}: {{ message }}</p>
  {% endif %}

  {% if developer_mode and routing_json %}
    <h2>Routing Details</h2>
    <pre class=\"json\">{{ routing_json }}</pre>
  {% elif routing_json %}
    <p class=\"hint\">Routing details hidden. Enable Developer mode to view.</p>
  {% endif %}
{% endblock %}
""",
    "delete.html": """
{% extends \"base.html\" %}
{% block content %}
  <h1>Delete</h1>
  <form method=\"post\" enctype=\"application/x-www-form-urlencoded\">
    <fieldset {% if not schema_ready %}disabled{% endif %}>
      <label for=\"entity\">Entity (optional)</label>
      <input id=\"entity\" type=\"text\" name=\"entity\" value=\"{{ entity }}\" placeholder=\"orders\" />

      <label for=\"where_json\">Where</label>
      <textarea id=\"where_json\" name=\"where_json\" placeholder='{"customer_id": 99999}'>{{ where_json }}</textarea>

      <p class=\"hint\" style=\"color:#c62828;\">This action cannot be undone</p>
      <button type=\"submit\">Submit</button>
    </fieldset>
  </form>

  {% if message %}
    <p class=\"{{ 'status-success' if status == 'SUCCESS' else 'status-failed' }}\">{{ status }}: {{ message }}</p>
  {% endif %}
{% endblock %}
""",
    "reliability_test.html": """
{% extends "base.html" %}
{% block content %}
  <h1>Reliability Test Suite</h1>
  <div class="panel">
    <h2>Run Grouped Tests</h2>
    <form method="post" enctype="application/x-www-form-urlencoded">
      <fieldset {% if not schema_ready %}disabled{% endif %}>
        <label for="race_rounds">Race rounds (used by I tests)</label>
        <input id="race_rounds" type="text" name="race_rounds" value="{{ race_rounds }}" />
        <label for="parallel_inserts">Parallel inserts (used by I tests)</label>
        <input id="parallel_inserts" type="text" name="parallel_inserts" value="{{ parallel_inserts }}" />
        <label style="display:flex; align-items:center; gap:8px; margin-top:6px; margin-bottom:10px;">
          <input type="checkbox" name="stop_on_failure" value="1" {% if stop_on_failure %}checked{% endif %} />
          Stop after first failed test
        </label>
        <div style="margin-top:8px; display:flex; flex-wrap:wrap; gap:8px;">
          {% for section in reliability_sections %}
            <button type="submit" name="test_group" value="{{ section.code }}">Run {{ section.title }}</button>
          {% endfor %}
        </div>
      </fieldset>
    </form>
  </div>
  <div class="panel">
    <h2>Available Sections</h2>
    <table>
      <tr><th>Category</th><th>Tests</th></tr>
      {% for section in reliability_sections %}
        <tr>
          <td><strong>{{ section.title }}</strong></td>
          <td>
            <ul style="margin:0; padding-left:18px;">
              {% for test_name in section.tests %}
                <li>{{ test_name }}</li>
              {% endfor %}
            </ul>
          </td>
        </tr>
      {% endfor %}
    </table>
  </div>
  {% if message %}
    <p class="{{ 'status-success' if status == 'SUCCESS' else 'status-failed' }}">{{ status }}: {{ message }}</p>
  {% endif %}
  {% if results %}
    <div class="panel">
      <h2>Latest Run: {{ selected_group }} - {{ selected_group_title }}</h2>
      <table>
        <tr><th>Test</th><th>Status</th><th>Details</th></tr>
        {% for row in results %}
          <tr>
            <td>{{ row.name }}</td>
            <td class="{{ 'status-success' if row.status == 'PASS' else 'status-failed' }}">{{ row.status }}</td>
            <td>{{ row.detail }}</td>
          </tr>
        {% endfor %}
      </table>
    </div>
  {% endif %}
{% endblock %}
""",
    "routing_map.html": """
{% extends \"base.html\" %}
{% block content %}
  <h1>Entity Routing Map</h1>
  {% if not schema_ready %}
    <p class=\"hint\">Schema not ready yet.</p>
  {% elif not developer_mode %}
    <p class=\"hint\">Routing details hidden. Enable Developer mode to view where entities are stored.</p>
  {% else %}
    <table>
      <tr>
        <th>Entity / Field</th>
        <th>Type</th>
        <th>Required</th>
        <th>Backend</th>
        <th>Storage Detail</th>
      </tr>
      {% for row in routing_rows %}
        <tr>
          <td>{{ row.name }}</td>
          <td>{{ row.type }}</td>
          <td>{{ row.required }}</td>
          <td>{{ row.backend }}</td>
          <td>{{ row.detail }}</td>
        </tr>
      {% endfor %}
    </table>
  {% endif %}
{% endblock %}
""",
    "sessions.html": """
{% extends \"base.html\" %}
{% block content %}
  <h1>Active Sessions</h1>
  <div class=\"panel\">
    <h2>Current User</h2>
    <table>
      <tr><th>Username</th><td>{{ user.username }}</td></tr>
      <tr><th>Role</th><td>{{ user.role | title }}</td></tr>
      <tr><th>Last Login</th><td>{{ user.last_login or 'Never' }}</td></tr>
    </table>
    <form method=\"post\" action=\"/logout\" style=\"margin-top: 10px;\">
      <button type=\"submit\" style=\"background: #d32f2f; color: white; border: none; padding: 8px 16px; border-radius: 4px; cursor: pointer;\">Logout</button>
    </form>
  </div>
  <div class=\"panel\">
    <h2>Current Session</h2>
    {% if session_data %}
      <table>
        <tr><th>Session ID</th><td>{{ session_data.session_id[:8] }}...</td></tr>
        <tr><th>Started At</th><td>{{ session_data.started_at }}</td></tr>
        <tr><th>Last Active</th><td>{{ session_data.last_active }}</td></tr>
        <tr><th>Queries This Session</th><td>{{ session_data.query_count }}</td></tr>
        {% if session_data.username %}
          <tr><th>User</th><td>{{ session_data.username }} ({{ session_data.user_role }})</td></tr>
        {% endif %}
      </table>
    {% else %}
      <p class=\"hint\">No active session found.</p>
    {% endif %}
  </div>
  {% if is_developer %}
  <div class=\"panel\">
    <h2>All Active Sessions</h2>
    {% if all_sessions %}
      <div class=\"scroll-table\">
        <table>
          <tr>
            <th>Session ID</th>
            <th>User</th>
            <th>Started At</th>
            <th>Last Active</th>
            <th>Query Count</th>
          </tr>
          {% for session in all_sessions %}
            <tr>
              <td>{{ session.session_id[:8] }}...</td>
              <td>{{ session.username or 'Anonymous' }} {% if session.user_role %}({{ session.user_role }}){% endif %}</td>
              <td>{{ session.started_at }}</td>
              <td>{{ session.last_active }}</td>
              <td>{{ session.query_count }}</td>
            </tr>
          {% endfor %}
        </table>
      </div>
    {% else %}
      <p class=\"hint\">No sessions found.</p>
    {% endif %}
  </div>
  <div class=\"panel\">
    <h2>All Users</h2>
    {% if all_users %}
      <div class=\"scroll-table\">
        <table>
          <tr>
            <th>Username</th>
            <th>Role</th>
            <th>Created At</th>
            <th>Last Login</th>
          </tr>
          {% for user_data in all_users %}
            <tr>
              <td>{{ user_data.username }}</td>
              <td>{{ user_data.role | title }}</td>
              <td>{{ user_data.created_at }}</td>
              <td>{{ user_data.last_login or 'Never' }}</td>
            </tr>
          {% endfor %}
        </table>
      </div>
    {% else %}
      <p class=\"hint\">No users found.</p>
    {% endif %}
  </div>
  {% endif %}
  <div class=\"panel\">
    <h2>System Status</h2>
    <table>
      <tr><th>Schema Status</th><td>{{ schema_status }}</td></tr>
      <tr>
        <th>MongoDB</th>
        <td class=\"{{ 'status-success' if mongo_status else 'status-failed' }}\">
          {{ 'Connected ✅' if mongo_status else 'Unavailable ❌' }}
        </td>
      </tr>
      <tr>
        <th>PostgreSQL</th>
        <td class=\"{{ 'status-success' if schema_ready else 'status-failed' }}\">
          {{ 'Connected ✅' if schema_ready else 'Not initialized ❌' }}
        </td>
      </tr>
      <tr><th>MongoDB Buffer</th><td>{{ buffer_count }}</td></tr>
      <tr><th>Staging Buffer</th><td>{{ staging_count }}</td></tr>
    </table>
  </div>
  <div class=\"panel\">
    <h2>Recent Activity</h2>
    {% if recent_queries %}
      <table>
        <tr>
          <th>Time</th>
          <th>Operation</th>
          <th>Status</th>
          <th>Duration (ms)</th>
        </tr>
        {% for query in recent_queries %}
          <tr>
            <td>{{ query.timestamp }}</td>
            <td>{{ query.operation | upper }}</td>
            <td class=\"{{ 'status-success' if query.status == 'SUCCESS' else 'status-failed' }}\">
              {{ query.status }}
            </td>
            <td>{{ query.duration_ms | round(1) }}</td>
          </tr>
        {% endfor %}
      </table>
    {% else %}
      <p class=\"hint\">No queries recorded yet.</p>
    {% endif %}
    <p><a href=\"/query-history\">View full query history →</a></p>
  </div>
{% endblock %}
""",
    "users.html": """
{% extends \"base.html\" %}
{% block content %}
  <h1>Users</h1>
  <div class=\"panel\">
    <h2>Create User</h2>
    {% if create_user_error %}
      <div class=\"banner\">{{ create_user_error }}</div>
    {% elif create_user_message %}
      <div class=\"status-success\" style=\"margin-bottom:10px;\">{{ create_user_message }}</div>
    {% endif %}
    <form method=\"post\" style=\"display: grid; gap: 12px; max-width: 520px;\">
      <input type=\"hidden\" name=\"action\" value=\"create_user\" />
      <label>
        Username
        <input type=\"text\" name=\"new_username\" required />
      </label>
      <label>
        Password
        <input type=\"password\" name=\"new_password\" required />
      </label>
      <label>
        Confirm Password
        <input type=\"password\" name=\"confirm_password\" required />
      </label>
      <label>
        Role
        <select name=\"new_role\" required>
          <option value=\"user\">Normal User</option>
          <option value=\"developer\">Developer</option>
        </select>
      </label>
      <button type=\"submit\" style=\"width: fit-content;\">Create User</button>
    </form>
  </div>
  <div class=\"panel\">
    <h2>Update or Delete User</h2>
    {% if manage_user_error %}
      <div class=\"banner\">{{ manage_user_error }}</div>
    {% elif manage_user_message %}
      <div class=\"status-success\" style=\"margin-bottom:10px;\">{{ manage_user_message }}</div>
    {% endif %}
    <form method=\"post\" style=\"display: grid; gap: 12px; max-width: 520px;\">
      <label>
        Select User
        <select name=\"edit_username\" required>
          <option value=\"\">Choose user</option>
          {% for user_data in all_users %}
            <option value=\"{{ user_data.username }}\">{{ user_data.username }} ({{ user_data.role }})</option>
          {% endfor %}
        </select>
      </label>
      <label>
        New Password
        <input type=\"password\" name=\"edit_password\" placeholder=\"Leave blank to keep current password\" />
      </label>
      <label>
        Role
        <select name=\"edit_role\" required>
          <option value=\"user\">Normal User</option>
          <option value=\"developer\">Developer</option>
        </select>
      </label>
      <div style=\"display:flex; gap:12px; flex-wrap:wrap;\">
        <button type=\"submit\" name=\"action\" value=\"update_user\" style=\"width: fit-content;\">Update User</button>
        <button type=\"submit\" name=\"action\" value=\"delete_user\" style=\"width: fit-content; background: #d32f2f; color: white;\">Delete User</button>
      </div>
    </form>
  </div>
  <div class=\"panel\">
    <h2>All Users</h2>
    {% if all_users %}
      <div class=\"scroll-table\">
        <table>
          <tr>
            <th>Username</th>
            <th>Role</th>
            <th>Created At</th>
            <th>Last Login</th>
          </tr>
          {% for user_data in all_users %}
            <tr>
              <td>{{ user_data.username }}</td>
              <td>{{ user_data.role | title }}</td>
              <td>{{ user_data.created_at }}</td>
              <td>{{ user_data.last_login or 'Never' }}</td>
            </tr>
          {% endfor %}
        </table>
      </div>
    {% else %}
      <p class=\"hint\">No users have been created yet.</p>
    {% endif %}
  </div>
{% endblock %}
""",
    # ── CHANGED: query_history.html ─────────────────────────────────────────
    # Re-run logic:
    #   read   → always show Re-run  (links to /query?replay=ID)
    #   insert → show Re-run only when status == FAILED  (links to /insert?replay=ID)
    #   update → always show Re-run  (links to /update?replay=ID)
    #   delete → never show Re-run
    "query_history.html": """
{% extends \"base.html\" %}
{% block content %}
  <h1>Query Execution History</h1>
  <p class=\"hint\">Showing {{ total_count }} recorded operations</p>
  {% if not history %}
    <p class=\"hint\">No queries have been recorded yet.
    Run a query from the Query Runner page.</p>
  {% else %}
    <div class=\"scroll-table\">
      <table>
        <tr>
          <th>#</th>
          <th>Timestamp</th>
          <th>User</th>
          <th>Role</th>
          <th>Session</th>
          <th>IP</th>
          <th>Operation</th>
          <th>Status</th>
          <th>Duration (ms)</th>
          <th>Results</th>
          <th>Message</th>
          <th>Action</th>
        </tr>
        <tbody id="history-tbody">
        {% for entry in history %}
          <tr>
            <td>{{ entry.id }}</td>
            <td>{{ entry.timestamp }}</td>
            <td>{{ entry.username or '-' }}</td>
            <td>{{ entry.user_role or '-' }}</td>
            <td>{{ entry.session_id[:8] if entry.session_id else '-' }}</td>
            <td>{{ entry.ip_address or '-' }}</td>
            <td>{{ entry.operation | upper }}</td>
            <td class=\"{{ 'status-success' if entry.status == 'SUCCESS' else 'status-failed' }}\">
              {{ entry.status }}
            </td>
            <td>{{ entry.duration_ms | round(1) }}</td>
            <td>{{ entry.result_count }}</td>
            <td>{{ entry.message[:60] }}{% if entry.message|length > 60 %}...{% endif %}</td>
            <td>
              {% if entry.operation == 'read' %}
                <a href=\"/query?replay={{ entry.id }}\" style=\"color:var(--link);\">Re-run</a>
              {% elif entry.operation == 'insert' and entry.status == 'FAILED' %}
                <a href=\"/insert?replay={{ entry.id }}\" style=\"color:var(--link);\">Re-run</a>
              {% elif entry.operation == 'update' %}
                <a href=\"/update?replay={{ entry.id }}\" style=\"color:var(--link);\">Re-run</a>
              {% endif %}
            </td>
          </tr>
        {% endfor %}
        </tbody>
      </table>
    </div>
  {% endif %}
  <script>
    const historySocket = io();
    historySocket.on('connect', () => console.log('History page SocketIO connected'));
    historySocket.on('connect_error', (err) => console.error('History page SocketIO connection error:', err));

    const rerunLink = (entry) => {
      if (entry.operation === 'read') {
        return `<a href="/query?replay=${entry.id}" style="color:var(--link);">Re-run</a>`;
      }
      if (entry.operation === 'insert' && entry.status === 'FAILED') {
        return `<a href="/insert?replay=${entry.id}" style="color:var(--link);">Re-run</a>`;
      }
      if (entry.operation === 'update') {
        return `<a href="/update?replay=${entry.id}" style="color:var(--link);">Re-run</a>`;
      }
      return '';
    };

    const updateHistoryTable = (data) => {
      const tbody = document.getElementById('history-tbody');
      tbody.innerHTML = data.history.map(entry => `
        <tr>
          <td>${entry.id}</td>
          <td>${entry.timestamp}</td>
          <td>${entry.username || '-'}</td>
          <td>${entry.user_role || '-'}</td>
          <td>${entry.session_id ? entry.session_id.substring(0,8) : '-'}</td>
          <td>${entry.ip_address || '-'}</td>
          <td>${entry.operation.toUpperCase()}</td>
          <td class="${entry.status === 'SUCCESS' ? 'status-success' : 'status-failed'}">${entry.status}</td>
          <td>${entry.duration_ms.toFixed(1)}</td>
          <td>${entry.result_count}</td>
          <td>${entry.message.length > 60 ? entry.message.substring(0,60) + '...' : entry.message}</td>
          <td>${rerunLink(entry)}</td>
        </tr>
      `).join('');
      document.querySelector('.hint').textContent = `Showing ${data.total_count} recorded operations`;
    };

    const fetchHistory = () => {
      fetch('/api/history')
        .then(response => response.json())
        .then(updateHistoryTable)
        .catch(err => console.error('History fetch failed:', err));
    };

    historySocket.on('dashboard_update', function() {
      fetchHistory();
    });

    setInterval(fetchHistory, 5000);
  </script>
{% endblock %}
""",
    "field_inspector.html": """
{% extends \"base.html\" %}
{% block content %}
  <h1>Field Inspector</h1>
  <p class=\"hint\">Record: {{ global_key }} = {{ key_value }}</p>
  <p><a href=\"/records\">← Back to Records</a></p>
  {% if error or not field_rows %}
    <div class=\"banner\">{{ error if error else 'Record not found.' }}</div>
  {% else %}
    <table>
      <tr>
        <th>Field Name</th>
        <th>Value</th>
        <th>Value Type</th>
      </tr>
      {% for row in field_rows %}
        <tr>
          <td><strong>{{ row.field }}</strong></td>
          <td>
            {% if row.value is none %}
              <em>null</em>
            {% elif row.value is mapping %}
              <table style=\"width:auto; border:none; margin:0;\">
                {% for k, v in row.value.items() %}
                  <tr>
                    <td style=\"border:none; padding:2px 8px 2px 0; font-weight:600;\">{{ k }}</td>
                    <td style=\"border:none; padding:2px 0;\">{{ v }}</td>
                  </tr>
                {% endfor %}
              </table>
            {% elif row.value is sequence and row.value is not string %}
              {% if row.value | length == 0 %}
                <em>empty</em>
              {% else %}
                {% for item in row.value %}
                  {% if item is mapping %}
                    <div style=\"border:1px solid #e0e0e0; padding:6px; margin-bottom:4px; background:#fafafa;\">
                      {% for k, v in item.items() %}
                        <span style=\"margin-right:12px;\">
                          <strong>{{ k }}</strong>: {{ v }}
                        </span>
                      {% endfor %}
                    </div>
                  {% else %}
                    <div>{{ item }}</div>
                  {% endif %}
                {% endfor %}
              {% endif %}
            {% else %}
              {{ row.value | string | truncate(80) }}
            {% endif %}
          </td>
          <td class=\"hint\">{{ row.value_type }}</td>
        </tr>
      {% endfor %}
    </table>
  {% endif %}
{% endblock %}
""",
    "login.html": """
{% extends "base.html" %}
{% block content %}
  <div style="max-width: 400px; margin: 50px auto; padding: 20px; border: 1px solid #ddd; border-radius: 8px;">
    <h1 style="text-align: center; margin-bottom: 30px;">Login</h1>
    <form method="post" style="display: flex; flex-direction: column; gap: 15px;">
      <div>
        <label for="username" style="display: block; margin-bottom: 5px; font-weight: bold;">Username</label>
        <input id="username" type="text" name="username" required style="width: 100%; padding: 8px; border: 1px solid #ccc; border-radius: 4px; box-sizing: border-box;" />
      </div>
      <div>
        <label for="password" style="display: block; margin-bottom: 5px; font-weight: bold;">Password</label>
        <input id="password" type="password" name="password" required style="width: 100%; padding: 8px; border: 1px solid #ccc; border-radius: 4px; box-sizing: border-box;" />
      </div>
      {% if error %}
        <div style="color: #d32f2f; background: #ffebee; padding: 10px; border-radius: 4px; border: 1px solid #ffcdd2;">
          {{ error }}
        </div>
      {% endif %}
      <button type="submit" style="padding: 10px; background: #1976d2; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 16px;">Login</button>
    </form>
  </div>
{% endblock %}
""",
}

setattr(app, "jinja_loader", DictLoader(TEMPLATES))


@app.get("/debug/mongo-ping")
def debug_mongo_ping():
    try:
        from pymongo import MongoClient
        client = MongoClient(serverSelectionTimeoutMS=1200)
        client.admin.command("ping")
        client.close()
        return {"ok": True, "target": "mongodb://localhost:27017"}
    except Exception as e:
        return {"ok": False, "target": "mongodb://localhost:27017", "error": str(e)}


# ============================================================================
# Authentication Routes
# ============================================================================

@app.get("/login")
def login_page():
    if _get_current_user():
        return redirect("/")
    return render_template("login.html", page_title="Login")


@app.post("/login")
def login():
    username = request.form.get("username", "").strip()
    password = request.form.get("password", "").strip()

    if not username or not password:
        return render_template("login.html", page_title="Login",
                               error="Username and password are required")

    success, user = authenticate_user(username, password)
    if not success or not user:
        return render_template("login.html", page_title="Login",
                               error="Invalid username or password")

    response = redirect("/")
    response.set_cookie("username", username, max_age=30 * 24 * 60 * 60)
    return response


@app.post("/logout")
def logout():
    client_id = request.cookies.get("client_session_id")
    if client_id:
        try:
            delete_session(client_id)
        except Exception:
            pass
    response = redirect("/login")
    response.delete_cookie("username")
    response.delete_cookie("client_session_id")
    return response


# ============================================================================
# Home
# ============================================================================

@app.get("/")
def home():
    user = _require_auth()
    if not isinstance(user, dict):
        return user

    client_id = _client_session_id()
    start_session(client_id, user.get("username"), user.get("role"))

    ctx = _base_context()
    ctx["user"] = user
    mongo_down = _mongo_unreachable()

    history = get_history(limit=200)
    total_queries = len(history)
    last_query_time = history[0].get("timestamp", "Never") if history else "Never"

    from collections import Counter
    op_counts = Counter(e.get("operation", "") for e in history)
    most_used = op_counts.most_common(1)[0][0].upper() if op_counts else "N/A"
    successes = sum(1 for e in history if e.get("status") == "SUCCESS")
    success_rate = round((successes / total_queries) * 100, 1) if total_queries > 0 else 0.0

    buf_count = _buf_count()
    stg_count = _staging_count()
    distribution = _data_distribution(ctx.get("meta"))

    ctx.update({
        "page_title": "Hybrid Database - Logical Dashboard",
        "mongo_warning": mongo_down,
        "mongo_ok": not mongo_down,
        "total_queries": total_queries,
        "last_query_time": last_query_time,
        "most_used_op": most_used,
        "success_rate": success_rate,
        "buffer_count": buf_count,
        "staging_count": stg_count,
        "distribution": distribution,
    })
    return render_template("home.html", **ctx)


# ============================================================================
# Entities
# ============================================================================

@app.get("/entities")
def entities():
    user = _require_auth()
    if not isinstance(user, dict):
        return user

    ctx = _base_context()
    ctx["user"] = user
    ctx.update({
        "page_title": "Logical Entities",
        "entities": _logical_entities(ctx["meta"]),
    })
    return render_template("entities.html", **ctx)


@app.get("/routing-map")
def routing_map_page():
    user = _require_auth()
    if not isinstance(user, dict):
        return user

    ctx = _base_context()
    view = {
        "page_title": "Entity Routing Map",
        "routing_rows": _routing_map_rows(ctx.get("meta")),
    }
    return render_template("routing_map.html", **ctx, **view)


# ============================================================================
# Query — CHANGED: handles replay for read, insert (FAILED only), update
# ============================================================================

@app.route("/query", methods=["GET", "POST"])
def query_page():
    user = _require_auth()
    if not isinstance(user, dict):
        return user

    ctx = _base_context()
    ctx["user"] = user

    default_query = '{"operation": "read", "fields": ["*"], "where": {"customer_id": 12345}}'

    replay_id = request.args.get("replay", "").strip()
    if replay_id:
        try:
            all_history = get_history(limit=200)
            match = next(
                (e for e in all_history if str(e.get("id")) == replay_id), None
            )
            # Only replay read queries from this route
            if match and match.get("query_payload") and match.get("operation") == "read":
                default_query = json.dumps(match["query_payload"], indent=2)
        except Exception:
            pass

    view = {
        "page_title": "Query Runner",
        "query_json": default_query,
        "status": "",
        "message": "",
        "submitted_query": "",
        "result_json": "",
    }

    if request.method == "POST" and ctx["meta"]:
        raw = request.form.get("query_json", "").strip()
        view["query_json"] = _safe_json_text(raw, default_query)
        try:
            query = loads_strict_json(raw)
        except ValueError as exc:
            view["status"] = "FAILED"
            view["message"] = str(exc)
            return render_template("query.html", **ctx, **view)

        safe_query = _strip_internal(query)
        view["submitted_query"] = json.dumps(safe_query, indent=2, ensure_ascii=True)

        try:
            start_time = time.time()
            results = execute_read(query, ctx["meta"])
            duration_ms = (time.time() - start_time) * 1000
            safe_results = _strip_internal(results)
            if not safe_results:
                view["status"] = "SUCCESS"
                view["message"] = "No records found"
                view["result_json"] = "[]"
            else:
                view["status"] = "SUCCESS"
                view["message"] = "Query executed"
                view["result_json"] = json.dumps(safe_results, indent=2, ensure_ascii=True)
            try:
                log_query(
                    "read", view["status"], view["message"], duration_ms,
                    _strip_internal(query),
                    len(safe_results) if isinstance(safe_results, list) else 1,
                    _client_session_id(), request.remote_addr,
                    user.get("username"), user.get("role"),
                )
                update_session_activity(_client_session_id())
            except Exception:
                pass
            socketio.emit("dashboard_update")

        except Exception:
            view["status"] = "FAILED"
            view["message"] = "Operation failed. Please verify input and backend availability."
            try:
                log_query(
                    "read", "FAILED", view["message"], 0,
                    _strip_internal(query), 0,
                    _client_session_id(), request.remote_addr,
                    user.get("username"), user.get("role"),
                )
                update_session_activity(_client_session_id())
            except Exception:
                pass
            socketio.emit("dashboard_update")

    return render_template("query.html", **ctx, **view)


# ============================================================================
# Records
# ============================================================================

@app.get("/records/inspect")
def records_field_inspector():
    ctx = _base_context()
    error = ""
    field_rows = []
    key_value = request.args.get("key", "").strip()
    global_key = ""

    if not ctx["meta"]:
        error = "Schema not registered."
    elif not key_value:
        error = "No record key provided."
    else:
        meta = ctx["meta"]
        global_key = meta.get("global_key", "id")
        try:
            key_cast = int(key_value)
        except ValueError:
            key_cast = key_value
        try:
            query = {"operation": "read", "fields": ["*"], "where": {global_key: key_cast}}
            results = execute_read(query, meta)
            cleaned = _strip_internal(results)
            if isinstance(cleaned, list) and cleaned:
                record = cleaned[0]
                field_rows = sorted(
                    [{"field": k, "value": v, "value_type": type(v).__name__}
                     for k, v in record.items()],
                    key=lambda x: x["field"],
                )
        except Exception:
            error = "Failed to retrieve record."

    ctx.update({
        "page_title": "Record Inspector",
        "key_value": key_value,
        "global_key": global_key,
        "field_rows": field_rows,
        "error": error,
    })
    return render_template("field_inspector.html", **ctx)


# ============================================================================
# Strict validation helpers
# ============================================================================

def _normalize_schema_type(type_name: Any) -> str:
    try:
        return str(type_name or "").strip().lower()
    except Exception:
        return ""


def _schema_field_meta(meta: dict | None, field_name: str) -> dict | None:
    try:
        if not meta:
            return None
        return (meta.get("fields", {}) or {}).get(field_name)
    except Exception:
        return None


def _is_value_of_type(expected_type: str, value: Any) -> bool:
    if value is None:
        return True
    t = _normalize_schema_type(expected_type)
    if t in ("int", "integer"):
        return isinstance(value, int) and not isinstance(value, bool)
    if t in ("float", "double", "number", "numeric"):
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if t in ("bool", "boolean"):
        return isinstance(value, bool)
    if t in ("str", "string", "text"):
        return isinstance(value, str)
    if t in ("array", "list"):
        return isinstance(value, list)
    if t in ("object", "dict", "map"):
        return isinstance(value, dict)
    return True


def _validate_payload_strict(
    meta: dict | None,
    payload: Any,
    *,
    operation_label: str,
    allow_unknown_fields: bool = True,
) -> tuple[bool, str]:
    if not meta:
        return True, "OK"
    if not isinstance(payload, dict):
        return False, f"{operation_label} must be a JSON object."
    for k, v in payload.items():
        if k in INTERNAL_KEYS:
            continue
        fmeta = _schema_field_meta(meta, k)
        if not fmeta:
            if allow_unknown_fields:
                continue
            return False, f"Unknown field '{k}'."
        if v is None and fmeta.get("not_null"):
            return False, f"Field '{k}' is required (not null)."
        # NOTE: type mismatches are intentionally NOT rejected here. The engine
        # owns type handling: it coerces safe/representational mismatches
        # (12345 -> "12345", "42" -> 42) and applies the configured
        # TYPE_CONFLICT_POLICY for genuinely un-coercible values (adaptive ->
        # widen field to Mongo; strict -> reject with a clear message). Doing a
        # strict isinstance check here would contradict the engine and block
        # both coercion and the type-drift feature.
    return True, "OK"


def _strict_fail(view: dict, ctx: dict, template_name: str, status: str, message: str):
    view["status"] = status
    view["message"] = message
    return render_template(template_name, **ctx, **view)


# ============================================================================
# Insert — CHANGED: handles replay for FAILED inserts
# ============================================================================

@app.route("/insert", methods=["GET", "POST"])
def insert_page():
    user = _require_auth()
    if not isinstance(user, dict):
        return user

    ctx = _base_context()
    ctx["user"] = user

    default_data = '{"customer_id": 99999, "name": "Alice", "email": "alice@example.com"}'

    # Replay: only for FAILED inserts
    replay_id = request.args.get("replay", "").strip()
    if replay_id:
        try:
            all_history = get_history(limit=200)
            match = next(
                (e for e in all_history if str(e.get("id")) == replay_id), None
            )
            if (
                match
                and match.get("query_payload")
                and match.get("operation") == "insert"
                and match.get("status") == "FAILED"
            ):
                payload = match["query_payload"]
                # query_payload is stored as {"data": {...}}
                data_part = payload.get("data", payload)
                default_data = json.dumps(data_part, indent=2)
        except Exception:
            pass

    view = {
        "page_title": "Insert",
        "data_json": default_data,
        "status": "",
        "message": "",
        "routing_json": "",
    }

    if request.method == "POST" and ctx["meta"]:
        raw_data = request.form.get("data_json", "").strip()
        view["data_json"] = _safe_json_text(raw_data, view["data_json"])

        try:
            data = loads_strict_json(raw_data)
        except ValueError as exc:
            view["status"] = "FAILED"
            view["message"] = str(exc)
            return render_template("insert.html", **ctx, **view)

        ok, err = _validate_payload_strict(ctx.get("meta"), data,
                                           operation_label="Insert payload")
        if not ok:
            return _strict_fail(view, ctx, "insert.html", "FAILED", err)

        query = {"operation": "insert", "data": data}
        start_time = time.time()
        result = tc.execute(query)
        duration_ms = (time.time() - start_time) * 1000

        if result["success"]:
            view["status"] = "SUCCESS"
            view["message"] = "Insert completed"
            routing = (result.get("data") or {}).get("routing")
            if routing:
                view["routing_json"] = json.dumps(
                    _strip_internal(routing), indent=2, ensure_ascii=True
                )
        else:
            view["status"] = "FAILED"
            msg = result.get("message", "Operation failed.")
            if result.get("rolled_back"):
                msg += " (transaction was rolled back)"
            view["message"] = msg

        try:
            log_query(
                "insert", view["status"], view["message"], duration_ms,
                _strip_internal({"data": data}),
                1 if result["success"] else 0,
                _client_session_id(), request.remote_addr,
                user.get("username"), user.get("role"),
            )
            update_session_activity(_client_session_id())
        except Exception:
            pass
        socketio.emit("dashboard_update")

    return render_template("insert.html", **ctx, **view)


# ============================================================================
# Update — CHANGED: handles replay for all updates
# ============================================================================

@app.route("/update", methods=["GET", "POST"])
def update_page():
    user = _require_auth()
    if not isinstance(user, dict):
        return user

    ctx = _base_context()
    ctx["user"] = user

    default_where = '{"customer_id": 99999}'
    default_data  = '{"name": "Alice Updated"}'
    default_entity = ""

    # Replay: always allowed for update
    replay_id = request.args.get("replay", "").strip()
    if replay_id:
        try:
            all_history = get_history(limit=200)
            match = next(
                (e for e in all_history if str(e.get("id")) == replay_id), None
            )
            if match and match.get("query_payload") and match.get("operation") == "update":
                payload = match["query_payload"]
                # query_payload stored as {"where": {...}, "data": {...}}
                where_part  = payload.get("where", {})
                data_part   = payload.get("data", {})
                entity_part = payload.get("entity", "")
                default_where  = json.dumps(where_part, indent=2)
                default_data   = json.dumps(data_part, indent=2)
                default_entity = entity_part or ""
        except Exception:
            pass

    view = {
        "page_title": "Update",
        "entity": default_entity,
        "where_json": default_where,
        "data_json": default_data,
        "status": "",
        "message": "",
        "routing_json": "",
    }

    if request.method == "POST" and ctx["meta"]:
        entity    = request.form.get("entity", "").strip()
        raw_where = request.form.get("where_json", "").strip()
        raw_data  = request.form.get("data_json", "").strip()

        if entity:
            fmeta = (ctx["meta"].get("fields", {}) if ctx.get("meta") else {}).get(entity)
            if not fmeta or fmeta.get("type") not in ("array", "object"):
                entity = ""

        view["entity"]     = entity
        view["where_json"] = _safe_json_text(raw_where, view["where_json"])
        view["data_json"]  = _safe_json_text(raw_data,  view["data_json"])

        try:
            where = loads_strict_json(raw_where)
            data  = loads_strict_json(raw_data)
        except ValueError as exc:
            view["status"]  = "FAILED"
            view["message"] = str(exc)
            return render_template("update.html", **ctx, **view)

        ok_w, err_w = _validate_payload_strict(ctx.get("meta"), where,
                                               operation_label="Update where")
        if not ok_w:
            return _strict_fail(view, ctx, "update.html", "FAILED", err_w)

        ok_d, err_d = _validate_payload_strict(ctx.get("meta"), data,
                                               operation_label="Update data")
        if not ok_d:
            return _strict_fail(view, ctx, "update.html", "FAILED", err_d)

        query = {"operation": "update", "where": where, "data": data}
        if entity:
            query["entity"] = entity

        start_time = time.time()
        result = tc.execute(query)
        duration_ms = (time.time() - start_time) * 1000

        if result["success"]:
            view["status"]  = "SUCCESS"
            view["message"] = "Update completed"
            routing = (result.get("data") or {}).get("routing")
            if routing:
                view["routing_json"] = json.dumps(
                    _strip_internal(routing), indent=2, ensure_ascii=True
                )
        else:
            view["status"] = "FAILED"
            msg = result.get("message", "Operation failed.")
            if result.get("rolled_back"):
                msg += " (transaction was rolled back)"
            view["message"] = msg

        try:
            log_query(
                "update", view["status"], view["message"], duration_ms,
                _strip_internal({"where": where, "data": data}),
                1 if result["success"] else 0,
                _client_session_id(), request.remote_addr,
                user.get("username"), user.get("role"),
            )
            update_session_activity(_client_session_id())
        except Exception:
            pass
        socketio.emit("dashboard_update")

    return render_template("update.html", **ctx, **view)


# ============================================================================
# Delete — no replay (unchanged logic)
# ============================================================================

@app.route("/delete", methods=["GET", "POST"])
def delete_page():
    user = _require_auth()
    if not isinstance(user, dict):
        return user

    ctx = _base_context()
    ctx["user"] = user

    view = {
        "page_title": "Delete",
        "entity": "",
        "where_json": '{"customer_id": 99999}',
        "status": "",
        "message": "",
    }

    if request.method == "POST" and ctx["meta"]:
        entity    = request.form.get("entity", "").strip()
        raw_where = request.form.get("where_json", "").strip()

        view["entity"]     = entity
        view["where_json"] = _safe_json_text(raw_where, view["where_json"])

        try:
            where = loads_strict_json(raw_where)
        except ValueError as exc:
            view["status"]  = "FAILED"
            view["message"] = str(exc)
            return render_template("delete.html", **ctx, **view)

        ok_w, err_w = _validate_payload_strict(ctx.get("meta"), where,
                                               operation_label="Delete where")
        if not ok_w:
            return _strict_fail(view, ctx, "delete.html", "FAILED", err_w)

        query = {"operation": "delete", "where": where}
        if entity:
            query["entity"] = entity

        start_time = time.time()
        result = tc.execute(query)
        duration_ms = (time.time() - start_time) * 1000

        if result["success"]:
            view["status"]  = "SUCCESS"
            view["message"] = "Delete completed"
        else:
            view["status"] = "FAILED"
            msg = result.get("message", "Operation failed.")
            if result.get("rolled_back"):
                msg += " (transaction was rolled back)"
            view["message"] = msg

        try:
            log_query(
                "delete", view["status"], view["message"], duration_ms,
                _strip_internal({"where": where}),
                1 if result["success"] else 0,
                _client_session_id(), request.remote_addr,
                user.get("username"), user.get("role"),
            )
            update_session_activity(_client_session_id())
        except Exception:
            pass
        socketio.emit("dashboard_update")

    return render_template("delete.html", **ctx, **view)


# ============================================================================
# Reliability Tests
# ============================================================================

@app.route("/reliability-test", methods=["GET", "POST"])
def reliability_test_page():
    user = _require_developer()
    if not isinstance(user, dict):
        return user

    ctx = _base_context()
    group_map = _reliability_group_map()

    view = {
        "page_title": "Reliability Tests",
        "reliability_sections": _reliability_sections(group_map),
        "race_rounds": "4",
        "parallel_inserts": "8",
        "stop_on_failure": True,
        "selected_group": "",
        "selected_group_title": "",
        "results": [],
        "status": "",
        "message": "",
    }

    if request.method == "POST":
        if not ctx["meta"]:
            view["status"] = "FAILED"
            view["message"] = "Metadata is missing or invalid JSON."
            return render_template("reliability_test.html", **ctx, **view)

        selected_group       = str(request.form.get("test_group", "A")).upper()
        race_rounds_raw      = request.form.get("race_rounds", "4").strip()
        parallel_inserts_raw = request.form.get("parallel_inserts", "8").strip()
        stop_on_failure      = request.form.get("stop_on_failure") == "1"

        race_rounds      = _parse_positive_int(race_rounds_raw, 4)
        parallel_inserts = _parse_positive_int(parallel_inserts_raw, 8)

        view["race_rounds"]      = str(race_rounds)
        view["parallel_inserts"] = str(parallel_inserts)
        view["stop_on_failure"]  = stop_on_failure

        if selected_group not in group_map:
            view["status"]  = "FAILED"
            view["message"] = "Unknown test group selected."
            return render_template("reliability_test.html", **ctx, **view)

        view["selected_group"]       = selected_group
        view["selected_group_title"] = group_map[selected_group]["title"]

        global_key = ctx["meta"].get("global_key") if ctx.get("meta") else None
        if not global_key:
            view["status"]  = "FAILED"
            view["message"] = "Metadata missing global_key."
            return render_template("reliability_test.html", **ctx, **view)

        runner_probe = TransactionCoordinator()
        sql_ok, mongo_ok = runner_probe._check_backends()
        if not (sql_ok and mongo_ok):
            view["status"]  = "FAILED"
            view["message"] = (
                f"Backends unavailable "
                f"(sql_available={sql_ok}, mongo_available={mongo_ok})."
            )
            return render_template("reliability_test.html", **ctx, **view)

        opts     = SimpleNamespace(race_rounds=race_rounds, parallel_inserts=parallel_inserts)
        tests    = group_map[selected_group]["tests"]
        failures = 0
        stopped_early = False

        for test_name, test_fn in tests:
            try:
                runner = TransactionCoordinator()
                test_fn(runner, global_key, opts)
                view["results"].append({"name": test_name, "status": "PASS", "detail": "OK"})
            except Exception as exc:
                failures += 1
                view["results"].append({"name": test_name, "status": "FAIL", "detail": str(exc)})
                if stop_on_failure:
                    stopped_early = True
                    break

        executed = len(view["results"])
        passed   = executed - failures
        if failures == 0:
            view["status"]  = "SUCCESS"
            view["message"] = f"{view['selected_group_title']} tests passed ({passed}/{executed})."
        else:
            view["status"] = "FAILED"
            if stopped_early:
                view["message"] = (
                    f"{view['selected_group_title']} stopped after first failure "
                    f"({passed}/{executed} passed)."
                )
            else:
                view["message"] = (
                    f"{view['selected_group_title']} tests failed ({passed}/{executed} passed)."
                )

    return render_template("reliability_test.html", **ctx, **view)


# ============================================================================
# Sessions
# ============================================================================

@app.route("/sessions", methods=["GET", "POST"])
def sessions_page():
    user = _require_auth()
    if not isinstance(user, dict):
        return user

    client_id = _client_session_id()
    start_session(client_id, user.get("username"), user.get("role"))

    session_data  = get_session(client_id)
    recent_queries = get_history(limit=5)
    mongo_status  = not _mongo_unreachable()
    buf_count     = _buf_count()
    stg_count     = _staging_count()

    ctx = _base_context()
    ctx["user"] = user

    if user.get("role") == "developer":
        try:
            with open(paths.SESSION_FILE, "r", encoding="utf-8") as f:
                session_store = json.load(f)
            all_sessions = list(session_store.get("sessions", {}).values())
        except Exception:
            all_sessions = []

        all_users_list = get_all_users()
        ctx.update({
            "all_sessions": all_sessions,
            "all_users":    all_users_list,
            "is_developer": True,
        })

    ctx.update({
        "page_title":     "Active Sessions",
        "session_data":   session_data,
        "recent_queries": recent_queries,
        "mongo_status":   mongo_status,
        "buffer_count":   buf_count,
        "staging_count":  stg_count,
    })
    return render_template("sessions.html", **ctx)


# ============================================================================
# Users
# ============================================================================

@app.route("/users", methods=["GET", "POST"])
def users_page():
    user = _require_auth()
    if not isinstance(user, dict):
        return user
    if user.get("role") != "developer":
        return redirect("/")

    ctx = _base_context()
    ctx["user"] = user
    ctx["create_user_message"] = ""
    ctx["create_user_error"]   = ""
    ctx["manage_user_message"] = ""
    ctx["manage_user_error"]   = ""

    if request.method == "POST":
        action = request.form.get("action", "create_user")

        if action == "create_user":
            new_username      = request.form.get("new_username", "").strip()
            new_password      = request.form.get("new_password", "").strip()
            confirm_password  = request.form.get("confirm_password", "").strip()
            new_role          = request.form.get("new_role", "user")

            if not new_username or not new_password:
                ctx["create_user_error"] = "Username and password are required."
            elif new_password != confirm_password:
                ctx["create_user_error"] = "Passwords do not match."
            elif len(new_password) < 6:
                ctx["create_user_error"] = "Password must be at least 6 characters."
            else:
                success, message = register_user(new_username, new_password, new_role)
                if success:
                    ctx["create_user_message"] = message
                else:
                    ctx["create_user_error"] = message

        elif action == "update_user":
            edit_username = request.form.get("edit_username", "").strip()
            edit_password = request.form.get("edit_password", "").strip()
            edit_role     = request.form.get("edit_role", "user")

            if not edit_username:
                ctx["manage_user_error"] = "Select a user to update."
            elif edit_username == user.get("username") and edit_role != user.get("role"):
                ctx["manage_user_error"] = "You cannot change your own role while logged in."
            else:
                password_to_update = edit_password if edit_password else None
                success, message = update_user(edit_username, password_to_update, edit_role)
                if success:
                    ctx["manage_user_message"] = message
                else:
                    ctx["manage_user_error"] = message

        elif action == "delete_user":
            delete_username = request.form.get("edit_username", "").strip()
            if not delete_username:
                ctx["manage_user_error"] = "Select a user to delete."
            elif delete_username == user.get("username"):
                ctx["manage_user_error"] = "You cannot delete your own account."
            else:
                success, message = delete_user(delete_username)
                if success:
                    ctx["manage_user_message"] = message
                    try:
                        delete_sessions_for_user(delete_username)
                    except Exception:
                        pass
                else:
                    ctx["manage_user_error"] = message

    all_users = get_all_users()
    ctx.update({
        "page_title": "Users",
        "all_users":  all_users,
        "is_developer": True,
    })
    return render_template("users.html", **ctx)


# ============================================================================
# Query History
# ============================================================================

@app.get("/query-history")
def query_history_page():
    user = _require_auth()
    if not isinstance(user, dict):
        return user

    all_history = get_history(limit=100)
    history = [h for h in all_history if h.get("username") == user["username"]]
    ctx = _base_context()
    ctx["user"] = user
    ctx.update({
        "page_title":  "Query History",
        "history":     history,
        "total_count": len(history),
    })
    return render_template("query_history.html", **ctx)


@app.get("/api/history")
def api_history():
    user = _require_auth()
    if not isinstance(user, dict):
        return {"error": "Authentication required"}, 401

    all_history = get_history(limit=100)
    history = [h for h in all_history if h.get("username") == user["username"]]
    return {"history": history, "total_count": len(history)}


# ============================================================================
# Exports
# ============================================================================

@app.get("/export/entities/<entity_name>/csv")
def export_entity_csv(entity_name: str):
    import csv
    import io

    ctx = _base_context()
    if not ctx["meta"]:
        return "Schema not registered", 400

    meta = ctx["meta"]
    try:
        query      = {"operation": "read", "fields": ["*"], "where": {}}
        raw_results = execute_read(query, meta)
        cleaned    = _strip_internal(raw_results)
        if not isinstance(cleaned, list) or not cleaned:
            return "No data found", 404

        records   = [r for r in cleaned if isinstance(r, dict)]
        key_union = set()
        for rec in records:
            key_union.update(rec.keys())
        global_key = meta.get("global_key", "")
        fieldnames = (
            [global_key] if global_key in key_union else []
        ) + sorted(k for k in key_union if k != global_key)

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for rec in records:
            flat = {}
            for k in fieldnames:
                v = rec.get(k)
                flat[k] = json.dumps(v) if isinstance(v, (list, dict)) else v
            writer.writerow(flat)

        output.seek(0)
        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment; filename={entity_name}.csv"},
        )
    except Exception as e:
        return f"Export failed: {e}", 500


@app.get("/export/query/json")
def export_query_json():
    ctx = _base_context()
    if not ctx["meta"]:
        return "Schema not registered", 400

    try:
        query       = {"operation": "read", "fields": ["*"], "where": {}}
        raw_results = execute_read(query, ctx["meta"])
        cleaned     = _strip_internal(raw_results)
        return Response(
            json.dumps(cleaned, indent=2, ensure_ascii=True),
            mimetype="application/json",
            headers={"Content-Disposition": "attachment; filename=query_results.json"},
        )
    except Exception as e:
        return f"Export failed: {e}", 500


# ============================================================================
# Records Browser
# ============================================================================

@app.get("/records")
def records_page():
    user = _require_auth()
    if not isinstance(user, dict):
        return user

    ctx = _base_context()
    ctx["user"] = user
    if not ctx["meta"]:
        return render_template("records.html", **ctx, error="Schema not registered.")

    meta       = ctx["meta"]
    global_key = meta.get("global_key", "id")

    try:
        page = int(request.args.get("page", 1))
    except ValueError:
        page = 1
    if page < 1:
        page = 1
    per_page = 25
    offset   = (page - 1) * per_page
    search   = (request.args.get("search", "") or "").strip().lower()

    try:
        query       = {"operation": "read", "fields": ["*"], "where": {}}
        raw_results = execute_read(query, meta)
        cleaned     = _strip_internal(raw_results)
        records_all = [r for r in cleaned if isinstance(r, dict)] if isinstance(cleaned, list) else []

        key_union = set()
        for rec in records_all:
            key_union.update(rec.keys())
        columns = (
            [global_key] if global_key in key_union else []
        ) + sorted(k for k in key_union if k != global_key)

        if search:
            records_all = [
                rec for rec in records_all
                if search in " ".join(str(v) for v in rec.values()).lower()
            ]

        total_count = len(records_all)
        total_pages = max(1, (total_count + per_page - 1) // per_page)
        records_page_slice = records_all[offset: offset + per_page]

        ctx.update({
            "page_title":  "All Records",
            "records":     records_page_slice,
            "columns":     columns,
            "global_key":  global_key,
            "search":      search,
            "page":        page,
            "per_page":    per_page,
            "total_count": total_count,
            "total_pages": total_pages,
            "has_prev":    page > 1,
            "has_next":    page < total_pages,
        })
        return render_template("records.html", **ctx)

    except Exception as e:
        ctx.update({"error": f"Failed to load records: {e}"})
        return render_template("records.html", **ctx)


# ============================================================================
# Entry Point
# ============================================================================

def main():
    """Dashboard entry point (console-script `hybriddb-dashboard`)."""
    print("Dashboard running at http://localhost:5001")
    socketio.run(app, host="0.0.0.0", port=5001, debug=False)


if __name__ == "__main__":
    main()