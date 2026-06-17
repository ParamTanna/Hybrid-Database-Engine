import json
import os
import uuid
from datetime import datetime, timedelta, timezone
import hashlib

from hybriddb.config import paths

HISTORY_FILE = paths.HISTORY_FILE
SESSION_FILE = paths.SESSION_FILE
USERS_FILE = paths.USERS_FILE

# Helper function to get current UTC time as ISO format
def _current_utc_time():
    return datetime.now(timezone.utc).isoformat()

# Helper function to hash passwords
def _hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

# 1. Log a query
def log_query(operation: str, status: str, message: str, duration_ms: float, query_payload: dict, result_count: int, session_id: str = "", ip_address: str = "", username: str = "", user_role: str = "") -> None:
    try:
        # Load existing history
        if os.path.exists(HISTORY_FILE):
            with open(HISTORY_FILE, "r", encoding="utf-8") as f:
                history = json.load(f)
        else:
            history = []

        # Prepare new record with current timestamp
        query_timestamp = _current_utc_time()
        record = {
            "id": uuid.uuid4().hex,
            "operation": operation,
            "status": status,
            "message": message,
            "duration_ms": duration_ms,
            "query_payload": {k: v for k, v in query_payload.items() if not k.startswith("_")},
            "result_count": result_count,
            "timestamp": query_timestamp,
            "session_id": session_id,
            "ip_address": ip_address,
            "username": username,
            "user_role": user_role,
        }

        # Append and trim to last 200 entries
        history.append(record)
        if len(history) > 200:
            history = history[-200:]

        # Save back to file
        with open(HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(history, f, indent=2)
        
        # Update session's last_active with the query timestamp to keep it in sync
        if session_id:
            _update_session_last_active(session_id, query_timestamp)
    except Exception as e:
        print(f"Error logging query: {e}")

# 2. Get history
def get_history(limit: int = 50) -> list[dict]:
    try:
        if not os.path.exists(HISTORY_FILE):
            return []
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            history = json.load(f)
        return history[-limit:][::-1]  # Return newest first
    except Exception as e:
        print(f"Error retrieving history: {e}")
        return []

# 3. Clear history
def clear_history() -> int:
    try:
        if not os.path.exists(HISTORY_FILE):
            return 0
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            history = json.load(f)
        count = len(history)
        os.remove(HISTORY_FILE)
        return count
    except Exception as e:
        print(f"Error clearing history: {e}")
        return 0

def _load_sessions() -> dict:
    try:
        if not os.path.exists(SESSION_FILE):
            return {"sessions": {}}
        with open(SESSION_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict) and "sessions" in data and isinstance(data["sessions"], dict):
            return data
        if isinstance(data, dict) and "session_id" in data:
            return {"sessions": {"legacy": data}}
        return {"sessions": {}}
    except Exception:
        return {"sessions": {}}


def _save_sessions(data: dict) -> None:
    try:
        with open(SESSION_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"Error saving sessions: {e}")


def delete_session(client_id: str) -> bool:
    try:
        data = _load_sessions()
        sessions = data["sessions"]
        if client_id in sessions:
            del sessions[client_id]
            _save_sessions(data)
            return True
        return False
    except Exception as e:
        print(f"Error deleting session: {e}")
        return False


def delete_sessions_for_user(username: str) -> int:
    try:
        data = _load_sessions()
        sessions = data["sessions"]
        deleted = [key for key, session in sessions.items() if session.get("username") == username]
        for key in deleted:
            del sessions[key]
        if deleted:
            _save_sessions(data)
        return len(deleted)
    except Exception as e:
        print(f"Error deleting sessions for user {username}: {e}")
        return 0


# 4. Start a session
def start_session(client_id: str, username: str = "", user_role: str = "") -> dict:
    try:
        now = datetime.now(timezone.utc)
        data = _load_sessions()
        sessions = data["sessions"]
        session = sessions.get(client_id)
        if session:
            started_at = datetime.fromisoformat(session["started_at"])
            if now - started_at < timedelta(hours=2):
                # Update user info if provided
                if username:
                    session["username"] = username
                if user_role:
                    session["user_role"] = user_role
                return session

        session = {
            "session_id": str(uuid.uuid4()),
            "started_at": _current_utc_time(),
            "last_active": _current_utc_time(),
            "query_count": 0,
            "username": username,
            "user_role": user_role,
        }
        sessions[client_id] = session
        _save_sessions(data)
        return session
    except Exception as e:
        print(f"Error starting session: {e}")
        return {}


# 5. Get session
def get_session(client_id: str) -> dict | None:
    try:
        data = _load_sessions()
        return data["sessions"].get(client_id)
    except Exception as e:
        print(f"Error retrieving session: {e}")
        return None


# 6. Update session last_active with a specific timestamp (used by log_query)
def _update_session_last_active(client_id: str, timestamp: str) -> None:
    """
    Updates a session's last_active field with the given timestamp.
    This ensures last_active stays in sync with actual query timestamps.
    The client_id is the key in the sessions dict (passed as session_id from log_query).
    """
    try:
        data = _load_sessions()
        sessions = data["sessions"]
        session = sessions.get(client_id)
        if session:
            session["last_active"] = timestamp
            _save_sessions(data)
    except Exception as e:
        print(f"Error updating session last_active timestamp: {e}")


# 7. Update session activity
def update_session_activity(client_id: str) -> None:
    """
    Updates session activity timestamp and query count.
    Syncs last_active with the latest query timestamp from history.
    """
    try:
        data = _load_sessions()
        sessions = data["sessions"]
        session = sessions.get(client_id)
        if not session:
            return
        
        # Sync last_active with the latest query timestamp for this session
        session["query_count"] = session.get("query_count", 0) + 1
        
        # Find the most recent query for this session to get accurate last_active
        try:
            if os.path.exists(HISTORY_FILE):
                with open(HISTORY_FILE, "r", encoding="utf-8") as f:
                    history = json.load(f)
                    # Find latest query for this session
                    for entry in reversed(history):
                        if entry.get("session_id") == client_id:
                            session["last_active"] = entry.get("timestamp", _current_utc_time())
                            _save_sessions(data)
                            return
        except Exception:
            pass
        
        # Fallback: if no query found in history, use current time
        session["last_active"] = _current_utc_time()
        _save_sessions(data)
    except Exception as e:
        print(f"Error updating session activity: {e}")


def get_active_user_count(active_window_minutes: int = 5) -> int:
    try:
        data = _load_sessions()
        now = datetime.now(timezone.utc)
        count = 0
        for session in data["sessions"].values():
            try:
                last_active_str = session.get("last_active", "")
                if not last_active_str:
                    continue
                last_active = datetime.fromisoformat(last_active_str)
                if (now - last_active).total_seconds() <= active_window_minutes * 60:
                    count += 1
            except Exception:
                continue
        return count
    except Exception:
        return 0


def get_request_count_last_n_minutes(minutes: int = 5) -> int:
    """
    Returns number of queries executed in last N minutes.
    Uses query history timestamps.
    """
    try:
        history = get_history(limit=200)
        if not history:
            return 0
        cutoff = datetime.now(timezone.utc)
        count = 0
        for entry in history:
            try:
                ts = datetime.fromisoformat(entry.get("timestamp", ""))
                diff = cutoff - ts
                if diff.total_seconds() <= minutes * 60:
                    count += 1
            except Exception:
                continue
        return count
    except Exception:
        return 0


# User Management Functions

def _load_users() -> dict:
    """Load users from users.json file."""
    try:
        if not os.path.exists(USERS_FILE):
            return {"users": {}}
        with open(USERS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict) and "users" in data and isinstance(data["users"], dict):
            return data
        return {"users": {}}
    except Exception:
        return {"users": {}}


def _save_users(data: dict) -> None:
    """Save users to users.json file."""
    try:
        with open(USERS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"Error saving users: {e}")


def register_user(username: str, password: str, role: str = "user") -> tuple[bool, str]:
    """
    Register a new user.
    Returns (success, message)
    """
    try:
        if role not in ["user", "developer"]:
            return False, "Invalid role. Must be 'user' or 'developer'"

        data = _load_users()
        users = data["users"]

        if username in users:
            return False, "Username already exists"

        user = {
            "username": username,
            "password_hash": _hash_password(password),
            "role": role,
            "created_at": _current_utc_time(),
            "last_login": None,
        }

        users[username] = user
        _save_users(data)
        return True, "User registered successfully"
    except Exception as e:
        return False, f"Error registering user: {e}"


def authenticate_user(username: str, password: str) -> tuple[bool, dict | None]:
    """
    Authenticate a user.
    Returns (success, user_data)
    """
    try:
        data = _load_users()
        users = data["users"]

        user = users.get(username)
        if not user:
            return False, None

        if user["password_hash"] != _hash_password(password):
            return False, None

        # Update last login
        user["last_login"] = _current_utc_time()
        _save_users(data)

        return True, user
    except Exception as e:
        print(f"Error authenticating user: {e}")
        return False, None


def get_user(username: str) -> dict | None:
    """Get user data by username."""
    try:
        data = _load_users()
        return data["users"].get(username)
    except Exception:
        return None


def get_all_users() -> list[dict]:
    """Get all users (for developer view)."""
    try:
        data = _load_users()
        return list(data["users"].values())
    except Exception:
        return []


def update_user_last_login(username: str) -> None:
    """Update user's last login timestamp."""
    try:
        data = _load_users()
        users = data["users"]
        if username in users:
            users[username]["last_login"] = _current_utc_time()
            _save_users(data)
    except Exception as e:
        print(f"Error updating user last login: {e}")


def update_user(username: str, password: str | None = None, role: str | None = None) -> tuple[bool, str]:
    try:
        data = _load_users()
        users = data["users"]
        user = users.get(username)
        if not user:
            return False, "User not found"

        if role is not None:
            if role not in ["user", "developer"]:
                return False, "Invalid role. Must be 'user' or 'developer'"
            user["role"] = role

        if password is not None and password != "":
            if len(password) < 6:
                return False, "Password must be at least 6 characters long"
            user["password_hash"] = _hash_password(password)

        _save_users(data)
        return True, "User updated successfully"
    except Exception as e:
        return False, f"Error updating user: {e}"


def delete_user(username: str) -> tuple[bool, str]:
    try:
        data = _load_users()
        users = data["users"]
        if username not in users:
            return False, "User not found"
        del users[username]
        _save_users(data)
        return True, "User deleted successfully"
    except Exception as e:
        return False, f"Error deleting user: {e}"
