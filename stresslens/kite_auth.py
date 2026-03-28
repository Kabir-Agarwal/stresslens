"""
TradeMind — Zerodha Kite Connect OAuth Module
Handles login URL generation, callback token exchange, session storage.
"""

import os
import sqlite3
import hashlib
from datetime import datetime, timedelta

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "stresslens.db")

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def init_auth_db():
    """Create user_sessions table if it doesn't exist."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS user_sessions (
            user_id TEXT PRIMARY KEY,
            access_token TEXT NOT NULL,
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()


# Initialize on import
init_auth_db()


# ---------------------------------------------------------------------------
# Demo mode detection
# ---------------------------------------------------------------------------

def is_demo_mode() -> bool:
    """Return True if Kite API key is not configured."""
    api_key = os.getenv("KITE_API_KEY", "").strip()
    return not api_key


# ---------------------------------------------------------------------------
# Login URL
# ---------------------------------------------------------------------------

def get_login_url() -> str:
    """Generate Zerodha Kite Connect login URL."""
    if is_demo_mode():
        return "/auth/callback?request_token=demo_token&status=success"
    api_key = os.getenv("KITE_API_KEY", "")
    return f"https://kite.zerodha.com/connect/login?v=3&api_key={api_key}"


# ---------------------------------------------------------------------------
# Token exchange
# ---------------------------------------------------------------------------

def exchange_request_token(request_token: str) -> dict:
    """
    Exchange request_token for access_token.
    In demo mode, returns a fake session.
    In live mode, uses KiteConnect SDK.
    """
    if is_demo_mode() or request_token == "demo_token":
        user_id = "demo"
        access_token = "demo_access_token"
        store_session(user_id, access_token)
        return {"user_id": user_id, "access_token": access_token}

    try:
        from kiteconnect import KiteConnect
        api_key = os.getenv("KITE_API_KEY", "")
        api_secret = os.getenv("KITE_API_SECRET", "")

        kite = KiteConnect(api_key=api_key)
        data = kite.generate_session(request_token, api_secret=api_secret)

        user_id = data.get("user_id", "unknown")
        access_token = data.get("access_token", "")

        store_session(user_id, access_token)
        return {"user_id": user_id, "access_token": access_token}

    except Exception as e:
        raise RuntimeError(f"Token exchange failed: {e}")


# ---------------------------------------------------------------------------
# Session management
# ---------------------------------------------------------------------------

def store_session(user_id: str, access_token: str):
    """Store access token in database. Kite tokens expire at 6 AM next day."""
    now = datetime.now()
    # Kite access tokens expire at 6:00 AM the next trading day
    expires = (now + timedelta(days=1)).replace(hour=6, minute=0, second=0, microsecond=0)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT OR REPLACE INTO user_sessions (user_id, access_token, created_at, expires_at)
        VALUES (?, ?, ?, ?)
    """, (user_id, access_token, now.isoformat(), expires.isoformat()))
    conn.commit()
    conn.close()


def get_valid_token(user_id: str = None) -> dict:
    """
    Get a valid (non-expired) access token from the database.
    If user_id is None, returns the most recently created session.
    Returns dict with user_id, access_token or None.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    now = datetime.now().isoformat()

    if user_id:
        row = conn.execute(
            "SELECT * FROM user_sessions WHERE user_id = ? AND expires_at > ? ORDER BY created_at DESC LIMIT 1",
            (user_id, now)
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT * FROM user_sessions WHERE expires_at > ? ORDER BY created_at DESC LIMIT 1",
            (now,)
        ).fetchone()
    conn.close()

    if row:
        return {"user_id": row["user_id"], "access_token": row["access_token"]}
    return None


def is_authenticated(user_id: str = None) -> bool:
    """Check if there is a valid session."""
    return get_valid_token(user_id) is not None


def get_auth_status() -> dict:
    """Return authentication status for the current session."""
    session = get_valid_token()
    if session:
        return {"authenticated": True, "user_id": session["user_id"], "demo_mode": is_demo_mode()}
    return {"authenticated": False, "user_id": None, "demo_mode": is_demo_mode()}


def logout(user_id: str = None):
    """Clear access token(s) from database."""
    conn = sqlite3.connect(DB_PATH)
    if user_id:
        conn.execute("DELETE FROM user_sessions WHERE user_id = ?", (user_id,))
    else:
        conn.execute("DELETE FROM user_sessions")
    conn.commit()
    conn.close()


def get_kite_client():
    """
    Return an authenticated KiteConnect instance, or None if demo mode / not authenticated.
    """
    if is_demo_mode():
        return None

    session = get_valid_token()
    if not session:
        return None

    try:
        from kiteconnect import KiteConnect
        api_key = os.getenv("KITE_API_KEY", "")
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(session["access_token"])
        return kite
    except Exception:
        return None
