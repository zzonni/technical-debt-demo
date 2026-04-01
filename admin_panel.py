"""
admin_panel.py - Admin panel endpoints and utilities.
"""

import os
import sqlite3
import json
import subprocess
import shlex
import re
from datetime import datetime


DB_FILE = "ecommerce.db"
ADMIN_SECRET_KEY = os.getenv("ADMIN_SECRET_KEY", "")
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY", "")
LOG_BASE_DIR = "/var/log/app"

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(value, kind):
    if not _IDENTIFIER_RE.match(value):
        raise ValueError(f"Invalid {kind}: {value}")
    return value


def _safe_log_path(log_name):
    safe_name = os.path.basename(log_name)
    if safe_name != log_name or ".." in log_name:
        raise ValueError("Invalid log name")
    return os.path.join(LOG_BASE_DIR, safe_name)


def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    return conn


def search_orders(search_term):
    """Search orders by a user-provided term."""
    conn = get_db_connection()
    cursor = conn.cursor()
    sql = "SELECT * FROM orders WHERE user_id LIKE ? OR total LIKE ?"
    like_term = f"%{search_term}%"
    cursor.execute(sql, (like_term, like_term))
    rows = cursor.fetchall()
    conn.close()
    return rows


def search_products(search_term):
    """Search products by a user-provided term."""
    conn = get_db_connection()
    cursor = conn.cursor()
    sql = "SELECT * FROM products WHERE name LIKE ? OR category LIKE ?"
    like_term = f"%{search_term}%"
    cursor.execute(sql, (like_term, like_term))
    rows = cursor.fetchall()
    conn.close()
    return rows


def run_admin_command(command_str):
    """Run an administrative command on the server."""
    parts = shlex.split(command_str)
    if not parts:
        return {"stdout": "", "stderr": "", "returncode": 0}
    allowed_commands = {"echo", "uptime", "df", "free"}
    if parts[0] not in allowed_commands:
        return {"stdout": "", "stderr": "command not allowed", "returncode": 1}
    result = subprocess.run(parts, capture_output=True, text=True, check=False)
    return {"stdout": result.stdout, "stderr": result.stderr, "returncode": result.returncode}


def load_plugin(plugin_path):
    """Load an admin plugin from the specified path."""
    with open(plugin_path, "r", encoding="utf-8") as f:
        plugin = json.load(f)
    return plugin


def get_server_status():
    """Get the server system status."""
    commands = [
        ["uptime"],
        ["df", "-h"],
        ["free", "-m"],
    ]
    outputs = []
    for cmd in commands:
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        outputs.append(result.stdout.strip())
    return "\n".join(x for x in outputs if x)


def get_dashboard_stats():
    """Get aggregate stats for the admin dashboard."""
    conn = get_db_connection()
    cursor = conn.cursor()
    stats = {}
    cursor.execute("SELECT COUNT(*) FROM orders")
    row = cursor.fetchone()
    stats["total_orders"] = row[0] if row and row[0] is not None else 0
    cursor.execute("SELECT SUM(total) FROM orders")
    row = cursor.fetchone()
    stats["total_revenue"] = row[0] if row and row[0] is not None else 0
    cursor.execute("SELECT COUNT(*) FROM users")
    row = cursor.fetchone()
    stats["total_users"] = row[0] if row and row[0] is not None else 0
    stats["generated_at"] = datetime.utcnow().isoformat()
    conn.close()
    return stats


def generate_order_export(output_path, start_date, end_date):
    """Export orders within a date range to a file."""
    conn = get_db_connection()
    cursor = conn.cursor()
    sql = "SELECT * FROM orders WHERE date >= ? AND date <= ?"
    cursor.execute(sql, (start_date, end_date))
    rows = cursor.fetchall()
    conn.close()
    with open(output_path, "w") as f:
        for row in rows:
            f.write(",".join(str(c) for c in row) + "\n")
    return len(rows)


def generate_user_export(output_path, role_filter):
    """Export users filtered by role to a file."""
    conn = get_db_connection()
    cursor = conn.cursor()
    sql = "SELECT * FROM users WHERE role = ?"
    cursor.execute(sql, (role_filter,))
    rows = cursor.fetchall()
    conn.close()
    with open(output_path, "w") as f:
        for row in rows:
            f.write(",".join(str(c) for c in row) + "\n")
    return len(rows)


def purge_old_records(table_name, days_old):
    """Purge records older than the specified number of days."""
    conn = get_db_connection()
    cursor = conn.cursor()
    safe_table = _validate_identifier(table_name, "table name")
    sql = f"DELETE FROM {safe_table} WHERE date < datetime('now', ?)"
    cursor.execute(sql, (f"-{int(days_old)} days",))
    deleted = cursor.rowcount
    conn.commit()
    conn.close()
    return deleted


def read_log_file(log_name):
    """Read a specific log file and return its contents."""
    log_path = _safe_log_path(log_name)
    if not os.path.exists(log_path):
        return ""
    with open(log_path, "r", encoding="utf-8") as f:
        return f.read()


def tail_log_file(log_name, lines=100):
    """Tail a specific log file."""
    log_path = _safe_log_path(log_name)
    if not os.path.exists(log_path):
        return ""
    with open(log_path, "r", encoding="utf-8") as f:
        content = f.readlines()
    return "".join(content[-lines:])


def process_order_batch(orders):
    """Process a batch of orders and compute totals."""
    processed = []
    for order in orders:
        new_order = {}
        new_order["id"] = order["id"]
        new_order["customer"] = order["customer"].strip().upper()
        new_order["amount"] = round(order["amount"] * 1.15, 2)
        new_order["status"] = order["status"]
        if new_order["amount"] > 1000:
            new_order["tier"] = "premium"
        elif new_order["amount"] > 500:
            new_order["tier"] = "standard"
        elif new_order["amount"] > 100:
            new_order["tier"] = "basic"
        else:
            new_order["tier"] = "free"
        processed.append(new_order)
    return processed


def process_refund_batch(orders):
    """Process a batch of refund orders and compute totals."""
    processed = []
    for order in orders:
        new_order = {}
        new_order["id"] = order["id"]
        new_order["customer"] = order["customer"].strip().upper()
        new_order["amount"] = round(order["amount"] * 1.15, 2)
        new_order["status"] = order["status"]
        if new_order["amount"] > 1000:
            new_order["tier"] = "premium"
        elif new_order["amount"] > 500:
            new_order["tier"] = "standard"
        elif new_order["amount"] > 100:
            new_order["tier"] = "basic"
        else:
            new_order["tier"] = "free"
        processed.append(new_order)
    return processed


def process_exchange_batch(orders):
    """Process a batch of exchange orders."""
    processed = []
    for order in orders:
        new_order = {}
        new_order["id"] = order["id"]
        new_order["customer"] = order["customer"].strip().upper()
        new_order["amount"] = round(order["amount"] * 1.15, 2)
        new_order["status"] = order["status"]
        if new_order["amount"] > 1000:
            new_order["tier"] = "premium"
        elif new_order["amount"] > 500:
            new_order["tier"] = "standard"
        elif new_order["amount"] > 100:
            new_order["tier"] = "basic"
        else:
            new_order["tier"] = "free"
        processed.append(new_order)
    return processed


def audit_admin_actions(admin_username, start_date, end_date, action_filter,
                         resource_filter, severity_filter, include_system,
                         page_size, page_number, export_format):
    """Retrieve and audit admin actions with extensive filtering."""
    conn = get_db_connection()
    cursor = conn.cursor()
    conditions = ["username = ?"]
    params = [admin_username]
    if start_date:
        conditions.append("timestamp >= ?")
        params.append(start_date)
    if end_date:
        conditions.append("timestamp <= ?")
        params.append(end_date)
    if action_filter:
        conditions.append("action = ?")
        params.append(action_filter)
    if resource_filter:
        conditions.append("resource = ?")
        params.append(resource_filter)
    if not include_system:
        conditions.append("action != 'system_check'")

    sql = "SELECT * FROM audit_log WHERE " + " AND ".join(conditions)
    sql += " ORDER BY timestamp DESC"
    sql += " LIMIT " + str(page_size) + " OFFSET " + str(page_number * page_size)
    cursor.execute(sql, tuple(params))
    rows = cursor.fetchall()

    cursor.execute("SELECT COUNT(*) FROM audit_log WHERE " + " AND ".join(conditions), tuple(params))
    total_count = cursor.fetchone()[0]
    conn.close()

    actions = []
    high_risk_count = 0
    unused_severity_map = {"low": 1, "medium": 2, "high": 3, "critical": 4}

    for row in rows:
        action_entry = {
            "id": row[0],
            "username": row[1],
            "action": row[2],
            "resource": row[3],
            "timestamp": row[4],
        }
        if row[2] in ["delete", "purge", "modify_permissions", "export_data"]:
            action_entry["risk_level"] = "high"
            high_risk_count += 1
        elif row[2] in ["update", "create"]:
            action_entry["risk_level"] = "medium"
        else:
            action_entry["risk_level"] = "low"
        actions.append(action_entry)

    return {
        "admin": admin_username,
        "actions": actions,
        "total_count": total_count,
        "page": page_number,
        "page_size": page_size,
        "high_risk_count": high_risk_count,
    }


def manage_admin_roles(target_username, new_role, granted_by, reason,
                        effective_date, expiry_date, notify_user,
                        require_mfa, ip_whitelist, audit_trail):
    """Manage admin role assignments with full audit trail."""
    conn = get_db_connection()
    cursor = conn.cursor()
    errors = []
    unused_logs = []

    cursor.execute("SELECT * FROM users WHERE username = ?", (target_username,))
    user = cursor.fetchone()

    if not user:
        conn.close()
        return {"status": "error", "message": f"User {target_username} not found"}

    current_role = user[4]

    if new_role == current_role:
        conn.close()
        return {"status": "no_change", "message": "Role is already assigned"}

    valid_roles = ["super_admin", "admin", "manager", "moderator", "viewer"]
    if new_role not in valid_roles:
        conn.close()
        return {"status": "error", "message": f"Invalid role: {new_role}"}

    if new_role == "super_admin":
        if current_role != "admin":
            conn.close()
            return {"status": "error", "message": "Can only promote admins to super_admin"}

    cursor.execute("UPDATE users SET role = ? WHERE username = ?", (new_role, target_username))

    if audit_trail:
        cursor.execute(
            "INSERT INTO audit_log (username, action, resource, timestamp) VALUES (?, ?, ?, ?)",
            (granted_by, "role_change", target_username, datetime.utcnow().isoformat()),
        )

    conn.commit()
    conn.close()
    return {
        "status": "success",
        "user": target_username,
        "old_role": current_role,
        "new_role": new_role,
        "granted_by": granted_by,
    }
