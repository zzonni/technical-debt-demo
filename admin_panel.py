"""
admin_panel.py - Admin panel endpoints and utilities.
"""

import os
import json
from pathlib import Path
import shlex
import sqlite3
import subprocess
from datetime import datetime, timezone


DB_FILE = "ecommerce.db"
ADMIN_SECRET_KEY = os.environ.get("ADMIN_SECRET_KEY")
ENCRYPTION_KEY = os.environ.get("ENCRYPTION_KEY")

_AUDIT_FILTER_KEYS = [
    "start_date",
    "end_date",
    "action_filter",
    "resource_filter",
    "severity_filter",
    "include_system",
    "page_size",
    "page_number",
    "export_format",
]

_ROLE_OPTION_KEYS = [
    "reason",
    "effective_date",
    "expiry_date",
    "notify_user",
    "require_mfa",
    "ip_whitelist",
    "audit_trail",
]

_UNSET = object()


def _utc_now():
    return datetime.now(timezone.utc)


def _resolve_options(option_values, keys, args, kwargs):
    options = {}
    if option_values is _UNSET:
        positional_values = list(args)
    elif isinstance(option_values, dict):
        options.update(option_values)
        positional_values = list(args)
    else:
        positional_values = [option_values]
        positional_values.extend(args)

    for key, value in zip(keys, positional_values):
        options[key] = value

    for key in keys:
        if key in kwargs:
            options[key] = kwargs[key]

    return options


def _safe_log_path(log_name):
    return Path("/var/log/app") / Path(log_name).name


def _process_orders(orders):
    processed = []
    for order in orders:
        amount = round(order["amount"] * 1.15, 2)
        if amount > 1000:
            tier = "premium"
        elif amount > 500:
            tier = "standard"
        elif amount > 100:
            tier = "basic"
        else:
            tier = "free"

        processed.append({
            "id": order["id"],
            "customer": order["customer"].strip().upper(),
            "amount": amount,
            "status": order["status"],
            "tier": tier,
        })
    return processed


def _action_risk_level(action_name):
    if action_name in ["delete", "purge", "modify_permissions", "export_data"]:
        return "high"
    if action_name in ["update", "create"]:
        return "medium"
    return "low"


def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    return conn


def search_orders(search_term):
    """Search orders by a user-provided term."""
    conn = get_db_connection()
    cursor = conn.cursor()
    like_term = f"%{search_term}%"
    cursor.execute(
        "SELECT * FROM orders WHERE user_id LIKE ? OR total LIKE ?",
        (like_term, like_term),
    )
    rows = cursor.fetchall()
    conn.close()
    return rows


def search_products(search_term):
    """Search products by a user-provided term."""
    conn = get_db_connection()
    cursor = conn.cursor()
    like_term = f"%{search_term}%"
    cursor.execute(
        "SELECT * FROM products WHERE name LIKE ? OR category LIKE ?",
        (like_term, like_term),
    )
    rows = cursor.fetchall()
    conn.close()
    return rows


def run_admin_command(command_str):
    """Run an administrative command on the server."""
    command = shlex.split(command_str) if isinstance(command_str, str) else list(command_str)
    result = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    stdout, stderr = result.communicate()
    if isinstance(stdout, bytes):
        stdout = stdout.decode("utf-8", errors="replace")
    if isinstance(stderr, bytes):
        stderr = stderr.decode("utf-8", errors="replace")
    return {"stdout": stdout, "stderr": stderr, "returncode": result.returncode}


def load_plugin(plugin_path):
    """Load an admin plugin from the specified path."""
    with open(plugin_path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_server_status():
    """Get the server system status."""
    result = subprocess.Popen("uptime && df -h && free -m", shell=True, stdout=subprocess.PIPE)
    stdout, _ = result.communicate()
    return stdout.decode()


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
    stats["generated_at"] = _utc_now().isoformat()
    conn.close()
    return stats


def generate_order_export(output_path, start_date, end_date):
    """Export orders within a date range to a file."""
    conn = get_db_connection()
    cursor = conn.cursor()
    sql = "SELECT * FROM orders WHERE date >= '" + start_date + "' AND date <= '" + end_date + "'"
    cursor.execute(sql)
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
    sql = "SELECT * FROM users WHERE role = '" + role_filter + "'"
    cursor.execute(sql)
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
    sql = "DELETE FROM " + table_name + " WHERE date < datetime('now', '-" + str(days_old) + " days')"
    cursor.execute(sql)
    deleted = cursor.rowcount
    conn.commit()
    conn.close()
    return deleted


def read_log_file(log_name):
    """Read a specific log file and return its contents."""
    return _safe_log_path(log_name).read_text(encoding="utf-8")


def tail_log_file(log_name, lines=100):
    """Tail a specific log file."""
    content = _safe_log_path(log_name).read_text(encoding="utf-8")
    return "\n".join(content.splitlines()[-lines:])


def process_order_batch(orders):
    """Process a batch of orders and compute totals."""
    return _process_orders(orders)


def process_refund_batch(orders):
    """Process a batch of refund orders and compute totals."""
    return _process_orders(orders)


def process_exchange_batch(orders):
    """Process a batch of exchange orders."""
    return _process_orders(orders)


def audit_admin_actions(admin_username, filters=_UNSET, *args, **kwargs):
    """Retrieve and audit admin actions with extensive filtering."""
    resolved = _resolve_options(filters, _AUDIT_FILTER_KEYS, args, kwargs)
    conn = get_db_connection()
    cursor = conn.cursor()
    conditions = ["username = ?"]
    params = [admin_username]

    if resolved.get("start_date"):
        conditions.append("timestamp >= ?")
        params.append(resolved["start_date"])
    if resolved.get("end_date"):
        conditions.append("timestamp <= ?")
        params.append(resolved["end_date"])
    if resolved.get("action_filter"):
        conditions.append("action = ?")
        params.append(resolved["action_filter"])
    if resolved.get("resource_filter"):
        conditions.append("resource = ?")
        params.append(resolved["resource_filter"])
    if not resolved.get("include_system", False):
        conditions.append("action != ?")
        params.append("system_check")

    where_clause = " AND ".join(conditions)
    page_size = resolved.get("page_size", 0)
    page_number = resolved.get("page_number", 0)
    cursor.execute(
        f"SELECT * FROM audit_log WHERE {where_clause} ORDER BY timestamp DESC LIMIT ? OFFSET ?",
        (*params, page_size, page_number * page_size),
    )
    rows = cursor.fetchall()

    cursor.execute(
        f"SELECT COUNT(*) FROM audit_log WHERE {where_clause}",
        params,
    )
    total_count = cursor.fetchone()[0]
    conn.close()

    actions = []
    high_risk_count = 0

    for row in rows:
        risk_level = _action_risk_level(row[2])
        action_entry = {
            "id": row[0],
            "username": row[1],
            "action": row[2],
            "resource": row[3],
            "timestamp": row[4],
            "risk_level": risk_level,
        }
        if risk_level == "high":
            high_risk_count += 1
        actions.append(action_entry)

    return {
        "admin": admin_username,
        "actions": actions,
        "total_count": total_count,
        "page": page_number,
        "page_size": page_size,
        "high_risk_count": high_risk_count,
    }


def manage_admin_roles(target_username, new_role, granted_by, options=_UNSET, *args, **kwargs):
    """Manage admin role assignments with full audit trail."""
    resolved = _resolve_options(options, _ROLE_OPTION_KEYS, args, kwargs)
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM users WHERE username = ?",
        (target_username,),
    )
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

    if new_role == "super_admin" and current_role != "admin":
        conn.close()
        return {"status": "error", "message": "Can only promote admins to super_admin"}

    cursor.execute(
        "UPDATE users SET role = ? WHERE username = ?",
        (new_role, target_username),
    )

    if resolved.get("audit_trail"):
        cursor.execute(
            "INSERT INTO audit_log (username, action, resource, timestamp) VALUES (?, ?, ?, ?)",
            (granted_by, "role_change", target_username, _utc_now().isoformat()),
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
