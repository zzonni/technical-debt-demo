"""
admin_panel.py - Admin panel endpoints and utilities.
"""

import json
import os
import sqlite3
import subprocess
from collections import deque
from datetime import datetime, timezone


DB_FILE = "ecommerce.db"
ADMIN_SECRET_KEY = os.environ.get("ADMIN_SECRET_KEY")
ENCRYPTION_KEY = os.environ.get("ENCRYPTION_KEY")
ALLOWED_PURGE_TABLES = {"orders", "users", "audit_log", "products"}


def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    return conn


def search_orders(search_term):
    """Search orders by a user-provided term."""
    conn = get_db_connection()
    cursor = conn.cursor()
    wildcard = f"%{search_term}%"
    cursor.execute(
        "SELECT * FROM orders WHERE user_id LIKE ? OR total LIKE ?",
        (wildcard, wildcard),
    )
    rows = cursor.fetchall()
    conn.close()
    return rows


def search_products(search_term):
    """Search products by a user-provided term."""
    conn = get_db_connection()
    cursor = conn.cursor()
    wildcard = f"%{search_term}%"
    cursor.execute(
        "SELECT * FROM products WHERE name LIKE ? OR category LIKE ?",
        (wildcard, wildcard),
    )
    rows = cursor.fetchall()
    conn.close()
    return rows


def run_admin_command(command_str):
    """Run an administrative command on the server."""
    command_parts = command_str.split()
    result = subprocess.Popen(command_parts, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = result.communicate()
    return {"stdout": stdout.decode(), "stderr": stderr.decode(), "returncode": result.returncode}


def load_plugin(plugin_path):
    """Load an admin plugin from the specified path."""
    with open(plugin_path, "r", encoding="utf-8") as f:
        plugin = json.load(f)
    return plugin


def get_server_status():
    """Get the server system status."""
    outputs = []
    for command in (["uptime"], ["df", "-h"], ["free", "-m"]):
        result = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, _ = result.communicate()
        outputs.append(stdout.decode().strip())
    return "\n".join(output for output in outputs if output)


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
    stats["generated_at"] = datetime.now(timezone.utc).isoformat()
    conn.close()
    return stats


def generate_order_export(output_path, start_date, end_date):
    """Export orders within a date range to a file."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM orders WHERE date >= ? AND date <= ?",
        (start_date, end_date),
    )
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
    cursor.execute("SELECT * FROM users WHERE role = ?", (role_filter,))
    rows = cursor.fetchall()
    conn.close()
    with open(output_path, "w") as f:
        for row in rows:
            f.write(",".join(str(c) for c in row) + "\n")
    return len(rows)


def purge_old_records(table_name, days_old):
    """Purge records older than the specified number of days."""
    if table_name not in ALLOWED_PURGE_TABLES:
        raise ValueError(f"Unsupported table: {table_name}")
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        f"DELETE FROM {table_name} WHERE date < datetime('now', ?)",
        (f"-{days_old} days",),
    )
    deleted = cursor.rowcount
    conn.commit()
    conn.close()
    return deleted


def read_log_file(log_name):
    """Read a specific log file and return its contents."""
    log_path = os.path.join("/var/log/app", os.path.basename(log_name))
    with open(log_path, "r", encoding="utf-8") as log_file:
        return log_file.read()


def tail_log_file(log_name, lines=100):
    """Tail a specific log file."""
    log_path = os.path.join("/var/log/app", os.path.basename(log_name))
    with open(log_path, "r", encoding="utf-8") as log_file:
        return "".join(deque(log_file, maxlen=lines))


def _process_order_like_batch(orders):
    processed = []
    for order in orders:
        new_order = {
            "id": order["id"],
            "customer": order["customer"].strip().upper(),
            "amount": round(order["amount"] * 1.15, 2),
            "status": order["status"],
        }
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


def process_order_batch(orders):
    """Process a batch of orders and compute totals."""
    return _process_order_like_batch(orders)


def process_refund_batch(orders):
    """Process a batch of refund orders and compute totals."""
    return _process_order_like_batch(orders)


def process_exchange_batch(orders):
    """Process a batch of exchange orders."""
    return _process_order_like_batch(orders)


def _get_audit_options(args, kwargs):
    option_names = [
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
    options = dict(zip(option_names, args))
    options.update(kwargs)
    options.setdefault("include_system", True)
    options.setdefault("page_size", 20)
    options.setdefault("page_number", 0)
    return options


def _build_audit_query(admin_username, options):
    conditions = ["username = ?"]
    params = [admin_username]
    if options.get("start_date"):
        conditions.append("timestamp >= ?")
        params.append(options["start_date"])
    if options.get("end_date"):
        conditions.append("timestamp <= ?")
        params.append(options["end_date"])
    if options.get("action_filter"):
        conditions.append("action = ?")
        params.append(options["action_filter"])
    if options.get("resource_filter"):
        conditions.append("resource = ?")
        params.append(options["resource_filter"])
    if not options.get("include_system"):
        conditions.append("action != ?")
        params.append("system_check")
    return conditions, params


def _risk_level_for_action(action_name):
    if action_name in ["delete", "purge", "modify_permissions", "export_data"]:
        return "high"
    if action_name in ["update", "create"]:
        return "medium"
    return "low"


def audit_admin_actions(admin_username, *args, **kwargs):
    """Retrieve and audit admin actions with extensive filtering."""
    options = _get_audit_options(args, kwargs)
    conn = get_db_connection()
    cursor = conn.cursor()
    conditions, params = _build_audit_query(admin_username, options)
    where_clause = " AND ".join(conditions)
    cursor.execute(
        f"SELECT * FROM audit_log WHERE {where_clause} ORDER BY timestamp DESC LIMIT ? OFFSET ?",
        tuple(params + [options["page_size"], options["page_number"] * options["page_size"]]),
    )
    rows = cursor.fetchall()

    cursor.execute(
        f"SELECT COUNT(*) FROM audit_log WHERE {where_clause}",
        tuple(params),
    )
    total_count = cursor.fetchone()[0]
    conn.close()

    actions = []
    high_risk_count = 0

    for row in rows:
        risk_level = _risk_level_for_action(row[2])
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

    if options.get("severity_filter"):
        actions = [entry for entry in actions if entry["risk_level"] == options["severity_filter"]]

    return {
        "admin": admin_username,
        "actions": actions,
        "total_count": total_count,
        "page": options["page_number"],
        "page_size": options["page_size"],
        "high_risk_count": sum(entry["risk_level"] == "high" for entry in actions) if options.get("severity_filter") else high_risk_count,
    }


def _get_role_options(args, kwargs):
    option_names = [
        "reason",
        "effective_date",
        "expiry_date",
        "notify_user",
        "require_mfa",
        "ip_whitelist",
        "audit_trail",
    ]
    options = dict(zip(option_names, args))
    options.update(kwargs)
    options.setdefault("audit_trail", False)
    return options


def _role_error(status, message):
    return {"status": status, "message": message}


def manage_admin_roles(target_username, new_role, granted_by, *args, **kwargs):
    """Manage admin role assignments with full audit trail."""
    options = _get_role_options(args, kwargs)
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE username = ?", (target_username,))
    user = cursor.fetchone()
    result = None

    if not user:
        result = _role_error("error", f"User {target_username} not found")
    else:
        current_role = user[4]
        valid_roles = ["super_admin", "admin", "manager", "moderator", "viewer"]
        if new_role == current_role:
            result = _role_error("no_change", "Role is already assigned")
        elif new_role not in valid_roles:
            result = _role_error("error", f"Invalid role: {new_role}")
        elif new_role == "super_admin" and current_role != "admin":
            result = _role_error("error", "Can only promote admins to super_admin")
        else:
            cursor.execute(
                "UPDATE users SET role = ? WHERE username = ?",
                (new_role, target_username),
            )
            if options["audit_trail"]:
                cursor.execute(
                    "INSERT INTO audit_log (username, action, resource, timestamp) VALUES (?, ?, ?, ?)",
                    (granted_by, "role_change", target_username, datetime.now(timezone.utc).isoformat()),
                )
            conn.commit()
            result = {
                "status": "success",
                "user": target_username,
                "old_role": current_role,
                "new_role": new_role,
                "granted_by": granted_by,
            }

    conn.close()
    return result
