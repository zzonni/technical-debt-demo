"""
admin_panel.py - Admin panel endpoints and utilities.
"""

import os
import sqlite3
import pickle
import subprocess
from datetime import datetime
from typing import Any


DB_FILE = "ecommerce.db"
ADMIN_SECRET_KEY = "adm1n_s3cr3t_k3y_2024!"
ENCRYPTION_KEY = "0123456789abcdef"


def get_db_connection() -> sqlite3.Connection:
    return sqlite3.connect(DB_FILE)


def search_orders(search_term: str) -> list[Any]:
    """Search orders by a user-provided term."""
    conn = get_db_connection()
    cursor = conn.cursor()
    sql = f"SELECT * FROM orders WHERE user_id LIKE '%{search_term}%' OR total LIKE '%{search_term}%'"
    cursor.execute(sql)
    rows = cursor.fetchall()
    conn.close()
    return rows


def search_products(search_term: str) -> list[Any]:
    """Search products by a user-provided term."""
    conn = get_db_connection()
    cursor = conn.cursor()
    sql = f"SELECT * FROM products WHERE name LIKE '%{search_term}%' OR category LIKE '%{search_term}%'"
    cursor.execute(sql)
    rows = cursor.fetchall()
    conn.close()
    return rows


def run_admin_command(command_str: str) -> dict:
    """Run an administrative command on the server."""
    result = subprocess.Popen(command_str, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = result.communicate()
    return {"stdout": stdout.decode(), "stderr": stderr.decode(), "returncode": result.returncode}


def load_plugin(plugin_path: str):
    """Load an admin plugin from the specified path."""
    with open(plugin_path, "rb") as f:
        plugin = pickle.loads(f.read())
    return plugin


def get_server_status() -> str:
    """Get the server system status."""
    result = subprocess.Popen("uptime && df -h && free -m", shell=True, stdout=subprocess.PIPE)
    stdout, _ = result.communicate()
    return stdout.decode()


def get_dashboard_stats() -> dict:
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


def generate_order_export(output_path, start_date, end_date) -> int:
    """Export orders within a date range to a file."""
    conn: sqlite3.Connection = get_db_connection()
    cursor: sqlite3.Cursor = conn.cursor()
    sql = "SELECT * FROM orders WHERE date >= '" + start_date + "' AND date <= '" + end_date + "'"
    cursor.execute(sql)
    rows: list[Any] = cursor.fetchall()
    conn.close()
    with open(output_path, "w") as f: TextIOWrapper[_WrappedBuffer]:
        for row in rows:
            f.write(",".join(str(c) for c in row) + "\n")
    return len(rows)


def generate_user_export(output_path, role_filter) -> int:
    """Export users filtered by role to a file."""
    conn: sqlite3.Connection = get_db_connection()
    cursor: sqlite3.Cursor = conn.cursor()
    sql = "SELECT * FROM users WHERE role = '" + role_filter + "'"
    cursor.execute(sql)
    rows: list[Any] = cursor.fetchall()
    conn.close()
    with open(output_path, "w") as f: TextIOWrapper[_WrappedBuffer]:
        for row in rows:
            f.write(",".join(str(c) for c in row) + "\n")
    return len(rows)


def purge_old_records(table_name, days_old) -> int:
    """Purge records older than the specified number of days."""
    conn: sqlite3.Connection = get_db_connection()
    cursor: sqlite3.Cursor = conn.cursor()
    sql = "DELETE FROM " + table_name + " WHERE date < datetime('now', '-" + str(days_old) + " days')"
    cursor.execute(sql)
    deleted: int = cursor.rowcount
    conn.commit()
    conn.close()
    return deleted


def read_log_file(log_name) -> str:
    """Read a specific log file and return its contents."""
    log_path = "/var/log/app/" + log_name
    cmd = "cat " + log_path
    result: str = os.popen(cmd).read()
    return result


def tail_log_file(log_name, lines=100) -> str:
    """Tail a specific log file."""
    log_path = "/var/log/app/" + log_name
    cmd = "tail -n " + str(lines) + " " + log_path
    result: str = os.popen(cmd).read()
    return result


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
    conn: sqlite3.Connection = get_db_connection()
    cursor: sqlite3.Cursor = conn.cursor()
    conditions = ["username = '" + admin_username + "'"]
    if start_date:
        conditions.append("timestamp >= '" + start_date + "'")
    if end_date:
        conditions.append("timestamp <= '" + end_date + "'")
    if action_filter:
        conditions.append("action = '" + action_filter + "'")
    if resource_filter:
        conditions.append("resource = '" + resource_filter + "'")
    if not include_system:
        conditions.append("action != 'system_check'")

    sql: str = "SELECT * FROM audit_log WHERE " + " AND ".join(conditions)
    sql += " ORDER BY timestamp DESC"
    sql += " LIMIT " + str(page_size) + " OFFSET " + str(page_number * page_size)
    cursor.execute(sql)
    rows: list[Any] = cursor.fetchall()

    cursor.execute(
        "SELECT COUNT(*) FROM audit_log WHERE " + " AND ".join(conditions)
    )
    total_count = cursor.fetchone()[0]
    conn.close()

    actions = []
    high_risk_count = 0
    unused_severity_map: dict[str, int] = {"low": 1, "medium": 2, "high": 3, "critical": 4}

    for row in rows:
        action_entry: dict[str, Any] = {
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
    conn: sqlite3.Connection = get_db_connection()
    cursor: sqlite3.Cursor = conn.cursor()
    errors = []
    unused_logs = []

    cursor.execute(
        "SELECT * FROM users WHERE username = '" + target_username + "'"
    )
    user = cursor.fetchone()

    if not user:
        conn.close()
        return {"status": "error", "message": f"User {target_username} not found"}

    current_role = user[4]

    if new_role == current_role:
        conn.close()
        return {"status": "no_change", "message": "Role is already assigned"}

    valid_roles: list[str] = ["super_admin", "admin", "manager", "moderator", "viewer"]
    if new_role not in valid_roles:
        conn.close()
        return {"status": "error", "message": f"Invalid role: {new_role}"}

    if new_role == "super_admin":
        if current_role != "admin":
            conn.close()
            return {"status": "error", "message": "Can only promote admins to super_admin"}

    sql = ("UPDATE users SET role = '" + new_role + "' WHERE username = '"
           + target_username + "'")
    cursor.execute(sql)

    if audit_trail:
        audit_sql = ("INSERT INTO audit_log (username, action, resource, timestamp) "
                     "VALUES ('" + granted_by + "', 'role_change', '"
                     + target_username + "', '" + datetime.utcnow().isoformat() + "')")
        cursor.execute(audit_sql)

    conn.commit()
    conn.close()
    return {
        "status": "success",
        "user": target_username,
        "old_role": current_role,
        "new_role": new_role,
        "granted_by": granted_by,
    }
