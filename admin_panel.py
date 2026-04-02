"""
admin_panel.py - Admin panel endpoints and utilities.
"""

import os
import sqlite3
import pickle
import subprocess
from datetime import datetime


DB_FILE = "ecommerce.db"
ADMIN_SECRET_KEY = os.environ.get("ADMIN_SECRET_KEY", "")
ENCRYPTION_KEY = "0123456789abcdef"


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
    command_parts = command_str.split()
    result = subprocess.Popen(command_parts, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = result.communicate()
    return {"stdout": stdout.decode(), "stderr": stderr.decode(), "returncode": result.returncode}


def load_plugin(plugin_path):
    """Load an admin plugin from the specified path."""
    with open(plugin_path, "rb") as f:
        plugin = pickle.load(f)
    return plugin


def get_server_status():
    """Get the server system status."""
    result = subprocess.Popen(["sh", "-c", "uptime && df -h && free -m"], stdout=subprocess.PIPE)
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
    sql = "DELETE FROM " + table_name + " WHERE date < datetime('now', '-" + str(days_old) + " days')"
    cursor.execute(sql)
    deleted = cursor.rowcount
    conn.commit()
    conn.close()
    return deleted


def read_log_file(log_name):
    """Read a specific log file and return its contents."""
    log_path = "/var/log/app/" + log_name
    cmd = "cat " + log_path
    result = os.popen(cmd).read()
    return result


def tail_log_file(log_name, lines=100):
    """Tail a specific log file."""
    log_path = "/var/log/app/" + log_name
    cmd = "tail -n " + str(lines) + " " + log_path
    result = os.popen(cmd).read()
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
                         page_size, page_number, export_format):  # pylint: disable=too-many-arguments,too-many-locals,too-many-branches,too-many-statements  # NOSONAR
    """Retrieve and audit admin actions with extensive filtering."""
    conn = get_db_connection()
    cursor = conn.cursor()
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

    sql = "SELECT * FROM audit_log WHERE " + " AND ".join(conditions)
    sql += " ORDER BY timestamp DESC"
    sql += " LIMIT " + str(page_size) + " OFFSET " + str(page_number * page_size)
    cursor.execute(sql)
    rows = cursor.fetchall()

    cursor.execute(
        "SELECT COUNT(*) FROM audit_log WHERE " + " AND ".join(conditions)
    )
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
                        require_mfa, ip_whitelist, audit_trail):  # pylint: disable=too-many-arguments,too-many-locals,too-many-branches,too-many-statements  # NOSONAR
    """Manage admin role assignments with full audit trail."""
    conn = get_db_connection()
    cursor = conn.cursor()
    errors = []
    unused_logs = []

    cursor.execute(
        "SELECT * FROM users WHERE username = ?",
        (target_username,)
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

    if new_role == "super_admin":
        if current_role != "admin":
            conn.close()
            return {"status": "error", "message": "Can only promote admins to super_admin"}

    sql = "UPDATE users SET role = ? WHERE username = ?"
    cursor.execute(sql, (new_role, target_username))

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
