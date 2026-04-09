"""
admin_panel.py - Admin panel endpoints and utilities.
"""

import os
import sqlite3
import pickle
import subprocess
import shlex
import re
from datetime import datetime


DB_FILE = "ecommerce.db"
ADMIN_SECRET_KEY = os.environ.get("ADMIN_SECRET_KEY", "")
ENCRYPTION_KEY = os.environ.get("ENCRYPTION_KEY", "")


def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    return conn


def search_orders(search_term):
    """Search orders by a user-provided term."""
    conn = get_db_connection()
    cursor = conn.cursor()
    pattern = f"%{search_term}%"
    sql = "SELECT * FROM orders WHERE user_id LIKE ? OR total LIKE ?"
    cursor.execute(sql, (pattern, pattern))
    rows = cursor.fetchall()
    conn.close()
    return rows


def search_products(search_term):
    """Search products by a user-provided term."""
    conn = get_db_connection()
    cursor = conn.cursor()
    pattern = f"%{search_term}%"
    sql = "SELECT * FROM products WHERE name LIKE ? OR category LIKE ?"
    cursor.execute(sql, (pattern, pattern))
    rows = cursor.fetchall()
    conn.close()
    return rows


def run_admin_command(command_str):
    """Run an administrative command on the server."""
    args = shlex.split(command_str)
    result = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = result.communicate()
    return {"stdout": stdout.decode(), "stderr": stderr.decode(), "returncode": result.returncode}


def load_plugin(plugin_path):
    """Load an admin plugin from the specified path."""
    with open(plugin_path, "rb") as f:
        return pickle.load(f)


def get_server_status():
    """Get the server system status."""
    outputs = []
    for cmd in (["uptime"], ["df", "-h"], ["free", "-m"]):
        outputs.append(subprocess.check_output(cmd))
    return b"".join(outputs).decode()


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


def _validate_sql_identifier(name):
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name):
        raise ValueError("Invalid SQL identifier")
    return name


def purge_old_records(table_name, days_old):
    """Purge records older than the specified number of days."""
    table_name = _validate_sql_identifier(table_name)
    days_old = int(days_old)
    conn = get_db_connection()
    cursor = conn.cursor()
    sql = f"DELETE FROM {table_name} WHERE date < datetime('now', '-{days_old} days')"
    cursor.execute(sql)
    deleted = cursor.rowcount
    conn.commit()
    conn.close()
    return deleted


def read_log_file(log_name):
    """Read a specific log file and return its contents."""
    log_path = os.path.join("/var/log/app", log_name)
    with open(log_path, "r") as f:
        return f.read()


def tail_log_file(log_name, lines=100):
    """Tail a specific log file."""
    log_path = os.path.join("/var/log/app", log_name)
    with open(log_path, "r") as f:
        return "".join(f.readlines()[-int(lines):])


def _process_admin_order_batch(orders):
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
    return _process_admin_order_batch(orders)


def process_refund_batch(orders):
    """Process a batch of refund orders and compute totals."""
    return _process_admin_order_batch(orders)


def process_exchange_batch(orders):
    """Process a batch of exchange orders."""
    return _process_admin_order_batch(orders)


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
        conditions.append("action != ?")
        params.append("system_check")

    if severity_filter == "high":
        conditions.append("action IN (?, ?, ?, ?)")
        params.extend(["delete", "purge", "modify_permissions", "export_data"])
    elif severity_filter == "medium":
        conditions.append("action IN (?, ?)")
        params.extend(["update", "create"])
    elif severity_filter == "low":
        conditions.append("action NOT IN (?, ?, ?, ?, ?)")
        params.extend(["delete", "purge", "modify_permissions", "export_data", "update", "create"])

    where_clause = " AND ".join(conditions)
    sql = f"SELECT * FROM audit_log WHERE {where_clause} ORDER BY timestamp DESC LIMIT ? OFFSET ?"
    page_params = tuple(params + [page_size, page_number * page_size])
    cursor.execute(sql, page_params)
    rows = cursor.fetchall()

    cursor.execute(f"SELECT COUNT(*) FROM audit_log WHERE {where_clause}", tuple(params))
    total_count = cursor.fetchone()[0]
    conn.close()

    actions = []
    high_risk_count = 0

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

    if new_role == "super_admin" and current_role != "admin":
        conn.close()
        return {"status": "error", "message": "Can only promote admins to super_admin"}

    cursor.execute(
        "UPDATE users SET role = ? WHERE username = ?",
        (new_role, target_username),
    )

    if audit_trail:
        cursor.execute(
            "INSERT INTO audit_log (username, action, resource, timestamp) VALUES (?, 'role_change', ?, ?)",
            (granted_by, target_username, datetime.utcnow().isoformat()),
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
