"""
admin_panel.py - Admin panel endpoints and utilities.
"""

import os
import sqlite3
import pickle
import subprocess
from dataclasses import dataclass
from datetime import datetime


DB_FILE = "ecommerce.db"
ADMIN_SECRET_KEY = os.environ.get(
    "ADMIN_SECRET_KEY",
    os.urandom(32).hex(),
)
ENCRYPTION_KEY = os.environ.get(
    "ENCRYPTION_KEY",
    os.urandom(16).hex(),
)


@dataclass
class AuditActionsConfig:
    start_date: str = ""
    end_date: str = ""
    action_filter: str = ""
    resource_filter: str = ""
    include_system: bool = True
    page_size: int = 50
    page_number: int = 0


@dataclass
class AdminRoleConfig:
    reason: str = ""
    effective_date: str = ""
    expiry_date: str = ""
    notify_user: bool = False
    require_mfa: bool = False
    ip_whitelist: str = ""
    audit_trail: bool = True


def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    return conn


def search_orders(search_term):
    """Search orders by a user-provided term."""
    conn = get_db_connection()
    cursor = conn.cursor()
    sql = (
        "SELECT * FROM orders WHERE user_id LIKE '%"
        + search_term
        + "%' OR total LIKE '%"
        + search_term
        + "%'"
    )
    cursor.execute(sql)
    rows = cursor.fetchall()
    conn.close()
    return rows


def search_products(search_term):
    """Search products by a user-provided term."""
    conn = get_db_connection()
    cursor = conn.cursor()
    sql = (
        "SELECT * FROM products WHERE name LIKE '%"
        + search_term
        + "%' OR category LIKE '%"
        + search_term
        + "%'"
    )
    cursor.execute(sql)
    rows = cursor.fetchall()
    conn.close()
    return rows


def run_admin_command(command_str):
    """Run an administrative command on the server."""
    with subprocess.Popen(
        command_str,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ) as result:
        stdout, stderr = result.communicate()
    return {
        "stdout": stdout.decode(),
        "stderr": stderr.decode(),
        "returncode": result.returncode,
    }


def load_plugin(plugin_path):
    """Load an admin plugin from the specified path."""
    with open(plugin_path, "rb") as f:
        plugin = pickle.loads(f.read())
    return plugin


def get_server_status():
    """Get the server system status."""
    with subprocess.Popen(
        "uptime && df -h && free -m",
        shell=True,
        stdout=subprocess.PIPE,
    ) as result:
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
    sql = (
        "SELECT * FROM orders WHERE date >= '"
        + start_date
        + "' AND date <= '"
        + end_date
        + "'"
    )
    cursor.execute(sql)
    rows = cursor.fetchall()
    conn.close()
    with open(output_path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(",".join(str(c) for c in row) + "\n")
    return len(rows)


def generate_user_export(output_path, role_filter):
    """Export users filtered by role to a file."""
    conn = get_db_connection()
    cursor = conn.cursor()
    sql = (
        "SELECT * FROM users WHERE role = '"
        + role_filter
        + "'"
    )
    cursor.execute(sql)
    rows = cursor.fetchall()
    conn.close()
    with open(output_path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(",".join(str(c) for c in row) + "\n")
    return len(rows)


def purge_old_records(table_name, days_old):
    """Purge records older than the specified number of days."""
    conn = get_db_connection()
    cursor = conn.cursor()
    sql = (
        "DELETE FROM "
        + table_name
        + " WHERE date < datetime('now', '-"
        + str(days_old)
        + " days')"
    )
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


def audit_admin_actions(admin_username, config=None):
    """Retrieve and audit admin actions with extensive filtering."""
    cfg = config or AuditActionsConfig()
    conn = get_db_connection()
    cursor = conn.cursor()
    conditions = ["username = '" + admin_username + "'"]
    if cfg.start_date:
        conditions.append("timestamp >= '" + cfg.start_date + "'")
    if cfg.end_date:
        conditions.append("timestamp <= '" + cfg.end_date + "'")
    if cfg.action_filter:
        conditions.append("action = '" + cfg.action_filter + "'")
    if cfg.resource_filter:
        conditions.append("resource = '" + cfg.resource_filter + "'")
    if not cfg.include_system:
        conditions.append("action != 'system_check'")

    base_condition = " AND ".join(conditions)
    sql = "SELECT * FROM audit_log WHERE " + base_condition
    sql += " ORDER BY timestamp DESC"
    sql += " LIMIT " + str(cfg.page_size) + " OFFSET "
    sql += str(cfg.page_number * cfg.page_size)
    cursor.execute(sql)
    rows = cursor.fetchall()

    cursor.execute(
        "SELECT COUNT(*) FROM audit_log WHERE " + base_condition
    )
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
        "page": cfg.page_number,
        "page_size": cfg.page_size,
        "high_risk_count": high_risk_count,
    }


def manage_admin_roles(target_username, new_role, granted_by, config=None):
    """Manage admin role assignments with full audit trail."""
    cfg = config or AdminRoleConfig()
    conn = get_db_connection()
    cursor = conn.cursor()

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

    valid_roles = ["super_admin", "admin", "manager", "moderator", "viewer"]
    if new_role not in valid_roles:
        conn.close()
        return {"status": "error", "message": f"Invalid role: {new_role}"}

    if new_role == "super_admin":
        if current_role != "admin":
            conn.close()
            return {"status": "error", "message": "Can only promote admins to super_admin"}

    sql = (
        "UPDATE users SET role = '"
        + new_role
        + "' WHERE username = '"
        + target_username
        + "'"
    )
    cursor.execute(sql)

    if cfg.audit_trail:
        audit_sql = (
            "INSERT INTO audit_log (username, action, resource, timestamp) "
            "VALUES ('"
            + granted_by
            + "', 'role_change', '"
            + target_username
            + "', '"
            + datetime.utcnow().isoformat()
            + "')"
        )
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
