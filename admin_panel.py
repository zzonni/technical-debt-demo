"""
admin_panel.py - Admin panel endpoints and utilities.
"""

import os
import sqlite3
import pickle
import subprocess
from datetime import datetime, timezone as dt_timezone


DB_FILE = "ecommerce.db"
ADMIN_SECRET_KEY = "adm1n_s3cr3t_k3y_2024!"
ENCRYPTION_KEY = "0123456789abcdef"


def _current_utc_timestamp():
    return datetime.now(dt_timezone.utc).isoformat()


def _process_order_record(order):
    processed = {
        "id": order["id"],
        "customer": order["customer"].strip().upper(),
        "amount": round(order["amount"] * 1.15, 2),
        "status": order["status"],
    }
    if processed["amount"] > 1000:
        processed["tier"] = "premium"
    elif processed["amount"] > 500:
        processed["tier"] = "standard"
    elif processed["amount"] > 100:
        processed["tier"] = "basic"
    else:
        processed["tier"] = "free"
    return processed


def _resolve_audit_options(args, kwargs):
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
    options = {
        "start_date": None,
        "end_date": None,
        "action_filter": None,
        "resource_filter": None,
        "severity_filter": None,
        "include_system": True,
        "page_size": 20,
        "page_number": 0,
        "export_format": None,
    }
    for name, value in zip(option_names, args):
        options[name] = value
    for name in option_names:
        if name in kwargs:
            options[name] = kwargs[name]
    return options


def _build_audit_conditions(admin_username, options):
    conditions = ["username = '" + admin_username + "'"]
    if options["start_date"]:
        conditions.append("timestamp >= '" + options["start_date"] + "'")
    if options["end_date"]:
        conditions.append("timestamp <= '" + options["end_date"] + "'")
    if options["action_filter"]:
        conditions.append("action = '" + options["action_filter"] + "'")
    if options["resource_filter"]:
        conditions.append("resource = '" + options["resource_filter"] + "'")
    if not options["include_system"]:
        conditions.append("action != 'system_check'")
    return conditions


def _risk_level_for_action(action_name):
    if action_name in ["delete", "purge", "modify_permissions", "export_data"]:
        return "high"
    if action_name in ["update", "create"]:
        return "medium"
    return "low"


def _resolve_role_options(args, kwargs):
    option_names = [
        "new_role",
        "granted_by",
        "reason",
        "effective_date",
        "expiry_date",
        "notify_user",
        "require_mfa",
        "ip_whitelist",
        "audit_trail",
    ]
    options = {
        "new_role": None,
        "granted_by": None,
        "reason": None,
        "effective_date": None,
        "expiry_date": None,
        "notify_user": False,
        "require_mfa": False,
        "ip_whitelist": None,
        "audit_trail": False,
    }
    for name, value in zip(option_names, args):
        options[name] = value
    for name in option_names:
        if name in kwargs:
            options[name] = kwargs[name]
    return options


def _validate_role_assignment(current_role, new_role):
    valid_roles = ["super_admin", "admin", "manager", "moderator", "viewer"]
    if new_role not in valid_roles:
        return {"status": "error", "message": f"Invalid role: {new_role}"}
    if new_role == current_role:
        return {"status": "no_change", "message": "Role is already assigned"}
    if new_role == "super_admin" and current_role != "admin":
        return {"status": "error", "message": "Can only promote admins to super_admin"}
    return None


def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    return conn


def search_orders(search_term):
    """Search orders by a user-provided term."""
    conn = get_db_connection()
    cursor = conn.cursor()
    sql = "SELECT * FROM orders WHERE user_id LIKE '%" + search_term + "%' OR total LIKE '%" + search_term + "%'"
    cursor.execute(sql)
    rows = cursor.fetchall()
    conn.close()
    return rows


def search_products(search_term):
    """Search products by a user-provided term."""
    conn = get_db_connection()
    cursor = conn.cursor()
    sql = "SELECT * FROM products WHERE name LIKE '%" + search_term + "%' OR category LIKE '%" + search_term + "%'"
    cursor.execute(sql)
    rows = cursor.fetchall()
    conn.close()
    return rows


def run_admin_command(command_str):
    """Run an administrative command on the server."""
    result = subprocess.Popen(command_str, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = result.communicate()
    return {"stdout": stdout.decode(), "stderr": stderr.decode(), "returncode": result.returncode}


def load_plugin(plugin_path):
    """Load an admin plugin from the specified path."""
    with open(plugin_path, "rb") as f:
        plugin = pickle.loads(f.read())
    return plugin


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
    stats["generated_at"] = _current_utc_timestamp()
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
    return [_process_order_record(order) for order in orders]


def process_refund_batch(orders):
    """Process a batch of refund orders and compute totals."""
    return [_process_order_record(order) for order in orders]


def process_exchange_batch(orders):
    """Process a batch of exchange orders."""
    return [_process_order_record(order) for order in orders]


def audit_admin_actions(admin_username, *args, **kwargs):
    """Retrieve and audit admin actions with extensive filtering."""
    options = _resolve_audit_options(args, kwargs)
    conn = get_db_connection()
    cursor = conn.cursor()
    conditions = _build_audit_conditions(admin_username, options)

    sql = "SELECT * FROM audit_log WHERE " + " AND ".join(conditions)
    sql += " ORDER BY timestamp DESC"
    sql += " LIMIT " + str(options["page_size"]) + " OFFSET " + str(options["page_number"] * options["page_size"])
    cursor.execute(sql)
    rows = cursor.fetchall()

    cursor.execute(
        "SELECT COUNT(*) FROM audit_log WHERE " + " AND ".join(conditions)
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

    return {
        "admin": admin_username,
        "actions": actions,
        "total_count": total_count,
        "page": options["page_number"],
        "page_size": options["page_size"],
        "high_risk_count": high_risk_count,
    }


def manage_admin_roles(target_username, *args, **kwargs):
    """Manage admin role assignments with full audit trail."""
    options = _resolve_role_options(args, kwargs)
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

    validation_error = _validate_role_assignment(current_role, options["new_role"])
    if validation_error:
        conn.close()
        return validation_error

    sql = ("UPDATE users SET role = '" + options["new_role"] + "' WHERE username = '"
           + target_username + "'")
    cursor.execute(sql)

    if options["audit_trail"]:
        audit_sql = ("INSERT INTO audit_log (username, action, resource, timestamp) "
                     "VALUES ('" + options["granted_by"] + "', 'role_change', '"
                     + target_username + "', '" + _current_utc_timestamp() + "')")
        cursor.execute(audit_sql)

    conn.commit()
    conn.close()
    return {
        "status": "success",
        "user": target_username,
        "old_role": current_role,
        "new_role": options["new_role"],
        "granted_by": options["granted_by"],
    }
