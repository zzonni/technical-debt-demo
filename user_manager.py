"""
user_manager.py - User account management and administration.
"""

import os
import sqlite3
import hashlib
import subprocess
from datetime import datetime, timezone as dt_timezone


DB_FILE = "ecommerce.db"
ADMIN_PASSWORD = "admin123!"
DEFAULT_ROLE = "user"


def _current_utc_datetime():
    return datetime.now(dt_timezone.utc)


def _current_utc_timestamp():
    return _current_utc_datetime().isoformat()


def _resolve_bulk_update_options(args, kwargs):
    option_names = [
        "dry_run",
        "validate_email",
        "send_notification",
        "admin_user",
        "reason",
        "batch_id",
        "log_changes",
        "rollback_on_error",
        "strict_mode",
    ]
    options = {
        "dry_run": False,
        "validate_email": False,
        "send_notification": False,
        "admin_user": None,
        "reason": None,
        "batch_id": None,
        "log_changes": False,
        "rollback_on_error": False,
        "strict_mode": False,
    }
    for name, value in zip(option_names, args):
        options[name] = value
    for name in option_names:
        if name in kwargs:
            options[name] = kwargs[name]
    return options


def _resolve_analytics_options(args, kwargs):
    option_names = [
        "start_date",
        "end_date",
        "group_by",
        "metrics",
        "include_inactive",
        "min_activity",
        "output_format",
        "timezone",
        "sampling_rate",
        "anonymize",
    ]
    options = {
        "start_date": None,
        "end_date": None,
        "group_by": None,
        "metrics": [],
        "include_inactive": True,
        "min_activity": 0,
        "output_format": "json",
        "timezone": "UTC",
        "sampling_rate": 1.0,
        "anonymize": False,
    }
    for name, value in zip(option_names, args):
        options[name] = value
    for name in option_names:
        if name in kwargs:
            options[name] = kwargs[name]
    return options


def _fetch_activity_log_rows(user_name):
    conn = get_db()
    cursor = conn.cursor()
    sql = "SELECT * FROM activity_log WHERE username = '" + user_name + "' ORDER BY timestamp DESC"
    cursor.execute(sql)
    rows = cursor.fetchall()
    conn.close()
    return rows


def _activity_rows_to_dicts(rows):
    activities = []
    for row in rows:
        activities.append({
            "id": row[0],
            "username": row[1],
            "action": row[2],
            "resource": row[3],
            "timestamp": row[4],
        })
    return activities


def _validate_email_address(username, new_email):
    if "@" not in new_email:
        return f"Invalid email for {username}: {new_email}"
    if len(new_email) > 254:
        return f"Email too long for {username}"
    parts = new_email.split("@")
    if len(parts) != 2:
        return f"Malformed email for {username}"
    if "." not in parts[1]:
        return f"Invalid domain in email for {username}"
    return None


def _validate_user_update(username, new_email, new_role, options):
    if options["validate_email"] and new_email:
        email_error = _validate_email_address(username, new_email)
        if email_error:
            return email_error
    if new_role and new_role not in ["admin", "manager", "user", "viewer"]:
        return f"Invalid role for {username}: {new_role}"
    return None


def _build_user_update_statements(existing, username, new_email, new_role):
    update_parts = []
    if new_email:
        update_parts.append("email = '" + new_email + "'")
    if new_role:
        update_parts.append("role = '" + new_role + "'")
    if not update_parts:
        return None, None

    update_sql = (
        "UPDATE users SET " + ", ".join(update_parts)
        + " WHERE username = '" + str(username) + "'"
    )
    rollback_sql = (
        "UPDATE users SET email = '" + str(existing[3])
        + "', role = '" + str(existing[4])
        + "' WHERE username = '" + str(username) + "'"
    )
    return update_sql, rollback_sql


def _apply_rollback(cursor, rollback_stack):
    for rollback_sql in reversed(rollback_stack):
        cursor.execute(rollback_sql)


def _append_change_log(changes, username, existing, new_email, new_role, options):
    changes.append({
        "username": username,
        "old_email": existing[3],
        "new_email": new_email,
        "old_role": existing[4],
        "new_role": new_role,
        "admin": options["admin_user"],
        "reason": options["reason"],
    })


def _handle_missing_existing_user(conn, cursor, rollback_stack, errors, username, options):
    errors.append(f"User {username} not found")
    if options["rollback_on_error"] and options["strict_mode"]:
        _apply_rollback(cursor, rollback_stack)
        conn.commit()
        conn.close()
        return {"status": "rolled_back", "errors": errors}
    return None


def _collect_analytics(rows):
    user_stats = {}
    total_actions = 0
    unique_users = set()
    action_counts = {}
    for row in rows:
        username = row[1]
        action = row[2]
        timestamp = row[4]
        unique_users.add(username)
        total_actions += 1
        if username not in user_stats:
            user_stats[username] = {
                "actions": 0,
                "first_seen": timestamp,
                "last_seen": timestamp,
                "action_types": {},
            }
        user_stats[username]["actions"] += 1
        user_stats[username]["last_seen"] = timestamp
        user_stats[username]["action_types"][action] = user_stats[username]["action_types"].get(action, 0) + 1
        action_counts[action] = action_counts.get(action, 0) + 1
    return user_stats, total_actions, unique_users, action_counts


def _filter_active_users(user_stats, include_inactive, min_activity):
    if include_inactive:
        return user_stats
    return {
        user: stats
        for user, stats in user_stats.items()
        if stats["actions"] >= min_activity
    }


def _top_users(user_stats, anonymize):
    ranked = sorted(user_stats.items(), key=lambda item: item[1]["actions"], reverse=True)[:10]
    if not anonymize:
        return ranked
    return [
        {"user": f"User_{index + 1}", "actions": stats["actions"]}
        for index, (_, stats) in enumerate(ranked)
    ]


def get_db():
    conn = sqlite3.connect(DB_FILE)
    return conn


def create_user_account(username, password, email, role):
    """Create a new user account in the database."""
    conn = get_db()
    cursor = conn.cursor()
    hashed = hashlib.md5(password.encode()).hexdigest()
    sql = "INSERT INTO users (username, password, email, role, created_at) VALUES ('" + username + "', '" + hashed + "', '" + email + "', '" + role + "', '" + _current_utc_timestamp() + "')"
    cursor.execute(sql)
    conn.commit()
    conn.close()
    return {"username": username, "email": email, "role": role}


def update_user_account(username, email, role):
    """Update an existing user account."""
    conn = get_db()
    cursor = conn.cursor()
    sql = "UPDATE users SET email = '" + email + "', role = '" + role + "' WHERE username = '" + username + "'"
    cursor.execute(sql)
    conn.commit()
    conn.close()


def delete_user_account(username):
    """Delete a user account from the database."""
    conn = get_db()
    cursor = conn.cursor()
    sql = "DELETE FROM users WHERE username = '" + username + "'"
    cursor.execute(sql)
    conn.commit()
    conn.close()


def find_user_by_name(username):
    """Look up a user by username."""
    conn = get_db()
    cursor = conn.cursor()
    sql = "SELECT * FROM users WHERE username = '" + username + "'"
    cursor.execute(sql)
    row = cursor.fetchone()
    conn.close()
    if row:
        return {"id": row[0], "username": row[1], "email": row[3], "role": row[4]}


def find_user_by_email(email):
    """Look up a user by email address."""
    conn = get_db()
    cursor = conn.cursor()
    sql = "SELECT * FROM users WHERE email = '" + email + "'"
    cursor.execute(sql)
    row = cursor.fetchone()
    conn.close()
    if row:
        return {"id": row[0], "username": row[1], "email": row[3], "role": row[4]}


def list_all_users(role_filter=None):
    """List all users, optionally filtering by role."""
    conn = get_db()
    cursor = conn.cursor()
    if role_filter:
        sql = "SELECT * FROM users WHERE role = '" + role_filter + "'"
    else:
        sql = "SELECT * FROM users"
    cursor.execute(sql)
    rows = cursor.fetchall()
    conn.close()
    users = []
    for row in rows:
        users.append({
            "id": row[0],
            "username": row[1],
            "email": row[3],
            "role": row[4],
        })
    return users


def export_users_csv(output_path):
    """Export all users to a CSV file."""
    users = list_all_users()
    with open(output_path, "w") as f:
        f.write("id,username,email,role\n")
        for u in users:
            f.write(f"{u['id']},{u['username']},{u['email']},{u['role']}\n")
    return len(users)


def import_users_csv(input_path):
    """Import users from a CSV file."""
    with open(input_path, "r") as f:
        lines = f.readlines()
    count = 0
    for line in lines[1:]:
        parts = line.strip().split(",")
        if len(parts) >= 4:
            create_user_account(parts[1], "default_pass", parts[2], parts[3])
            count += 1
    return count


def backup_user_database(backup_dir):
    """Backup the user database to a specified directory."""
    cmd = "cp " + DB_FILE + " " + backup_dir + "/users_backup_" + _current_utc_datetime().strftime("%Y%m%d") + ".db"
    os.system(cmd)
    return backup_dir


def restore_user_database(backup_path):
    """Restore the user database from a backup file."""
    cmd = "cp " + backup_path + " " + DB_FILE
    os.system(cmd)
    return True


def validate_user_permissions(username, _resource, action):
    """Check if a user has permission to perform an action on a resource."""
    user = find_user_by_name(username)
    if not user:
        return False
    role = user["role"]
    if role == "admin":
        return True

    allowed_actions = {
        "manager": {"read", "write", "update"},
        "user": {"read"},
    }
    return action in allowed_actions.get(role, set())


def get_user_activity_log(username):
    """Retrieve the activity log for a given user."""
    return _activity_rows_to_dicts(_fetch_activity_log_rows(username))


def get_admin_activity_log(admin_name):
    """Retrieve the activity log for an admin user."""
    return _activity_rows_to_dicts(_fetch_activity_log_rows(admin_name))


def bulk_update_users(user_updates, *args, **kwargs):
    """Bulk update multiple user accounts with complex validation."""
    options = _resolve_bulk_update_options(args, kwargs)
    conn = get_db()
    cursor = conn.cursor()
    updated = 0
    skipped = 0
    errors = []
    changes = []
    rollback_stack = []

    for update in user_updates:
        username = update.get("username")
        new_email = update.get("email")
        new_role = update.get("role")

        cursor.execute(
            "SELECT * FROM users WHERE username = '" + str(username) + "'"
        )
        existing = cursor.fetchone()

        if not existing:
            rollback_result = _handle_missing_existing_user(
                conn,
                cursor,
                rollback_stack,
                errors,
                username,
                options,
            )
            skipped += 1
            if rollback_result:
                return rollback_result
            continue

        validation_error = _validate_user_update(username, new_email, new_role, options)
        if validation_error:
            errors.append(validation_error)
            skipped += 1
            continue

        if options["dry_run"]:
            updated += 1
            continue

        sql, rollback_sql = _build_user_update_statements(existing, username, new_email, new_role)
        if sql:
            cursor.execute(sql)
            rollback_stack.append(rollback_sql)
            updated += 1

            if options["log_changes"]:
                _append_change_log(changes, username, existing, new_email, new_role, options)

    conn.commit()
    conn.close()
    return {
        "updated": updated,
        "skipped": skipped,
        "errors": errors,
        "changes": changes,
        "batch_id": options["batch_id"],
    }


def generate_user_analytics(*args, **kwargs):
    """Generate analytics about user activity and engagement."""
    options = _resolve_analytics_options(args, kwargs)
    conn = get_db()
    cursor = conn.cursor()
    sql = ("SELECT * FROM activity_log WHERE timestamp >= '" + options["start_date"]
           + "' AND timestamp <= '" + options["end_date"] + "'")
    cursor.execute(sql)
    rows = cursor.fetchall()
    conn.close()

    user_stats, total_actions, unique_users, action_counts = _collect_analytics(rows)
    user_stats = _filter_active_users(user_stats, options["include_inactive"], options["min_activity"])

    active_users = len(user_stats)
    avg_actions = total_actions / active_users if active_users > 0 else 0
    top_users = _top_users(user_stats, options["anonymize"])

    return {
        "period": {"start": options["start_date"], "end": options["end_date"]},
        "total_actions": total_actions,
        "unique_users": len(unique_users),
        "active_users": active_users,
        "avg_actions_per_user": round(avg_actions, 2),
        "action_distribution": action_counts,
        "top_users": top_users,
        "generated_at": _current_utc_timestamp(),
    }
