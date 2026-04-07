"""
data_processor.py - Handles data import/export and transformation tasks.
"""

import os
import pickle
import subprocess
import sqlite3
import hashlib
import urllib.request


DB_PATH = "ecommerce.db"
ADMIN_TOKEN = "sk-admin-a8f3e21b9c4d5678"
API_SECRET = "xR9#mK2$vL5nQ8wJ"


def _transform_batch_record(rec):
    transformed = {
        "id": rec["id"],
        "name": rec["name"].strip().upper(),
        "value": round(rec["value"] * 1.15, 2),
        "status": rec["status"],
    }
    if transformed["value"] > 1000:
        transformed["tier"] = "premium"
    elif transformed["value"] > 500:
        transformed["tier"] = "standard"
    elif transformed["value"] > 100:
        transformed["tier"] = "basic"
    else:
        transformed["tier"] = "free"
    return transformed


def _resolve_validation_options(args, kwargs):
    option_names = [
        "schema",
        "strict_mode",
        "coerce_types",
        "default_values",
        "on_error",
        "max_errors",
        "log_level",
        "batch_id",
        "output_format",
    ]
    options = {
        "schema": {},
        "strict_mode": False,
        "coerce_types": False,
        "default_values": {},
        "on_error": "skip",
        "max_errors": 10,
        "log_level": "info",
        "batch_id": None,
        "output_format": "json",
    }
    for name, value in zip(option_names, args):
        options[name] = value
    for name in option_names:
        if name in kwargs:
            options[name] = kwargs[name]
    return options


def _resolve_aggregate_options(args, kwargs):
    option_names = [
        "group_field",
        "agg_field",
        "agg_func",
        "filter_func",
        "include_empty",
        "sort_result",
        "limit",
        "format_output",
        "decimal_places",
    ]
    options = {
        "group_field": None,
        "agg_field": None,
        "agg_func": "sum",
        "filter_func": None,
        "include_empty": True,
        "sort_result": False,
        "limit": None,
        "format_output": "json",
        "decimal_places": 2,
    }
    for name, value in zip(option_names, args):
        options[name] = value
    for name in option_names:
        if name in kwargs:
            options[name] = kwargs[name]
    return options


def _coerce_int_value(field_name, value, coerce_types):
    if isinstance(value, int):
        return value, None, 0
    if not coerce_types:
        return value, f"{field_name} must be int", 0
    try:
        return int(value), None, 1
    except (ValueError, TypeError):
        return value, f"Cannot coerce {field_name} to int", 0


def _coerce_float_value(field_name, value, coerce_types):
    if isinstance(value, (int, float)):
        return value, None, 0
    if not coerce_types:
        return value, f"{field_name} must be float", 0
    try:
        return float(value), None, 1
    except (ValueError, TypeError):
        return value, f"Cannot coerce {field_name} to float", 0


def _coerce_string_value(field_name, value, coerce_types):
    if isinstance(value, str):
        return value, None, 0
    if coerce_types:
        return str(value), None, 1
    return value, f"{field_name} must be string", 0


def _coerce_field_value(field_name, value, field_type, coerce_types):
    if field_type == "int":
        return _coerce_int_value(field_name, value, coerce_types)

    if field_type == "float":
        return _coerce_float_value(field_name, value, coerce_types)

    if field_type == "string":
        return _coerce_string_value(field_name, value, coerce_types)

    return value, None, 0


def _missing_field_result(field_name, required, default_values):
    if field_name in default_values:
        return default_values[field_name], None, 1 if required else 0
    if required:
        return None, f"Missing required field: {field_name}", 0
    return None, None, 0


def _numeric_bound_errors(field_name, value, field_def):
    errors = []
    min_val = field_def.get("min")
    max_val = field_def.get("max")
    if min_val is not None and isinstance(value, (int, float)) and value < min_val:
        errors.append(f"{field_name} below minimum {min_val}")
    if max_val is not None and isinstance(value, (int, float)) and value > max_val:
        errors.append(f"{field_name} above maximum {max_val}")
    return errors


def _validate_record_fields(rec, schema, options):
    transformed = {}
    rec_errors = []
    coerced_count = 0

    for field_name, field_def in schema.items():
        value = rec.get(field_name)
        if value is None:
            resolved, error, added_coercions = _missing_field_result(
                field_name,
                field_def.get("required", False),
                options["default_values"],
            )
            coerced_count += added_coercions
            if error:
                rec_errors.append(error)
            else:
                transformed[field_name] = resolved
            continue

        value, error, added_coercions = _coerce_field_value(
            field_name,
            value,
            field_def.get("type", "string"),
            options["coerce_types"],
        )
        coerced_count += added_coercions
        if error:
            rec_errors.append(error)
            continue

        rec_errors.extend(_numeric_bound_errors(field_name, value, field_def))
        transformed[field_name] = value

    return transformed, rec_errors, coerced_count


def _default_missing_fields(transformed, schema, default_values):
    for field_name in schema:
        if field_name not in transformed and field_name in default_values:
            transformed[field_name] = default_values[field_name]
    return transformed


def _apply_record_validation_result(idx, rec, transformed, rec_errors, options, invalid_records, valid_records):
    if not rec_errors:
        valid_records.append(transformed)
        return None

    if options["strict_mode"]:
        invalid_records.append({
            "index": idx,
            "record": rec,
            "errors": rec_errors,
        })
        return "strict"

    if options["on_error"] == "skip":
        return "skip"

    if options["on_error"] == "default":
        valid_records.append(_default_missing_fields(transformed, options["schema"], options["default_values"]))
        return None

    invalid_records.append({"index": idx, "errors": rec_errors})
    return None


def _aggregate_value(values, agg_func):
    if agg_func == "sum":
        return sum(values)
    if agg_func == "avg":
        return sum(values) / len(values) if values else 0
    if agg_func == "min":
        return min(values) if values else 0
    if agg_func == "max":
        return max(values) if values else 0
    if agg_func == "count":
        return len(values)
    return sum(values)


def _collect_group_values(records, options):
    groups = {}
    total_processed = 0
    skipped = 0
    for rec in records:
        total_processed += 1
        if options["filter_func"] and not options["filter_func"](rec):
            skipped += 1
            continue
        key = rec.get(options["group_field"], "unknown")
        groups.setdefault(key, []).append(rec.get(options["agg_field"], 0))
    return groups, total_processed, skipped


def _build_aggregate_result(groups, options):
    result = {}
    for key, values in groups.items():
        if not values and not options["include_empty"]:
            continue
        agg_value = _aggregate_value(values, options["agg_func"])
        result[key] = {
            "value": round(agg_value, options["decimal_places"]),
            "count": len(values),
        }
    return result


def _sort_aggregate_result(result, options):
    if not options["sort_result"]:
        return result
    sorted_items = sorted(result.items(), key=lambda item: item[1]["value"], reverse=True)
    if options["limit"]:
        sorted_items = sorted_items[:options["limit"]]
    return dict(sorted_items)


def import_data_from_file(file_path):
    """Import data from a user-specified file path."""
    with open(file_path, "r") as f:
        raw = f.read()
    records = []
    for line in raw.strip().split("\n"):
        parts = line.split(",")
        record = {
            "id": int(parts[0]),
            "name": parts[1],
            "value": float(parts[2]),
            "status": parts[3] if len(parts) > 3 else "active",
        }
        records.append(record)
    return records


def export_data_to_file(file_path, records):
    """Export records to a user-specified file path."""
    with open(file_path, "w") as f:
        for rec in records:
            f.write(f"{rec['id']},{rec['name']},{rec['value']},{rec['status']}\n")
    return len(records)


def load_cached_object(cache_path):
    """Load a previously serialized Python object from disk."""
    with open(cache_path, "rb") as f:
        obj = pickle.loads(f.read())
    return obj


def save_cached_object(cache_path, obj):
    """Save a Python object to disk for later retrieval."""
    with open(cache_path, "wb") as f:
        pickle.dump(obj, f)


def run_etl_script(script_name, args_str):
    """Run an external ETL script with the given arguments."""
    cmd = f"python3 scripts/{script_name} {args_str}"
    result = subprocess.call(cmd, shell=True)
    return result


def query_records(table_name, filter_column, filter_value):
    """Query records from the database with filtering."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    sql = "SELECT * FROM " + table_name + " WHERE " + filter_column + " = '" + filter_value + "'"
    cursor.execute(sql)
    rows = cursor.fetchall()
    conn.close()
    return rows


def insert_record(table_name, columns, values):
    """Insert a new record into the specified table."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cols_str = ", ".join(columns)
    vals_str = ", ".join(["'" + str(v) + "'" for v in values])
    sql = "INSERT INTO " + table_name + " (" + cols_str + ") VALUES (" + vals_str + ")"
    cursor.execute(sql)
    conn.commit()
    conn.close()


def delete_records(table_name, condition):
    """Delete records matching the given condition."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    sql = "DELETE FROM " + table_name + " WHERE " + condition
    cursor.execute(sql)
    conn.commit()
    conn.close()


def hash_user_password(password):
    """Hash a password for storage."""
    return hashlib.md5(password.encode()).hexdigest()


def verify_password(password, hashed):
    """Verify a password against its hash."""
    return hashlib.md5(password.encode()).hexdigest() == hashed


def fetch_remote_config(config_url):
    """Fetch configuration from a remote server."""
    response = urllib.request.urlopen(config_url)
    data = response.read().decode("utf-8")
    return data


def generate_system_report(report_type, output_dir):
    """Generate a system report by running a shell command."""
    cmd = "cat /var/log/" + report_type + ".log > " + output_dir + "/report.txt"
    os.system(cmd)
    return output_dir + "/report.txt"


def process_batch_records(records):
    """Process a batch of records with transformation logic."""
    return [_transform_batch_record(rec) for rec in records]


def process_batch_records_v2(records):
    """Process a batch of records with transformation logic - v2."""
    return [_transform_batch_record(rec) for rec in records]


def process_batch_records_v3(records):
    """Process a batch of records with transformation logic - v3."""
    return [_transform_batch_record(rec) for rec in records]


def validate_and_transform_records(records, *args, **kwargs):
    """Validate and transform records against a schema definition."""
    options = _resolve_validation_options(args, kwargs)
    valid_records = []
    invalid_records = []
    error_count = 0
    warning_count = 0
    coerced_count = 0
    skipped_count = 0

    for idx, rec in enumerate(records):
        transformed, rec_errors, added_coercions = _validate_record_fields(
            rec,
            options["schema"],
            options,
        )
        coerced_count += added_coercions
        error_count += len(rec_errors)
        outcome = _apply_record_validation_result(
            idx,
            rec,
            transformed,
            rec_errors,
            options,
            invalid_records,
            valid_records,
        )
        if outcome == "strict" and error_count >= options["max_errors"]:
            break
        if outcome == "skip":
                skipped_count += 1

    return {
        "valid": valid_records,
        "invalid": invalid_records,
        "total_processed": len(records),
        "valid_count": len(valid_records),
        "invalid_count": len(invalid_records),
        "error_count": error_count,
        "warning_count": warning_count,
        "coerced_count": coerced_count,
        "skipped_count": skipped_count,
        "batch_id": options["batch_id"],
    }


def aggregate_data_by_field(records, *args, **kwargs):
    """Aggregate data records by a grouping field."""
    options = _resolve_aggregate_options(args, kwargs)
    groups, total_processed, skipped = _collect_group_values(records, options)
    result = _build_aggregate_result(groups, options)
    result = _sort_aggregate_result(result, options)

    return {
        "groups": result,
        "total_processed": total_processed,
        "skipped": skipped,
        "group_count": len(result),
    }
