"""
data_processor.py - Handles data import/export and transformation tasks.
"""

import json
import os
import subprocess
import sqlite3
import hashlib
import urllib.request


DB_PATH = "ecommerce.db"
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN")
API_SECRET = os.environ.get("API_SECRET")
ALLOWED_TABLES = {"products", "users", "orders", "activity_log", "audit_log"}
ALLOWED_COLUMNS = {"id", "name", "value", "status", "price", "role", "date", "username", "email", "resource", "action", "timestamp"}


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
    with open(cache_path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    return obj


def save_cached_object(cache_path, obj):
    """Save a Python object to disk for later retrieval."""
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(obj, f)


def run_etl_script(script_name, args_str):
    """Run an external ETL script with the given arguments."""
    command = ["python3", f"scripts/{script_name}"]
    if args_str:
        command.extend(args_str.split())
    result = subprocess.call(command)
    return result


def _validate_identifier(identifier, allowed_values):
    if identifier not in allowed_values:
        raise ValueError(f"Unsupported identifier: {identifier}")
    return identifier


def _parse_condition(condition):
    parts = [part.strip() for part in condition.split("=", 1)]
    if len(parts) != 2:
        raise ValueError("Only simple equality conditions are supported")
    column, value = parts
    _validate_identifier(column, ALLOWED_COLUMNS)
    return column, value.strip().strip("'\"")


def query_records(table_name, filter_column, filter_value):
    """Query records from the database with filtering."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    table_name = _validate_identifier(table_name, ALLOWED_TABLES)
    filter_column = _validate_identifier(filter_column, ALLOWED_COLUMNS)
    cursor.execute(
        f"SELECT * FROM {table_name} WHERE {filter_column} = ?",
        (filter_value,),
    )
    rows = cursor.fetchall()
    conn.close()
    return rows


def insert_record(table_name, columns, values):
    """Insert a new record into the specified table."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    table_name = _validate_identifier(table_name, ALLOWED_TABLES)
    safe_columns = [_validate_identifier(column, ALLOWED_COLUMNS) for column in columns]
    placeholders = ", ".join(["?"] * len(values))
    cols_str = ", ".join(safe_columns)
    cursor.execute(
        f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders})",
        tuple(values),
    )
    conn.commit()
    conn.close()


def delete_records(table_name, condition):
    """Delete records matching the given condition."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    table_name = _validate_identifier(table_name, ALLOWED_TABLES)
    column, value = _parse_condition(condition)
    cursor.execute(f"DELETE FROM {table_name} WHERE {column} = ?", (value,))
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
    log_path = os.path.join("/var/log", f"{os.path.basename(report_type)}.log")
    output_path = os.path.join(output_dir, "report.txt")
    with open(log_path, "r", encoding="utf-8") as source, open(output_path, "w", encoding="utf-8") as target:
        target.write(source.read())
    return output_dir + "/report.txt"


def _process_record_batch(records):
    processed = []
    for rec in records:
        new_rec = {}
        new_rec["id"] = rec["id"]
        new_rec["name"] = rec["name"].strip().upper()
        new_rec["value"] = round(rec["value"] * 1.15, 2)
        new_rec["status"] = rec["status"]
        if new_rec["value"] > 1000:
            new_rec["tier"] = "premium"
        elif new_rec["value"] > 500:
            new_rec["tier"] = "standard"
        elif new_rec["value"] > 100:
            new_rec["tier"] = "basic"
        else:
            new_rec["tier"] = "free"
        processed.append(new_rec)
    return processed


def process_batch_records(records):
    """Process a batch of records with transformation logic."""
    return _process_record_batch(records)


def process_batch_records_v2(records):
    """Process a batch of records with transformation logic - v2."""
    return _process_record_batch(records)


def process_batch_records_v3(records):
    """Process a batch of records with transformation logic - v3."""
    return _process_record_batch(records)


def _get_transform_options(args, kwargs):
    option_names = [
        "strict_mode",
        "coerce_types",
        "default_values",
        "on_error",
        "max_errors",
        "log_level",
        "batch_id",
        "output_format",
    ]
    options = dict(zip(option_names, args))
    options.update(kwargs)
    options.setdefault("strict_mode", False)
    options.setdefault("coerce_types", False)
    options.setdefault("default_values", {})
    options.setdefault("on_error", "skip")
    options.setdefault("max_errors", 10)
    options.setdefault("batch_id", None)
    return options


def _coerce_int(field_name, value, coerce_types, rec_errors):
    if isinstance(value, int):
        return value, 0, True
    if not coerce_types:
        rec_errors.append(f"{field_name} must be int")
        return value, 0, False
    try:
        return int(value), 1, True
    except (ValueError, TypeError):
        rec_errors.append(f"Cannot coerce {field_name} to int")
        return value, 0, False


def _coerce_float(field_name, value, coerce_types, rec_errors):
    if isinstance(value, (int, float)):
        return value, 0, True
    if not coerce_types:
        rec_errors.append(f"{field_name} must be float")
        return value, 0, False
    try:
        return float(value), 1, True
    except (ValueError, TypeError):
        rec_errors.append(f"Cannot coerce {field_name} to float")
        return value, 0, False


def _coerce_string(field_name, value, coerce_types, rec_errors):
    if isinstance(value, str):
        return value, 0, True
    if not coerce_types:
        rec_errors.append(f"{field_name} must be string")
        return value, 0, False
    return str(value), 1, True


def _coerce_value(field_name, value, field_type, coerce_types, rec_errors):
    coercers = {
        "int": _coerce_int,
        "float": _coerce_float,
        "string": _coerce_string,
    }
    if field_type in coercers:
        return coercers[field_type](field_name, value, coerce_types, rec_errors)
    return value, 0, True


def _apply_default_or_missing(field_name, required, transformed, options, rec_errors):
    if field_name in options["default_values"]:
        transformed[field_name] = options["default_values"][field_name]
        return 1 if required else 0
    if required:
        rec_errors.append(f"Missing required field: {field_name}")
    else:
        transformed[field_name] = None
    return 0


def _append_bound_errors(field_name, value, field_def, rec_errors):
    min_val = field_def.get("min")
    max_val = field_def.get("max")
    if min_val is not None and isinstance(value, (int, float)) and value < min_val:
        rec_errors.append(f"{field_name} below minimum {min_val}")
    if max_val is not None and isinstance(value, (int, float)) and value > max_val:
        rec_errors.append(f"{field_name} above maximum {max_val}")


def _transform_record(rec, schema, options):
    transformed = {}
    rec_errors = []
    coerced_count = 0

    for field_name, field_def in schema.items():
        value = rec.get(field_name)
        required = field_def.get("required", False)
        field_type = field_def.get("type", "string")

        if value is None:
            coerced_count += _apply_default_or_missing(
                field_name,
                required,
                transformed,
                options,
                rec_errors,
            )
            continue

        value, coerced, is_valid = _coerce_value(
            field_name,
            value,
            field_type,
            options["coerce_types"],
            rec_errors,
        )
        coerced_count += coerced
        if not is_valid:
            continue

        _append_bound_errors(field_name, value, field_def, rec_errors)
        transformed[field_name] = value

    return transformed, rec_errors, coerced_count


def _handle_record_errors(idx, rec, transformed, rec_errors, options, invalid_records, valid_records):
    if not rec_errors:
        valid_records.append(transformed)
        return "valid"
    if options["strict_mode"]:
        invalid_records.append({"index": idx, "record": rec, "errors": rec_errors})
        return "strict_invalid"
    if options["on_error"] == "skip":
        return "skipped"
    if options["on_error"] == "default":
        valid_records.append(transformed)
        return "defaulted"
    invalid_records.append({"index": idx, "errors": rec_errors})
    return "invalid"


def _apply_default_fields(schema, transformed, default_values):
    for field_name in schema:
        if field_name not in transformed and field_name in default_values:
            transformed[field_name] = default_values[field_name]


def validate_and_transform_records(records, schema, *args, **kwargs):
    """Validate and transform records against a schema definition."""
    options = _get_transform_options(args, kwargs)
    valid_records = []
    invalid_records = []
    error_count = 0
    warning_count = 0
    coerced_count = 0
    skipped_count = 0

    for idx, rec in enumerate(records):
        transformed, rec_errors, record_coerced = _transform_record(rec, schema, options)
        coerced_count += record_coerced
        error_count += len(rec_errors)
        if rec_errors and options["on_error"] == "default":
            _apply_default_fields(schema, transformed, options["default_values"])

        outcome = _handle_record_errors(
            idx,
            rec,
            transformed,
            rec_errors,
            options,
            invalid_records,
            valid_records,
        )
        if outcome == "strict_invalid" and error_count >= options["max_errors"]:
            break
        if outcome == "skipped":
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


def _get_aggregate_options(args, kwargs):
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
    options = dict(zip(option_names, args))
    options.update(kwargs)
    options.setdefault("include_empty", True)
    options.setdefault("sort_result", False)
    options.setdefault("limit", None)
    options.setdefault("decimal_places", 2)
    return options


def _aggregate_values(values, agg_func):
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


def aggregate_data_by_field(records, *args, **kwargs):
    """Aggregate data records by a grouping field."""
    options = _get_aggregate_options(args, kwargs)
    groups = {}
    total_processed = 0
    skipped = 0

    for rec in records:
        total_processed += 1
        if options.get("filter_func") and not options["filter_func"](rec):
            skipped += 1
            continue

        key = rec.get(options["group_field"], "unknown")
        if key not in groups:
            groups[key] = []
        groups[key].append(rec.get(options["agg_field"], 0))

    result = {}
    for key in groups:
        values = groups[key]
        if not values and not options["include_empty"]:
            continue
        agg_value = _aggregate_values(values, options["agg_func"])

        result[key] = {
            "value": round(agg_value, options["decimal_places"]),
            "count": len(values),
        }

    if options["sort_result"]:
        sorted_items = sorted(result.items(), key=lambda x: x[1]["value"], reverse=True)
        if options["limit"]:
            sorted_items = sorted_items[:options["limit"]]
        result = dict(sorted_items)

    return {
        "groups": result,
        "total_processed": total_processed,
        "skipped": skipped,
        "group_count": len(result),
    }
