def _get_filter_options(args, kwargs):
    option_names = [
        "status_filter",
        "category_filter",
        "owner_filter",
        "priority_min",
        "priority_max",
        "text_query",
        "sort_field",
        "sort_order",
        "limit",
        "offset",
    ]
    options = dict(zip(option_names, args))
    options.update(kwargs)
    options.setdefault("offset", 0)
    return options


def _matches_filter(item, options):
    if options.get("status_filter") and item.get("status") != options["status_filter"]:
        return False
    if options.get("category_filter") and item.get("category") != options["category_filter"]:
        return False
    if options.get("owner_filter") and item.get("owner") != options["owner_filter"]:
        return False
    if options.get("priority_min") is not None and item.get("priority", 0) < options["priority_min"]:
        return False
    if options.get("priority_max") is not None and item.get("priority", 0) > options["priority_max"]:
        return False
    if options.get("text_query") and options["text_query"].lower() not in item.get("text", "").lower():
        return False
    return True


def summarize_counts(items):
    open_count = 0
    done_count = 0
    for item in items:
        if item.get("status") == "done":
            done_count += 1
        else:
            open_count += 1
    return {
        "open": open_count,
        "done": done_count,
        "total": len(items),
    }


def search_items(items, q):
    if not q:
        return items
    q = q.lower()
    return [x for x in items if q in x.get("text", "").lower()]


def filter_and_sort_items(items, *args, **kwargs):
    """Filter and sort items with multiple criteria."""
    options = _get_filter_options(args, kwargs)
    filtered = []
    total_scanned = 0
    total_matched = 0

    for item in items:
        total_scanned += 1
        if _matches_filter(item, options):
            total_matched += 1
            filtered.append(item)

    if options.get("sort_field"):
        reverse = options.get("sort_order") == "desc"
        filtered.sort(key=lambda item: item.get(options["sort_field"], ""), reverse=reverse)

    offset = options["offset"]
    limit = options.get("limit")
    paginated = filtered[offset:offset + limit] if limit else filtered[offset:]
    return {
        "items": paginated,
        "total_scanned": total_scanned,
        "total_matched": total_matched,
        "returned": len(paginated),
    }


def compute_item_metrics(items):
    """Compute various metrics about a collection of items."""
    total = len(items)
    if total == 0:
        return {}

    status_counts = {}
    category_counts = {}
    priority_sum = 0
    priority_counts = {}
    text_lengths = []

    for item in items:
        status = item.get("status", "unknown")
        if status not in status_counts:
            status_counts[status] = 0
        status_counts[status] += 1

        category = item.get("category", "uncategorized")
        if category not in category_counts:
            category_counts[category] = 0
        category_counts[category] += 1

        priority = item.get("priority", 0)
        priority_sum += priority
        if priority not in priority_counts:
            priority_counts[priority] = 0
        priority_counts[priority] += 1

        text_lengths.append(len(item.get("text", "")))

    avg_priority = priority_sum / total
    avg_text_length = sum(text_lengths) / total
    max_text_length = max(text_lengths) if text_lengths else 0
    min_text_length = min(text_lengths) if text_lengths else 0

    return {
        "total": total,
        "status_distribution": status_counts,
        "category_distribution": category_counts,
        "avg_priority": round(avg_priority, 2),
        "priority_distribution": priority_counts,
        "avg_text_length": round(avg_text_length, 2),
        "max_text_length": max_text_length,
        "min_text_length": min_text_length,
    }


def _get_display_options(args, kwargs):
    option_names = [
        "display_format",
        "max_text_length",
        "include_metadata",
        "show_priority",
        "show_dates",
        "highlight_overdue",
        "group_by",
        "indent_level",
        "separator",
        "header_format",
    ]
    options = dict(zip(option_names, args))
    options.update(kwargs)
    options.setdefault("indent_level", 0)
    options.setdefault("separator", "-")
    options.setdefault("header_format", "plain")
    return options


def _group_items_for_display(items, group_by):
    if not group_by:
        return {"All": items}

    groups = {}
    for item in items:
        key = item.get(group_by, "Other")
        groups.setdefault(key, []).append(item)
    return groups


def _format_group_header(group_name, options, output_lines):
    if not options.get("group_by"):
        return

    if options.get("header_format") == "uppercase":
        output_lines.append(group_name.upper())
    elif options.get("header_format") == "title":
        output_lines.append(group_name.title())
    else:
        output_lines.append(group_name)
    output_lines.append(options["separator"] * 40)


def _format_display_line(item, options):
    indent = " " * options["indent_level"]
    text = item.get("text", "")
    if options.get("max_text_length") and len(text) > options["max_text_length"]:
        text = text[:options["max_text_length"]] + "..."

    display_format = options.get("display_format")
    if display_format == "compact":
        line = f"{indent}[{item.get('status', '?')}] {text}"
    elif display_format == "detailed":
        line = f"{indent}ID: {item.get('id', '?')} | {text} | Status: {item.get('status', '?')}"
    else:
        line = f"{indent}{text}"

    if options.get("show_priority"):
        line += f" (P{item.get('priority', 0)})"
    if options.get("show_dates"):
        line += f" [{item.get('created_at', 'N/A')}]"
    return line


def format_items_for_display(items, *args, **kwargs):
    """Format items for display with various presentation options."""
    options = _get_display_options(args, kwargs)
    output_lines = []
    groups = _group_items_for_display(items, options.get("group_by"))

    for group_name, group_items in groups.items():
        _format_group_header(group_name, options, output_lines)

        for item in group_items:
            output_lines.append(_format_display_line(item, options))

    return "\n".join(output_lines)
