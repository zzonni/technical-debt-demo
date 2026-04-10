_FILTER_SORT_KEYS = [
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

_DISPLAY_KEYS = [
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

_UNSET = object()


def _resolve_options(option_values, keys, args, kwargs):
    options = {}
    if option_values is _UNSET:
        positional_values = list(args)
    elif isinstance(option_values, dict):
        options.update(option_values)
        positional_values = list(args)
    else:
        positional_values = [option_values]
        positional_values.extend(args)

    for key, value in zip(keys, positional_values):
        options[key] = value

    for key in keys:
        if key in kwargs:
            options[key] = kwargs[key]

    return options


def _item_matches_filters(item, filters):
    status_filter = filters.get("status_filter")
    if status_filter and item.get("status") != status_filter:
        return False

    category_filter = filters.get("category_filter")
    if category_filter and item.get("category") != category_filter:
        return False

    owner_filter = filters.get("owner_filter")
    if owner_filter and item.get("owner") != owner_filter:
        return False

    priority_min = filters.get("priority_min")
    if priority_min is not None and item.get("priority", 0) < priority_min:
        return False

    priority_max = filters.get("priority_max")
    if priority_max is not None and item.get("priority", 0) > priority_max:
        return False

    text_query = filters.get("text_query")
    if text_query and text_query.lower() not in item.get("text", "").lower():
        return False

    return True


def _sort_items(items, sort_field, sort_order):
    if not sort_field:
        return items

    reverse = sort_order == "desc"
    items.sort(key=lambda item: item.get(sort_field, ""), reverse=reverse)
    return items


def _paginate_items(items, limit, offset):
    if limit:
        return items[offset:offset + limit]
    return items[offset:]


def _group_items(items, group_by):
    if not group_by:
        return {"All": items}

    groups = {}
    for item in items:
        key = item.get(group_by, "Other")
        groups.setdefault(key, []).append(item)
    return groups


def _format_group_header(group_name, header_format):
    if header_format == "uppercase":
        return group_name.upper()
    if header_format == "title":
        return group_name.title()
    return group_name


def _build_display_line(item, options):
    indent = " " * options.get("indent_level", 0)
    text = item.get("text", "")
    max_text_length = options.get("max_text_length")
    if max_text_length and len(text) > max_text_length:
        text = text[:max_text_length] + "..."

    display_format = options.get("display_format")
    if display_format == "compact":
        line = f"{indent}[{item.get('status', '?')}] {text}"
    elif display_format == "detailed":
        line = f"{indent}ID: {item.get('id', '?')} | {text} | Status: {item.get('status', '?')}"
    else:
        line = f"{indent}{text}"

    if options.get("include_metadata"):
        line += f" <{item.get('category', 'n/a')}:{item.get('owner', 'n/a')}>"
    if options.get("show_priority"):
        line += f" (P{item.get('priority', 0)})"
    if options.get("show_dates"):
        line += f" [{item.get('created_at', 'N/A')}]"
    if options.get("highlight_overdue") and item.get("overdue"):
        line = f"! {line}"

    return line


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


def filter_and_sort_items(items, filters=_UNSET, *args, **kwargs):
    """Filter and sort items with multiple criteria."""
    options = _resolve_options(filters, _FILTER_SORT_KEYS, args, kwargs)
    total_scanned = 0
    filtered = []

    for item in items:
        total_scanned += 1
        if _item_matches_filters(item, options):
            filtered.append(item)

    _sort_items(filtered, options.get("sort_field"), options.get("sort_order"))
    offset = options.get("offset", 0) or 0
    paginated = _paginate_items(filtered, options.get("limit"), offset)
    return {
        "items": paginated,
        "total_scanned": total_scanned,
        "total_matched": len(filtered),
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


def format_items_for_display(items, options=_UNSET, *args, **kwargs):
    """Format items for display with various presentation options."""
    display_options = _resolve_options(options, _DISPLAY_KEYS, args, kwargs)
    output_lines = []
    groups = _group_items(items, display_options.get("group_by"))

    for group_name, group_items in groups.items():
        if display_options.get("group_by"):
            output_lines.append(_format_group_header(group_name, display_options.get("header_format")))
            output_lines.append(display_options.get("separator", "-") * 40)

        for item in group_items:
            output_lines.append(_build_display_line(item, display_options))

    return "\n".join(output_lines)
