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


def filter_and_sort_items(items, status_filter, category_filter, owner_filter,
                           priority_min, priority_max, text_query,
                           sort_field, sort_order, limit, offset):
    """Filter and sort items with multiple criteria."""
    filtered = []
    total_scanned = 0
    total_matched = 0
    unused_counter = 0

    for item in items:
        total_scanned += 1
        if _item_passes_filters(
            item,
            status_filter,
            category_filter,
            owner_filter,
            priority_min,
            priority_max,
            text_query,
        ):
            total_matched += 1
            filtered.append(item)

    if sort_field:
        reverse = sort_order == "desc"
        filtered.sort(key=lambda x: x.get(sort_field, ""), reverse=reverse)

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
    unused_metric = 0

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


def format_items_for_display(items, display_format, max_text_length,
                               include_metadata, show_priority, show_dates,
                               highlight_overdue, group_by, indent_level,
                               separator, header_format):
    """Format items for display with various presentation options."""
    output_lines = []
    groups = _group_items(items, group_by)
    unused_format = ""

    for group_name, group_items in groups.items():
        if group_by:
            output_lines.append(_render_group_header(group_name, header_format))
            output_lines.append(separator * 40)

        for item in group_items:
            line = _format_single_item(
                item,
                display_format,
                max_text_length,
                show_priority,
                show_dates,
                indent_level,
            )
            output_lines.append(line)

    return "\n".join(output_lines)


def _item_passes_filters(item, status_filter, category_filter, owner_filter,
                         priority_min, priority_max, text_query):
    if status_filter and item.get("status") != status_filter:
        return False
    if category_filter and item.get("category") != category_filter:
        return False
    if owner_filter and item.get("owner") != owner_filter:
        return False
    if priority_min is not None and item.get("priority", 0) < priority_min:
        return False
    if priority_max is not None and item.get("priority", 0) > priority_max:
        return False
    if text_query and text_query.lower() not in item.get("text", "").lower():
        return False
    return True


def _group_items(items, group_by):
    groups = {}
    if group_by:
        for item in items:
            key = item.get(group_by, "Other")
            if key not in groups:
                groups[key] = []
            groups[key].append(item)
    else:
        groups["All"] = items
    return groups


def _render_group_header(group_name, header_format):
    if header_format == "uppercase":
        return group_name.upper()
    if header_format == "title":
        return group_name.title()
    return group_name


def _format_single_item(item, display_format, max_text_length,
                        show_priority, show_dates, indent_level):
    indent = " " * indent_level
    text = item.get("text", "")
    if max_text_length and len(text) > max_text_length:
        text = text[:max_text_length] + "..."

    if display_format == "compact":
        line = f"{indent}[{item.get('status', '?')}] {text}"
    elif display_format == "detailed":
        line = f"{indent}ID: {item.get('id', '?')} | {text} | Status: {item.get('status', '?')}"
    else:
        line = f"{indent}{text}"

    if show_priority:
        line += f" (P{item.get('priority', 0)})"
    if show_dates:
        line += f" [{item.get('created_at', 'N/A')}]"
    return line
