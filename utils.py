from dataclasses import dataclass


# pylint: disable=too-many-instance-attributes
@dataclass
class FilterAndSortConfig:
    status_filter: str = ""
    category_filter: str = ""
    owner_filter: str = ""
    priority_min: int | None = None
    priority_max: int | None = None
    text_query: str = ""
    sort_field: str = ""
    sort_order: str = "asc"
    limit: int | None = None
    offset: int = 0


# pylint: disable=too-many-instance-attributes
@dataclass
class FormatItemsConfig:
    display_format: str = "compact"
    max_text_length: int | None = None
    include_metadata: bool = False
    show_priority: bool = False
    show_dates: bool = False
    highlight_overdue: bool = False
    group_by: str | None = None
    indent_level: int = 0
    separator: str = "-"
    header_format: str = "plain"


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


def _item_matches_filter(item, cfg):
    match = True
    if cfg.status_filter and item.get("status") != cfg.status_filter:
        match = False
    if cfg.category_filter and item.get("category") != cfg.category_filter:
        match = False
    if cfg.owner_filter and item.get("owner") != cfg.owner_filter:
        match = False
    if cfg.priority_min is not None and item.get("priority", 0) < cfg.priority_min:
        match = False
    if cfg.priority_max is not None and item.get("priority", 0) > cfg.priority_max:
        match = False
    if cfg.text_query and cfg.text_query.lower() not in item.get("text", "").lower():
        match = False
    return match


def filter_and_sort_items(items, config=None):
    """Filter and sort items with multiple criteria."""
    cfg = config or FilterAndSortConfig()
    filtered = []
    total_scanned = 0
    total_matched = 0

    for item in items:
        total_scanned += 1
        if not _item_matches_filter(item, cfg):
            continue
        total_matched += 1
        filtered.append(item)

    if cfg.sort_field:
        filtered.sort(
            key=lambda x: x.get(cfg.sort_field, ""),
            reverse=cfg.sort_order == "desc",
        )

    paginated = filtered[cfg.offset:cfg.offset + cfg.limit] if cfg.limit else filtered[cfg.offset:]
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


def format_items_for_display(items, config=None):
    """Format items for display with various presentation options."""
    cfg = config or FormatItemsConfig()
    output_lines = []
    groups = {}

    if cfg.group_by:
        for item in items:
            key = item.get(cfg.group_by, "Other")
            groups.setdefault(key, []).append(item)
    else:
        groups["All"] = items

    for group_name, group_items in groups.items():
        if cfg.group_by:
            _append_group_header(output_lines, group_name, cfg)

        for item in group_items:
            line = _format_item_line(item, cfg)
            output_lines.append(line)

    return "\n".join(output_lines)


def _append_group_header(output_lines, group_name, cfg):
    if cfg.header_format == "uppercase":
        output_lines.append(group_name.upper())
    elif cfg.header_format == "title":
        output_lines.append(group_name.title())
    else:
        output_lines.append(group_name)
    output_lines.append(cfg.separator * 40)


def _format_item_line(item, cfg):
    indent = " " * cfg.indent_level
    text = item.get("text", "")
    if cfg.max_text_length and len(text) > cfg.max_text_length:
        text = text[:cfg.max_text_length] + "..."

    if cfg.display_format == "compact":
        line = f"{indent}[{item.get('status', '?')}] {text}"
    elif cfg.display_format == "detailed":
        line = (
            f"{indent}ID: {item.get('id', '?')} | {text} | "
            f"Status: {item.get('status', '?')}"
        )
    else:
        line = f"{indent}{text}"

    if cfg.show_priority:
        line += f" (P{item.get('priority', 0)})"
    if cfg.show_dates:
        line += f" [{item.get('created_at', 'N/A')}]"

    return line
