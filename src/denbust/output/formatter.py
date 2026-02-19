"""Output formatting for unified items."""

from denbust.data_models import Category, SubCategory, UnifiedItem

# Category icons
CATEGORY_ICONS: dict[tuple[Category, SubCategory | None], str] = {
    (Category.BROTHEL, SubCategory.CLOSURE): "\U0001f6a8",  # 🚨
    (Category.BROTHEL, SubCategory.OPENING): "\u26a0\ufe0f",  # ⚠️
    (Category.PROSTITUTION, SubCategory.ARREST): "\U0001f46e",  # 👮
    (Category.PROSTITUTION, SubCategory.FINE): "\U0001f4b8",  # 💸
    (Category.PIMPING, SubCategory.ARREST): "\U0001f46e",  # 👮
    (Category.PIMPING, SubCategory.SENTENCE): "\u2696\ufe0f",  # ⚖️
    (Category.TRAFFICKING, SubCategory.ARREST): "\U0001f46e",  # 👮
    (Category.TRAFFICKING, SubCategory.RESCUE): "\U0001f198",  # 🆘
    (Category.TRAFFICKING, SubCategory.SENTENCE): "\u2696\ufe0f",  # ⚖️
    (Category.ENFORCEMENT, SubCategory.OPERATION): "\U0001f50d",  # 🔍
    (Category.ENFORCEMENT, SubCategory.OTHER): "\U0001f4cb",  # 📋
}

# Hebrew category names
CATEGORY_NAMES_HE: dict[Category, str] = {
    Category.BROTHEL: "בית בושת",
    Category.PROSTITUTION: "זנות",
    Category.PIMPING: "סרסור",
    Category.TRAFFICKING: "סחר בבני אדם",
    Category.ENFORCEMENT: "אכיפה",
    Category.NOT_RELEVANT: "לא רלוונטי",
}

# Hebrew sub-category names
SUBCATEGORY_NAMES_HE: dict[SubCategory, str] = {
    SubCategory.CLOSURE: "סגירה",
    SubCategory.OPENING: "פתיחה",
    SubCategory.ARREST: "מעצר",
    SubCategory.FINE: "קנס",
    SubCategory.SENTENCE: "גזר דין",
    SubCategory.RESCUE: "חילוץ",
    SubCategory.OPERATION: "מבצע",
    SubCategory.OTHER: "אחר",
}


def get_icon(category: Category, sub_category: SubCategory | None) -> str:
    """Get the icon for a category/sub-category combination.

    Args:
        category: Article category.
        sub_category: Article sub-category.

    Returns:
        Icon string.
    """
    icon = CATEGORY_ICONS.get((category, sub_category))
    if icon:
        return icon

    # Fallback icons by category
    fallback: dict[Category, str] = {
        Category.BROTHEL: "\U0001f6a8",  # 🚨
        Category.PROSTITUTION: "\U0001f46e",  # 👮
        Category.PIMPING: "\U0001f46e",  # 👮
        Category.TRAFFICKING: "\U0001f198",  # 🆘
        Category.ENFORCEMENT: "\U0001f50d",  # 🔍
        Category.NOT_RELEVANT: "\u2753",  # ❓
    }
    return fallback.get(category, "\U0001f4f0")  # 📰


def format_category(category: Category, sub_category: SubCategory | None) -> str:
    """Format category and sub-category as Hebrew string.

    Args:
        category: Article category.
        sub_category: Article sub-category.

    Returns:
        Formatted category string.
    """
    cat_name = CATEGORY_NAMES_HE.get(category, str(category.value))

    if sub_category:
        subcat_name = SUBCATEGORY_NAMES_HE.get(sub_category, str(sub_category.value))
        return f"{cat_name} » {subcat_name}"

    return cat_name


def format_unified_item(item: UnifiedItem) -> str:
    """Format a unified item for CLI output.

    Args:
        item: Unified item to format.

    Returns:
        Formatted string.
    """
    icon = get_icon(item.category, item.sub_category)
    category_str = format_category(item.category, item.sub_category)
    date_str = item.date.strftime("%Y-%m-%d")

    # Build output
    lines = [
        f"{icon} {item.headline}",
        f"תאריך: {date_str}",
        f"קטגוריה: {category_str}",
        "",
        f"תקציר: {item.summary[:300]}{'...' if len(item.summary) > 300 else ''}",
        "",
        "מקורות:",
    ]

    for source in item.sources:
        lines.append(f"• {source.source_name}: {source.url}")

    return "\n".join(lines)


def format_items(items: list[UnifiedItem]) -> str:
    """Format a list of unified items for CLI output.

    Args:
        items: List of unified items.

    Returns:
        Formatted string.
    """
    if not items:
        return "לא נמצאו כתבות רלוונטיות."

    separator = "\n" + "─" * 60 + "\n"
    formatted = [format_unified_item(item) for item in items]
    return separator.join(formatted)


def print_items(items: list[UnifiedItem]) -> None:
    """Print unified items to stdout.

    Args:
        items: List of unified items.
    """
    print(format_items(items))
    if items:
        print(f"\n{'─' * 60}")
        print(f"סה״כ: {len(items)} כתבות")
