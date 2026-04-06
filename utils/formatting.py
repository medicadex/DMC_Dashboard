import locale

def format_naira(amount):
    """Formats a number as Nigerian Naira (₦) with comma separators."""
    if amount is None:
        return "₦0.00"
    try:
        val = float(amount)
        return f"₦{val:,.2f}"
    except (ValueError, TypeError):
        return "₦0.00"

def format_naira_millions(amount):
    """
    Formats a number as Nigerian Naira (₦) in millions with two decimal places.
    Example: 1,234,560,000 becomes ₦1,234.56
    """
    if amount is None:
        return "₦0.00"
    try:
        val = float(amount) / 1000000.0
        return f"₦{val:,.2f}"
    except (ValueError, TypeError):
        return "₦0.00"

def parse_currency(currency_str):
    """Removes Naira symbol and commas to get a float."""
    if not currency_str:
        return 0.0
    try:
        clean_str = str(currency_str).replace('₦', '').replace(',', '').strip()
        return float(clean_str)
    except (ValueError, TypeError):
        return 0.0
