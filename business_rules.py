from datetime import date, datetime, timedelta


def to_date(value) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        return datetime.fromisoformat(value).date()
    raise TypeError(f"Unsupported date type: {type(value)!r}")


def warranty_status(order_date) -> tuple[bool, date]:
    purchased = to_date(order_date)
    expiry = purchased + timedelta(days=365)
    return date.today() <= expiry, expiry


def return_status(order_date) -> tuple[bool, date]:
    purchased = to_date(order_date)
    return_deadline = purchased + timedelta(days=30)
    return date.today() <= return_deadline, return_deadline


def can_create_new_order(order_value: float) -> bool:
    return order_value < 10000


def applecare_details(device: str, apple_care_enabled: bool) -> str:
    normalized = (device or "").strip().lower()

    if normalized == "macbook":
        rule = "MacBook AppleCare minimum charge is $99."
    elif normalized == "iphone":
        rule = "iPhone AppleCare includes full replacement coverage."
    elif normalized == "ipad":
        rule = "iPad AppleCare minimum charge is $49."
    else:
        rule = "AppleCare terms vary by device."

    if apple_care_enabled:
        return f"AppleCare is active for this order. {rule}"
    return f"AppleCare is not active for this order. {rule}"
