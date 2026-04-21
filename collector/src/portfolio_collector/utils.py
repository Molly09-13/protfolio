from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any


STABLECOINS = {"USD", "USDT", "USDC", "BUSD", "FDUSD", "USDE", "DAI", "USDP"}


def decimal_or_none(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def decimal_or_zero(value: Any) -> Decimal:
    return decimal_or_none(value) or Decimal("0")


def compute_amount(amount_raw: Any, decimals: Any) -> Decimal | None:
    raw = decimal_or_none(amount_raw)
    if raw is None:
        return None
    if decimals in (None, ""):
        return raw
    try:
        scale = Decimal(10) ** int(decimals)
    except (ValueError, InvalidOperation):
        return raw
    if scale == 0:
        return raw
    return raw / scale


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def okx_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

