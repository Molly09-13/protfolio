from __future__ import annotations

from decimal import Decimal
from typing import Any

from .utils import STABLECOINS, decimal_or_none


def normalize_usd_symbol(symbol: str) -> str:
    normalized = symbol.upper()
    if normalized in {"USD1", "USDG", "USDE", "RWUSD"}:
        return "USD"
    return normalized


def stablecoin_price(symbol: str) -> Decimal | None:
    if normalize_usd_symbol(symbol) in STABLECOINS:
        return Decimal("1")
    return None


def build_price_from_tickers(
    tickers: list[dict[str, Any]],
    *,
    symbol_key: str,
    price_key: str,
    quote_assets: tuple[str, ...] = ("USDT", "USDC", "USD"),
) -> dict[str, Decimal]:
    prices: dict[str, Decimal] = {}
    quote_order = {quote: index for index, quote in enumerate(quote_assets)}
    selected_rank: dict[str, int] = {}

    for ticker in tickers:
        market = str(ticker.get(symbol_key) or "").upper()
        price = decimal_or_none(ticker.get(price_key))
        if not market or price is None or price <= 0:
            continue

        for quote in quote_assets:
            if not market.endswith(quote) or market == quote:
                continue
            base = market[: -len(quote)]
            rank = quote_order[quote]
            if base and (base not in prices or rank < selected_rank[base]):
                prices[base] = price
                selected_rank[base] = rank
            break

    for stable in STABLECOINS:
        prices[stable] = Decimal("1")
    for stable in ("USD1", "USDG", "USDE", "RWUSD"):
        prices[stable] = Decimal("1")
    return prices
