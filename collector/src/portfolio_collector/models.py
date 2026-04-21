from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any


JsonDict = dict[str, Any]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(slots=True)
class RawIngestionRecord:
    source_type: str
    source: str
    account_key: str
    endpoint: str
    payload: Any
    request_params: JsonDict = field(default_factory=dict)
    http_status: int | None = None
    account_label: str | None = None
    received_at: datetime = field(default_factory=utc_now)


@dataclass(slots=True)
class PriceRecord:
    asset_uid: str
    price_source: str
    price_usd: Decimal
    quoted_at: datetime
    symbol: str | None = None
    chain: str | None = None
    token_address: str | None = None
    metadata: JsonDict = field(default_factory=dict)


@dataclass(slots=True)
class PositionRecord:
    source_type: str
    source: str
    account_key: str
    account_type: str
    position_kind: str
    asset_uid: str
    collected_at: datetime
    chain: str | None = None
    account_label: str | None = None
    subaccount: str | None = None
    wallet_address: str | None = None
    asset_symbol: str | None = None
    asset_name: str | None = None
    token_address: str | None = None
    amount_raw: Decimal | None = None
    decimals: int | None = None
    amount: Decimal | None = None
    price_usd: Decimal | None = None
    price_source: str | None = None
    price_as_of: datetime | None = None
    usd_value: Decimal | None = None
    is_verified: bool | None = None
    is_spam: bool | None = None
    metadata: JsonDict = field(default_factory=dict)


@dataclass(slots=True)
class SummaryRecord:
    source_type: str
    source: str
    account_key: str
    account_type: str
    metric_code: str
    metric_unit: str
    metric_value: Decimal
    collected_at: datetime
    account_label: str | None = None
    metadata: JsonDict = field(default_factory=dict)


@dataclass(slots=True)
class CollectionResult:
    positions: list[PositionRecord] = field(default_factory=list)
    prices: list[PriceRecord] = field(default_factory=list)
    source_summaries: list[SummaryRecord] = field(default_factory=list)
    raw_ingestions: list[RawIngestionRecord] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def extend(self, other: "CollectionResult") -> None:
        self.positions.extend(other.positions)
        self.prices.extend(other.prices)
        self.source_summaries.extend(other.source_summaries)
        self.raw_ingestions.extend(other.raw_ingestions)
        self.warnings.extend(other.warnings)

