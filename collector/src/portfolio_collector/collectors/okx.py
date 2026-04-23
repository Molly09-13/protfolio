from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import logging
from typing import Any
from urllib.parse import urlencode

import httpx

from ..config import OkxConfig
from ..models import CollectionResult, PositionRecord, RawIngestionRecord, SummaryRecord
from ..price_utils import build_price_from_tickers, stablecoin_price
from ..utils import decimal_or_none, okx_timestamp, utc_now
from .base import Collector


LOG = logging.getLogger(__name__)


def _list_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
    return []


def _okx_price_source(asset: str, price: Any, *, derived_from_equity: bool = False) -> str | None:
    if price is None:
        return None
    if derived_from_equity:
        return "okx:eqUsd/eq"
    if stablecoin_price(asset) is not None:
        return "hardcoded:stablecoin"
    return "okx:market_tickers"


class OkxCollector(Collector):
    name = "okx"

    def __init__(self, config: OkxConfig, client: httpx.AsyncClient) -> None:
        self._config = config
        self._client = client
        self._subaccount_limit = asyncio.Semaphore(4)
        self._price_map: dict[str, Any] = {}

    async def collect(self) -> CollectionResult:
        result = CollectionResult()
        collected_at = utc_now()
        self._price_map = await self._fetch_price_map()

        trading, status, params = await self._signed_request("GET", "/api/v5/account/balance")
        result.raw_ingestions.append(
            RawIngestionRecord(
                source_type="cex",
                source="okx",
                account_key="main",
                account_label="OKX Main",
                endpoint="/api/v5/account/balance",
                payload=trading,
                request_params=params,
                http_status=status,
            )
        )
        result.extend(
            self._positions_from_trading_balance(
                trading,
                account_key="main",
                account_label="OKX Main",
                account_type="trading",
                collected_at=collected_at,
            )
        )

        funding, status, params = await self._signed_request("GET", "/api/v5/asset/balances")
        result.raw_ingestions.append(
            RawIngestionRecord(
                source_type="cex",
                source="okx",
                account_key="main",
                account_label="OKX Main",
                endpoint="/api/v5/asset/balances",
                payload=funding,
                request_params=params,
                http_status=status,
            )
        )
        result.positions.extend(
            self._positions_from_funding_balance(
                funding,
                account_key="main",
                account_label="OKX Main",
                account_type="funding",
                collected_at=collected_at,
            )
        )

        valuation, status, params = await self._signed_request(
            "GET",
            "/api/v5/asset/asset-valuation",
            {"ccy": self._config.valuation_ccy},
        )
        result.raw_ingestions.append(
            RawIngestionRecord(
                source_type="cex",
                source="okx",
                account_key="main",
                account_label="OKX Main",
                endpoint="/api/v5/asset/asset-valuation",
                payload=valuation,
                request_params=params,
                http_status=status,
            )
        )
        result.source_summaries.extend(self._summaries_from_valuation(valuation, collected_at))

        positions, status, params = await self._signed_request("GET", "/api/v5/account/positions")
        result.raw_ingestions.append(
            RawIngestionRecord(
                source_type="cex",
                source="okx",
                account_key="main",
                account_label="OKX Main",
                endpoint="/api/v5/account/positions",
                payload=positions,
                request_params=params,
                http_status=status,
            )
        )
        result.positions.extend(
            self._positions_from_positions_endpoint(
                positions,
                account_key="main",
                account_label="OKX Main",
                account_type="positions",
                collected_at=collected_at,
            )
        )

        if self._config.include_subaccounts:
            subaccounts, status, params = await self._signed_request("GET", "/api/v5/users/subaccount/list")
            result.raw_ingestions.append(
                RawIngestionRecord(
                    source_type="cex",
                    source="okx",
                    account_key="main",
                    account_label="OKX Main",
                    endpoint="/api/v5/users/subaccount/list",
                    payload=subaccounts,
                    request_params=params,
                    http_status=status,
                )
            )
            names = [item.get("subAcct") for item in subaccounts.get("data", []) if item.get("subAcct")]
            gathered = await asyncio.gather(*(self._collect_subaccount(name) for name in names), return_exceptions=True)
            for item in gathered:
                if isinstance(item, Exception):
                    LOG.exception("okx sub-account collection failed", exc_info=item)
                    result.warnings.append(str(item))
                    continue
                result.extend(item)

        return result

    async def _collect_subaccount(self, subaccount: str) -> CollectionResult:
        async with self._subaccount_limit:
            result = CollectionResult()
            collected_at = utc_now()

            trading, status, params = await self._signed_request(
                "GET",
                "/api/v5/account/subaccount/balances",
                {"subAcct": subaccount},
            )
            result.raw_ingestions.append(
                RawIngestionRecord(
                    source_type="cex",
                    source="okx",
                    account_key=subaccount,
                    account_label=subaccount,
                    endpoint="/api/v5/account/subaccount/balances",
                    payload=trading,
                    request_params=params,
                    http_status=status,
                )
            )
            result.extend(
                self._positions_from_trading_balance(
                    trading,
                    account_key=subaccount,
                    account_label=subaccount,
                    account_type="subaccount_trading",
                    collected_at=collected_at,
                    subaccount=subaccount,
                )
            )

            funding, status, params = await self._signed_request(
                "GET",
                "/api/v5/asset/subaccount/balances",
                {"subAcct": subaccount},
            )
            result.raw_ingestions.append(
                RawIngestionRecord(
                    source_type="cex",
                    source="okx",
                    account_key=subaccount,
                    account_label=subaccount,
                    endpoint="/api/v5/asset/subaccount/balances",
                    payload=funding,
                    request_params=params,
                    http_status=status,
                )
            )
            result.positions.extend(
                self._positions_from_funding_balance(
                    funding,
                    account_key=subaccount,
                    account_label=subaccount,
                    account_type="subaccount_funding",
                    collected_at=collected_at,
                    subaccount=subaccount,
                )
            )

            return result

    def _positions_from_trading_balance(
        self,
        payload: dict[str, Any],
        *,
        account_key: str,
        account_label: str,
        account_type: str,
        collected_at,
        subaccount: str | None = None,
    ) -> CollectionResult:
        result = CollectionResult()
        data = _list_payload(payload)
        if not data:
            return result
        account = data[0]

        total_eq = decimal_or_none(account.get("totalEq"))
        if total_eq is not None:
            result.source_summaries.append(
                SummaryRecord(
                    source_type="cex",
                    source="okx",
                    account_key=account_key,
                    account_label=account_label,
                    account_type=account_type,
                    metric_code="total_equity",
                    metric_unit="USD",
                    metric_value=total_eq,
                    collected_at=collected_at,
                    metadata={"subaccount": subaccount},
                )
            )

        for item in account.get("details", []):
            ccy = item.get("ccy")
            eq = decimal_or_none(item.get("eq"))
            if not ccy or eq is None or eq == 0:
                continue
            eq_usd = decimal_or_none(item.get("eqUsd"))
            derived_from_equity = eq_usd is not None and eq != 0
            price_usd = (eq_usd / eq) if derived_from_equity else self._price_for_asset(ccy)
            if eq_usd is None and price_usd is not None:
                eq_usd = eq * price_usd
            result.positions.append(
                PositionRecord(
                    source_type="cex",
                    source="okx",
                    chain=None,
                    account_key=account_key,
                    account_label=account_label,
                    account_type=account_type,
                    subaccount=subaccount,
                    wallet_address=None,
                    position_kind="cex_asset",
                    asset_uid=f"cex:okx:{account_type}:{ccy}",
                    asset_symbol=ccy,
                    asset_name=ccy,
                    token_address=None,
                    amount_raw=None,
                    decimals=None,
                    amount=eq,
                    price_usd=price_usd,
                    price_source=_okx_price_source(ccy, price_usd, derived_from_equity=derived_from_equity),
                    price_as_of=collected_at if price_usd is not None else None,
                    usd_value=eq_usd,
                    is_verified=True,
                    is_spam=False,
                    metadata={
                        "avail_bal": item.get("availBal"),
                        "cash_bal": item.get("cashBal"),
                        "frozen_bal": item.get("frozenBal"),
                        "liab": item.get("liab"),
                        "upl": item.get("upl"),
                    },
                    collected_at=collected_at,
                )
            )
        return result

    def _positions_from_funding_balance(
        self,
        payload: dict[str, Any],
        *,
        account_key: str,
        account_label: str,
        account_type: str,
        collected_at,
        subaccount: str | None = None,
    ) -> list[PositionRecord]:
        rows: list[PositionRecord] = []
        for item in _list_payload(payload):
            ccy = item.get("ccy")
            bal = decimal_or_none(item.get("bal"))
            if not ccy or bal is None or bal == 0:
                continue
            price_usd = self._price_for_asset(ccy)
            rows.append(
                PositionRecord(
                    source_type="cex",
                    source="okx",
                    chain=None,
                    account_key=account_key,
                    account_label=account_label,
                    account_type=account_type,
                    subaccount=subaccount,
                    wallet_address=None,
                    position_kind="cex_asset",
                    asset_uid=f"cex:okx:{account_type}:{ccy}",
                    asset_symbol=ccy,
                    asset_name=ccy,
                    token_address=None,
                    amount_raw=None,
                    decimals=None,
                    amount=bal,
                    price_usd=price_usd,
                    price_source=_okx_price_source(ccy, price_usd),
                    price_as_of=collected_at if price_usd is not None else None,
                    usd_value=(bal * price_usd) if price_usd is not None else None,
                    is_verified=True,
                    is_spam=False,
                    metadata={
                        "avail_bal": item.get("availBal"),
                        "frozen_bal": item.get("frozenBal"),
                    },
                    collected_at=collected_at,
                )
            )
        return rows

    def _summaries_from_valuation(self, payload: dict[str, Any], collected_at) -> list[SummaryRecord]:
        rows: list[SummaryRecord] = []
        for item in _list_payload(payload):
            total = decimal_or_none(item.get("totalBal"))
            if total is not None:
                rows.append(
                    SummaryRecord(
                        source_type="cex",
                        source="okx",
                        account_key="main",
                        account_label="OKX Main",
                        account_type="portfolio",
                        metric_code="total_valuation",
                        metric_unit=self._config.valuation_ccy,
                        metric_value=total,
                        collected_at=collected_at,
                        metadata={},
                    )
                )
            for account_type, raw_value in (item.get("details") or {}).items():
                value = decimal_or_none(raw_value)
                if value is None:
                    continue
                rows.append(
                    SummaryRecord(
                        source_type="cex",
                        source="okx",
                        account_key="main",
                        account_label="OKX Main",
                        account_type=account_type,
                        metric_code="account_valuation",
                        metric_unit=self._config.valuation_ccy,
                        metric_value=value,
                        collected_at=collected_at,
                        metadata={},
                    )
                )
        return rows

    def _positions_from_positions_endpoint(
        self,
        payload: dict[str, Any],
        *,
        account_key: str,
        account_label: str,
        account_type: str,
        collected_at,
    ) -> list[PositionRecord]:
        rows: list[PositionRecord] = []
        for item in _list_payload(payload):
            inst_id = item.get("instId")
            pos = decimal_or_none(item.get("pos"))
            if not inst_id or pos is None or pos == 0:
                continue
            upl = decimal_or_none(item.get("upl"))
            rows.append(
                PositionRecord(
                    source_type="cex",
                    source="okx",
                    chain=None,
                    account_key=account_key,
                    account_label=account_label,
                    account_type=account_type,
                    subaccount=None,
                    wallet_address=None,
                    position_kind="cex_position",
                    asset_uid=f"cex:okx:{account_type}:{inst_id}:{item.get('posSide') or 'net'}",
                    asset_symbol=inst_id,
                    asset_name=inst_id,
                    token_address=None,
                    amount_raw=None,
                    decimals=None,
                    amount=pos,
                    price_usd=None,
                    price_source=None,
                    price_as_of=None,
                    usd_value=upl,
                    is_verified=True,
                    is_spam=False,
                    metadata={
                        "inst_type": item.get("instType"),
                        "ccy": item.get("ccy"),
                        "pos_side": item.get("posSide"),
                        "avg_px": item.get("avgPx"),
                        "mark_px": item.get("markPx"),
                        "liq_px": item.get("liqPx"),
                        "margin_mode": item.get("mgnMode"),
                        "upl_ratio": item.get("uplRatio"),
                    },
                    collected_at=collected_at,
                )
            )
        return rows

    async def _signed_request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> tuple[Any, int, dict[str, Any]]:
        prepared = params or {}
        query = urlencode(prepared, doseq=True)
        request_path = path if not query else f"{path}?{query}"
        timestamp = okx_timestamp()
        prehash = f"{timestamp}{method.upper()}{request_path}"
        sign = base64.b64encode(
            hmac.new(
                self._config.api_secret.encode(),
                prehash.encode(),
                hashlib.sha256,
            ).digest()
        ).decode()
        response = await self._client.request(
            method,
            f"{self._config.base_url}{request_path}",
            headers={
                "OK-ACCESS-KEY": self._config.api_key,
                "OK-ACCESS-SIGN": sign,
                "OK-ACCESS-TIMESTAMP": timestamp,
                "OK-ACCESS-PASSPHRASE": self._config.api_passphrase,
            },
        )
        response.raise_for_status()
        return response.json(), response.status_code, prepared

    async def _fetch_price_map(self) -> dict[str, Any]:
        response = await self._client.get(f"{self._config.base_url}/api/v5/market/tickers", params={"instType": "SPOT"})
        response.raise_for_status()
        payload = response.json()
        tickers = payload.get("data") if isinstance(payload, dict) else []
        if not isinstance(tickers, list):
            return {}
        normalized = []
        for ticker in tickers:
            inst_id = str(ticker.get("instId") or "").replace("-", "")
            normalized.append({"instId": inst_id, "last": ticker.get("last")})
        return build_price_from_tickers(normalized, symbol_key="instId", price_key="last")

    def _price_for_asset(self, asset: str) -> Any:
        stable = stablecoin_price(asset)
        if stable is not None:
            return stable
        return self._price_map.get(asset.upper())
