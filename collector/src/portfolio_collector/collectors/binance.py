from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
from typing import Any
from urllib.parse import urlencode

import httpx

from ..config import BinanceConfig
from ..models import CollectionResult, PositionRecord, RawIngestionRecord, SummaryRecord
from ..price_utils import build_price_from_tickers, stablecoin_price
from ..utils import decimal_or_none, decimal_or_zero, utc_now
from .base import Collector


LOG = logging.getLogger(__name__)


def _list_payload(payload: Any, preferred_keys: tuple[str, ...] = ("data", "balances", "rows", "snapshotVos")) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in preferred_keys:
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


def _binance_price_source(asset: str, price: Any) -> str | None:
    if price is None:
        return None
    if stablecoin_price(asset) is not None:
        return "hardcoded:stablecoin"
    return "binance:spot_ticker_price"


class BinanceCollector(Collector):
    name = "binance"

    def __init__(self, config: BinanceConfig, client: httpx.AsyncClient) -> None:
        self._config = config
        self._client = client
        self._subaccount_limit = asyncio.Semaphore(4)
        self._price_map: dict[str, Any] = {}

    async def collect(self) -> CollectionResult:
        result = CollectionResult()
        collected_at = utc_now()
        self._price_map = await self._fetch_price_map()

        permissions, status, params = await self._signed_request("GET", "/sapi/v1/account/apiRestrictions")
        result.raw_ingestions.append(
            RawIngestionRecord(
                source_type="cex",
                source="binance",
                account_key="main",
                account_label="Binance Main",
                endpoint="/sapi/v1/account/apiRestrictions",
                payload=permissions,
                request_params=params,
                http_status=status,
            )
        )

        wallet_balances, status, params = await self._signed_request(
            "GET",
            "/sapi/v1/asset/wallet/balance",
            {"quoteAsset": self._config.quote_asset},
        )
        result.raw_ingestions.append(
            RawIngestionRecord(
                source_type="cex",
                source="binance",
                account_key="main",
                account_label="Binance Main",
                endpoint="/sapi/v1/asset/wallet/balance",
                payload=wallet_balances,
                request_params=params,
                http_status=status,
            )
        )
        for item in _list_payload(wallet_balances, preferred_keys=("data",)):
            balance = decimal_or_none(item.get("balance"))
            if balance is None:
                continue
            result.source_summaries.append(
                SummaryRecord(
                    source_type="cex",
                    source="binance",
                    account_key="main",
                    account_label="Binance Main",
                    account_type=self._wallet_name_to_account_type(item.get("walletName")),
                    metric_code="wallet_balance_quote",
                    metric_unit=self._config.quote_asset,
                    metric_value=balance,
                    collected_at=collected_at,
                    metadata={"active": item.get("activate"), "wallet_name": item.get("walletName")},
                )
            )

        user_assets, status, params = await self._signed_request(
            "POST",
            "/sapi/v3/asset/getUserAsset",
            {"needBtcValuation": "true"},
        )
        result.raw_ingestions.append(
            RawIngestionRecord(
                source_type="cex",
                source="binance",
                account_key="main",
                account_label="Binance Main",
                endpoint="/sapi/v3/asset/getUserAsset",
                payload=user_assets,
                request_params=params,
                http_status=status,
            )
        )
        result.positions.extend(
            self._asset_rows_from_binance_payload(
                _list_payload(user_assets),
                account_key="main",
                account_label="Binance Main",
                account_type="spot",
                collected_at=collected_at,
            )
        )

        funding_assets, status, params = await self._signed_request(
            "POST",
            "/sapi/v1/asset/get-funding-asset",
            {"needBtcValuation": "true"},
        )
        result.raw_ingestions.append(
            RawIngestionRecord(
                source_type="cex",
                source="binance",
                account_key="main",
                account_label="Binance Main",
                endpoint="/sapi/v1/asset/get-funding-asset",
                payload=funding_assets,
                request_params=params,
                http_status=status,
            )
        )
        result.positions.extend(
            self._asset_rows_from_binance_payload(
                _list_payload(funding_assets),
                account_key="main",
                account_label="Binance Main",
                account_type="funding",
                collected_at=collected_at,
            )
        )

        cross_margin, status, params = await self._signed_request("GET", "/sapi/v1/margin/account")
        result.raw_ingestions.append(
            RawIngestionRecord(
                source_type="cex",
                source="binance",
                account_key="main",
                account_label="Binance Main",
                endpoint="/sapi/v1/margin/account",
                payload=cross_margin,
                request_params=params,
                http_status=status,
            )
        )
        result.extend(self._positions_from_cross_margin(cross_margin, collected_at=collected_at))

        isolated_margin, status, params = await self._signed_request("GET", "/sapi/v1/margin/isolated/account")
        result.raw_ingestions.append(
            RawIngestionRecord(
                source_type="cex",
                source="binance",
                account_key="main",
                account_label="Binance Main",
                endpoint="/sapi/v1/margin/isolated/account",
                payload=isolated_margin,
                request_params=params,
                http_status=status,
            )
        )
        result.extend(self._positions_from_isolated_margin(isolated_margin, collected_at=collected_at))

        usdt_futures, status, params = await self._signed_futures_request("GET", "https://fapi.binance.com", "/fapi/v3/account")
        result.raw_ingestions.append(
            RawIngestionRecord(
                source_type="cex",
                source="binance",
                account_key="main",
                account_label="Binance Main",
                endpoint="/fapi/v3/account",
                payload=usdt_futures,
                request_params=params,
                http_status=status,
            )
        )
        result.extend(
            self._positions_from_main_futures_account(
                usdt_futures,
                account_type="usdt_futures",
                collected_at=collected_at,
            )
        )

        coin_futures, status, params = await self._signed_futures_request("GET", "https://dapi.binance.com", "/dapi/v1/account")
        result.raw_ingestions.append(
            RawIngestionRecord(
                source_type="cex",
                source="binance",
                account_key="main",
                account_label="Binance Main",
                endpoint="/dapi/v1/account",
                payload=coin_futures,
                request_params=params,
                http_status=status,
            )
        )
        result.extend(
            self._positions_from_main_futures_account(
                coin_futures,
                account_type="coin_futures",
                collected_at=collected_at,
            )
        )

        if self._config.include_subaccounts:
            subaccounts, status, params = await self._signed_request("GET", "/sapi/v1/sub-account/list")
            result.raw_ingestions.append(
                RawIngestionRecord(
                    source_type="cex",
                    source="binance",
                    account_key="main",
                    account_label="Binance Main",
                    endpoint="/sapi/v1/sub-account/list",
                    payload=subaccounts,
                    request_params=params,
                    http_status=status,
                )
            )
            emails = [item.get("email") for item in subaccounts.get("subAccounts", []) if item.get("email")]
            gathered = await asyncio.gather(*(self._collect_subaccount(email) for email in emails), return_exceptions=True)
            for item in gathered:
                if isinstance(item, Exception):
                    LOG.exception("binance sub-account collection failed", exc_info=item)
                    result.warnings.append(str(item))
                    continue
                result.extend(item)

        return result

    def _positions_from_cross_margin(self, payload: dict[str, Any], *, collected_at) -> CollectionResult:
        result = CollectionResult()
        total_asset_of_btc = decimal_or_none(payload.get("totalAssetOfBtc"))
        if total_asset_of_btc is not None:
            result.source_summaries.append(
                SummaryRecord(
                    source_type="cex",
                    source="binance",
                    account_key="main",
                    account_label="Binance Main",
                    account_type="cross_margin",
                    metric_code="total_asset_btc",
                    metric_unit="BTC",
                    metric_value=total_asset_of_btc,
                    collected_at=collected_at,
                    metadata={},
                )
            )

        for item in payload.get("userAssets", []):
            asset = item.get("asset")
            if not asset:
                continue
            free = decimal_or_zero(item.get("free"))
            borrowed = decimal_or_zero(item.get("borrowed"))
            interest = decimal_or_zero(item.get("interest"))
            locked = decimal_or_zero(item.get("locked"))
            net_asset = decimal_or_none(item.get("netAsset"))
            amount = net_asset if net_asset is not None else (free + locked - borrowed - interest)
            if amount == 0:
                continue
            price_usd = self._price_for_asset(asset)
            result.positions.append(
                PositionRecord(
                    source_type="cex",
                    source="binance",
                    chain=None,
                    account_key="main",
                    account_label="Binance Main",
                    account_type="cross_margin",
                    subaccount=None,
                    wallet_address=None,
                    position_kind="cex_asset",
                    asset_uid=f"cex:binance:cross_margin:{asset}",
                    asset_symbol=asset,
                    asset_name=asset,
                    token_address=None,
                    amount_raw=None,
                    decimals=None,
                    amount=amount,
                    price_usd=price_usd,
                    price_source=_binance_price_source(asset, price_usd),
                    price_as_of=collected_at if price_usd is not None else None,
                    usd_value=(amount * price_usd) if price_usd is not None else None,
                    is_verified=True,
                    is_spam=False,
                    metadata={
                        "free": str(free),
                        "borrowed": str(borrowed),
                        "interest": str(interest),
                        "locked": str(locked),
                        "net_asset": item.get("netAsset"),
                    },
                    collected_at=collected_at,
                )
            )
        return result

    def _positions_from_isolated_margin(self, payload: dict[str, Any], *, collected_at) -> CollectionResult:
        result = CollectionResult()
        for asset_group in payload.get("assets", []):
            symbol = asset_group.get("symbol")
            for side_name in ("baseAsset", "quoteAsset"):
                side = asset_group.get(side_name) or {}
                asset = side.get("asset")
                if not asset:
                    continue
                net_asset = decimal_or_none(side.get("netAsset"))
                if net_asset is None or net_asset == 0:
                    continue
                price_usd = self._price_for_asset(asset)
                result.positions.append(
                    PositionRecord(
                        source_type="cex",
                        source="binance",
                        chain=None,
                        account_key="main",
                        account_label="Binance Main",
                        account_type="isolated_margin",
                        subaccount=symbol,
                        wallet_address=None,
                        position_kind="cex_asset",
                        asset_uid=f"cex:binance:isolated_margin:{symbol}:{asset}",
                        asset_symbol=asset,
                        asset_name=asset,
                        token_address=None,
                        amount_raw=None,
                        decimals=None,
                        amount=net_asset,
                        price_usd=price_usd,
                        price_source=_binance_price_source(asset, price_usd),
                        price_as_of=collected_at if price_usd is not None else None,
                        usd_value=(net_asset * price_usd) if price_usd is not None else None,
                        is_verified=True,
                        is_spam=False,
                        metadata={
                            "symbol": symbol,
                            "borrowed": side.get("borrowed"),
                            "interest": side.get("interest"),
                            "free": side.get("free"),
                            "locked": side.get("locked"),
                        },
                        collected_at=collected_at,
                    )
                )
        return result

    def _positions_from_main_futures_account(
        self,
        payload: dict[str, Any],
        *,
        account_type: str,
        collected_at,
    ) -> CollectionResult:
        result = CollectionResult()
        total_wallet_balance = decimal_or_none(payload.get("totalWalletBalance"))
        if total_wallet_balance is not None:
            result.source_summaries.append(
                SummaryRecord(
                    source_type="cex",
                    source="binance",
                    account_key="main",
                    account_label="Binance Main",
                    account_type=account_type,
                    metric_code="total_wallet_balance",
                    metric_unit="USD" if account_type == "usdt_futures" else "mixed",
                    metric_value=total_wallet_balance,
                    collected_at=collected_at,
                    metadata={},
                )
            )

        for item in payload.get("assets", []):
            asset = item.get("asset")
            wallet_balance = decimal_or_none(item.get("walletBalance"))
            if not asset or wallet_balance is None or wallet_balance == 0:
                continue
            price_usd = self._price_for_asset(asset)
            result.positions.append(
                PositionRecord(
                    source_type="cex",
                    source="binance",
                    chain=None,
                    account_key="main",
                    account_label="Binance Main",
                    account_type=account_type,
                    subaccount=None,
                    wallet_address=None,
                    position_kind="cex_asset",
                    asset_uid=f"cex:binance:{account_type}:{asset}",
                    asset_symbol=asset,
                    asset_name=asset,
                    token_address=None,
                    amount_raw=None,
                    decimals=None,
                    amount=wallet_balance,
                    price_usd=price_usd,
                    price_source=_binance_price_source(asset, price_usd),
                    price_as_of=collected_at if price_usd is not None else None,
                    usd_value=(wallet_balance * price_usd) if price_usd is not None else None,
                    is_verified=True,
                    is_spam=False,
                    metadata={
                        "initial_margin": item.get("initialMargin"),
                        "unrealized_profit": item.get("unrealizedProfit"),
                        "margin_balance": item.get("marginBalance"),
                        "max_withdraw_amount": item.get("maxWithdrawAmount"),
                    },
                    collected_at=collected_at,
                )
            )
        return result

    async def _collect_subaccount(self, email: str) -> CollectionResult:
        async with self._subaccount_limit:
            result = CollectionResult()
            collected_at = utc_now()

            spot_assets, status, params = await self._signed_request(
                "GET",
                "/sapi/v4/sub-account/assets",
                {"email": email},
            )
            result.raw_ingestions.append(
                RawIngestionRecord(
                    source_type="cex",
                    source="binance",
                    account_key=email,
                    account_label=email,
                    endpoint="/sapi/v4/sub-account/assets",
                    payload=spot_assets,
                    request_params=params,
                    http_status=status,
                )
            )
            result.positions.extend(
                self._asset_rows_from_binance_payload(
                    _list_payload(spot_assets, preferred_keys=("balances", "data")),
                    account_key=email,
                    account_label=email,
                    account_type="subaccount_spot",
                    collected_at=collected_at,
                    subaccount=email,
                )
            )

            for futures_type, account_type in ((1, "subaccount_usdt_futures"), (2, "subaccount_coin_futures")):
                futures_account, status, params = await self._signed_request(
                    "GET",
                    "/sapi/v2/sub-account/futures/account",
                    {"email": email, "futuresType": futures_type},
                )
                result.raw_ingestions.append(
                    RawIngestionRecord(
                        source_type="cex",
                        source="binance",
                        account_key=email,
                        account_label=email,
                        endpoint="/sapi/v2/sub-account/futures/account",
                        payload=futures_account,
                        request_params=params,
                        http_status=status,
                    )
                )
                result.extend(
                    self._positions_from_futures_account(
                        futures_account,
                        account_key=email,
                        account_label=email,
                        account_type=account_type,
                        subaccount=email,
                        collected_at=collected_at,
                    )
                )

            return result

    def _asset_rows_from_binance_payload(
        self,
        payload: list[dict[str, Any]],
        *,
        account_key: str,
        account_label: str,
        account_type: str,
        collected_at,
        subaccount: str | None = None,
    ) -> list[PositionRecord]:
        rows: list[PositionRecord] = []
        for item in payload:
            asset = item.get("asset")
            if not asset:
                continue
            free = decimal_or_zero(item.get("free"))
            locked = decimal_or_zero(item.get("locked"))
            freeze = decimal_or_zero(item.get("freeze"))
            withdrawing = decimal_or_zero(item.get("withdrawing"))
            ipoable = decimal_or_zero(item.get("ipoable"))
            amount = free + locked + freeze + withdrawing + ipoable
            if amount == 0:
                continue

            price_usd = self._price_for_asset(asset)
            usd_value = amount * price_usd if price_usd is not None else None
            rows.append(
                PositionRecord(
                    source_type="cex",
                    source="binance",
                    chain=None,
                    account_key=account_key,
                    account_label=account_label,
                    account_type=account_type,
                    subaccount=subaccount,
                    wallet_address=None,
                    position_kind="cex_asset",
                    asset_uid=f"cex:binance:{account_type}:{asset}",
                    asset_symbol=asset,
                    asset_name=asset,
                    token_address=None,
                    amount_raw=None,
                    decimals=None,
                    amount=amount,
                    price_usd=price_usd,
                    price_source=_binance_price_source(asset, price_usd),
                    price_as_of=collected_at if price_usd is not None else None,
                    usd_value=usd_value,
                    is_verified=True,
                    is_spam=False,
                    metadata={
                        "free": str(free),
                        "locked": str(locked),
                        "freeze": str(freeze),
                        "withdrawing": str(withdrawing),
                        "ipoable": str(ipoable),
                        "btc_valuation": item.get("btcValuation"),
                    },
                    collected_at=collected_at,
                )
            )
        return rows

    def _positions_from_futures_account(
        self,
        payload: dict[str, Any],
        *,
        account_key: str,
        account_label: str,
        account_type: str,
        subaccount: str,
        collected_at,
    ) -> CollectionResult:
        result = CollectionResult()
        data = payload.get("futureAccountResp") or payload.get("deliveryAccountResp")
        if not data:
            return result

        total_wallet = decimal_or_none(data.get("totalWalletBalance"))
        if total_wallet is not None:
            unit = "USD" if account_type == "subaccount_usdt_futures" else str(data.get("assets", [{}])[0].get("asset", "BTC"))
            result.source_summaries.append(
                SummaryRecord(
                    source_type="cex",
                    source="binance",
                    account_key=account_key,
                    account_label=account_label,
                    account_type=account_type,
                    metric_code="total_wallet_balance",
                    metric_unit=unit,
                    metric_value=total_wallet,
                    collected_at=collected_at,
                    metadata={"subaccount": subaccount},
                )
            )

        for item in data.get("assets", []):
            asset = item.get("asset")
            wallet_balance = decimal_or_none(item.get("walletBalance"))
            if not asset or wallet_balance is None or wallet_balance == 0:
                continue
            price_usd = self._price_for_asset(asset)
            result.positions.append(
                PositionRecord(
                    source_type="cex",
                    source="binance",
                    chain=None,
                    account_key=account_key,
                    account_label=account_label,
                    account_type=account_type,
                    subaccount=subaccount,
                    wallet_address=None,
                    position_kind="cex_asset",
                    asset_uid=f"cex:binance:{account_type}:{asset}",
                    asset_symbol=asset,
                    asset_name=asset,
                    token_address=None,
                    amount_raw=None,
                    decimals=None,
                    amount=wallet_balance,
                    price_usd=price_usd,
                    price_source=_binance_price_source(asset, price_usd),
                    price_as_of=collected_at if price_usd is not None else None,
                    usd_value=(wallet_balance * price_usd) if price_usd is not None else None,
                    is_verified=True,
                    is_spam=False,
                    metadata={
                        "initial_margin": item.get("initialMargin"),
                        "maintenance_margin": item.get("maintenanceMargin"),
                        "margin_balance": item.get("marginBalance"),
                        "max_withdraw_amount": item.get("maxWithdrawAmount"),
                        "unrealized_profit": item.get("unrealizedProfit"),
                    },
                    collected_at=collected_at,
                )
            )
        return result

    async def _signed_request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> tuple[Any, int, dict[str, Any]]:
        prepared: dict[str, Any] = {}
        if params:
            prepared.update(params)
        prepared["timestamp"] = str(int(utc_now().timestamp() * 1000))
        query = urlencode(prepared, doseq=True, safe="@")
        signature = hmac.new(
            self._config.api_secret.encode(),
            query.encode(),
            hashlib.sha256,
        ).hexdigest()
        url = f"{self._config.base_url}{path}?{query}&signature={signature}"
        response = await self._client.request(
            method,
            url,
            headers={"X-MBX-APIKEY": self._config.api_key},
        )
        response.raise_for_status()
        return response.json(), response.status_code, prepared

    async def _fetch_price_map(self) -> dict[str, Any]:
        response = await self._client.get(f"{self._config.base_url}/api/v3/ticker/price")
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list):
            return {}
        return build_price_from_tickers(payload, symbol_key="symbol", price_key="price")

    def _price_for_asset(self, asset: str) -> Any:
        stable = stablecoin_price(asset)
        if stable is not None:
            return stable
        return self._price_map.get(asset.upper())

    async def _signed_futures_request(
        self,
        method: str,
        base_url: str,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> tuple[Any, int, dict[str, Any]]:
        prepared: dict[str, Any] = {}
        if params:
            prepared.update(params)
        prepared["timestamp"] = str(int(utc_now().timestamp() * 1000))
        query = urlencode(prepared, doseq=True, safe="@")
        signature = hmac.new(
            self._config.api_secret.encode(),
            query.encode(),
            hashlib.sha256,
        ).hexdigest()
        url = f"{base_url}{path}?{query}&signature={signature}"
        response = await self._client.request(
            method,
            url,
            headers={"X-MBX-APIKEY": self._config.api_key},
        )
        response.raise_for_status()
        return response.json(), response.status_code, prepared

    @staticmethod
    def _wallet_name_to_account_type(value: str | None) -> str:
        if not value:
            return "unknown"
        return value.strip().lower().replace(" ", "_").replace("Ⓢ", "s")
