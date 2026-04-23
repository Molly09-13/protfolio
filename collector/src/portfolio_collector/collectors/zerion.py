from __future__ import annotations

import asyncio
import base64
import logging
from typing import Any

import httpx

from ..config import ZerionConfig, ZerionWalletConfig
from ..models import CollectionResult, PositionRecord, RawIngestionRecord, SummaryRecord
from ..utils import decimal_or_none, utc_now
from .base import Collector


LOG = logging.getLogger(__name__)


def _zerion_value(attributes: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = attributes.get(key)
        if value is not None:
            return value
    return None


def _flatten_included(included: list[dict[str, Any]] | None) -> dict[str, dict[str, Any]]:
    flattened: dict[str, dict[str, Any]] = {}
    for item in included or []:
        item_type = item.get("type")
        item_id = item.get("id")
        if item_type and item_id:
            flattened[f"{item_type}:{item_id}"] = item
    return flattened


def _first_non_empty(*values: Any) -> str | None:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _name_from_meta(meta: Any) -> str | None:
    if isinstance(meta, dict):
        return _first_non_empty(meta.get("name"), meta.get("symbol"), meta.get("id"))
    return _first_non_empty(meta)


class ZerionCollector(Collector):
    name = "zerion"

    def __init__(self, config: ZerionConfig, wallets: list[ZerionWalletConfig], client: httpx.AsyncClient) -> None:
        self._config = config
        self._wallets = wallets
        self._client = client
        self._limit = asyncio.Semaphore(1)
        self._request_lock = asyncio.Lock()
        self._min_interval_seconds = 1.0
        self._max_retries = 4

    async def collect(self) -> CollectionResult:
        result = CollectionResult()
        gathered = await asyncio.gather(*(self._collect_wallet(wallet) for wallet in self._wallets), return_exceptions=True)
        for item in gathered:
            if isinstance(item, Exception):
                LOG.exception("zerion collector task failed", exc_info=item)
                result.warnings.append(str(item))
                continue
            result.extend(item)
        return result

    async def _collect_wallet(self, wallet: ZerionWalletConfig) -> CollectionResult:
        async with self._limit:
            result = CollectionResult()
            account_key = wallet.label or wallet.address
            collected_at = utc_now()

            portfolio_params = {
                "currency": "usd",
                "filter[positions]": "no_filter",
            }
            if self._config.sync:
                portfolio_params["sync"] = "true"

            portfolio, status = await self._get(f"/v1/wallets/{wallet.address}/portfolio", portfolio_params)
            result.raw_ingestions.append(
                RawIngestionRecord(
                    source_type="onchain",
                    source="zerion",
                    account_key=account_key,
                    account_label=wallet.label,
                    endpoint=f"/v1/wallets/{wallet.address}/portfolio",
                    payload=portfolio,
                    request_params=portfolio_params,
                    http_status=status,
                )
            )
            result.source_summaries.extend(self._summaries_from_portfolio(wallet, portfolio, collected_at))

            positions_params = {
                "currency": "usd",
                "filter[positions]": "only_simple" if self._is_solana_address(wallet.address) else "no_filter",
                "filter[trash]": "only_non_trash",
                "sort": "-value",
                "page[size]": "100",
            }
            if self._config.sync:
                positions_params["sync"] = "true"

            page_url: str | None = f"/v1/wallets/{wallet.address}/positions/"
            page_params: dict[str, str] | None = positions_params
            while page_url:
                positions, status = await self._get(page_url, page_params)
                result.raw_ingestions.append(
                    RawIngestionRecord(
                        source_type="onchain",
                        source="zerion",
                        account_key=account_key,
                        account_label=wallet.label,
                        endpoint=page_url,
                        payload=positions,
                        request_params=page_params or {},
                        http_status=status,
                    )
                )
                result.positions.extend(self._positions_from_payload(wallet, positions, collected_at))
                next_link = (positions.get("links") or {}).get("next") if isinstance(positions, dict) else None
                page_url = self._path_from_next_link(next_link)
                page_params = None

            return result

    def _summaries_from_portfolio(
        self,
        wallet: ZerionWalletConfig,
        payload: dict[str, Any],
        collected_at,
    ) -> list[SummaryRecord]:
        rows: list[SummaryRecord] = []
        account_key = wallet.label or wallet.address
        data = payload.get("data") or {}
        attributes = data.get("attributes") or {}

        total_value = decimal_or_none(_zerion_value(attributes, "total", "value", "total_value"))
        if total_value is not None:
            rows.append(
                SummaryRecord(
                    source_type="onchain",
                    source="zerion",
                    account_key=account_key,
                    account_label=wallet.label,
                    account_type="wallet",
                    metric_code="total_usd_value",
                    metric_unit="USD",
                    metric_value=total_value,
                    collected_at=collected_at,
                    metadata={"wallet_address": wallet.address},
                )
            )

        positions_distribution = attributes.get("positions_distribution_by_type") or attributes.get("positions_distribution") or {}
        if isinstance(positions_distribution, dict):
            for position_type, raw_value in positions_distribution.items():
                value = decimal_or_none(raw_value)
                if value is None:
                    continue
                rows.append(
                    SummaryRecord(
                        source_type="onchain",
                        source="zerion",
                        account_key=account_key,
                        account_label=wallet.label,
                        account_type=str(position_type),
                        metric_code="position_type_usd_value",
                        metric_unit="USD",
                        metric_value=value,
                        collected_at=collected_at,
                        metadata={"wallet_address": wallet.address},
                    )
                )

        chain_distribution = attributes.get("positions_distribution_by_chain") or {}
        if isinstance(chain_distribution, dict):
            for chain, raw_value in chain_distribution.items():
                value = decimal_or_none(raw_value)
                if value is None:
                    continue
                rows.append(
                    SummaryRecord(
                        source_type="onchain",
                        source="zerion",
                        account_key=account_key,
                        account_label=wallet.label,
                        account_type="wallet_chain",
                        metric_code="chain_usd_value",
                        metric_unit="USD",
                        metric_value=value,
                        collected_at=collected_at,
                        metadata={"wallet_address": wallet.address, "chain": chain},
                    )
                )
        return rows

    def _positions_from_payload(
        self,
        wallet: ZerionWalletConfig,
        payload: dict[str, Any],
        collected_at,
    ) -> list[PositionRecord]:
        account_key = wallet.label or wallet.address
        included = _flatten_included(payload.get("included"))
        rows: list[PositionRecord] = []
        data = payload.get("data") or []
        if not isinstance(data, list):
            return rows

        for item in data:
            attributes = item.get("attributes") or {}
            relationships = item.get("relationships") or {}
            fungible_ref = ((relationships.get("fungible") or {}).get("data") or {})
            chain_ref = ((relationships.get("chain") or {}).get("data") or {})
            fungible = included.get(f"{fungible_ref.get('type')}:{fungible_ref.get('id')}", {})
            chain_item = included.get(f"{chain_ref.get('type')}:{chain_ref.get('id')}", {})
            fungible_attrs = fungible.get("attributes") or {}
            chain_attrs = chain_item.get("attributes") or {}
            embedded_fungible = attributes.get("fungible_info") or {}
            protocol_meta = attributes.get("protocol")
            app_meta = attributes.get("app") or attributes.get("application_metadata")

            quantity = attributes.get("quantity") or {}
            amount = decimal_or_none(quantity.get("float") or quantity.get("numeric") or quantity.get("value"))
            price_usd = decimal_or_none(attributes.get("price"))
            usd_value = decimal_or_none(_zerion_value(attributes, "value", "usd_value"))
            position_type = str(attributes.get("position_type") or attributes.get("type") or "wallet")
            chain = chain_attrs.get("id") or chain_item.get("id") or chain_ref.get("id")
            symbol = _first_non_empty(
                embedded_fungible.get("symbol"),
                fungible_attrs.get("symbol"),
                attributes.get("symbol"),
                attributes.get("display_symbol"),
                _name_from_meta(protocol_meta),
                _name_from_meta(app_meta),
            )
            name = _first_non_empty(
                None if attributes.get("name") == "Asset" else attributes.get("name"),
                attributes.get("display_name"),
                embedded_fungible.get("name"),
                fungible_attrs.get("name"),
                embedded_fungible.get("symbol"),
                fungible_attrs.get("symbol"),
                _name_from_meta(protocol_meta),
                _name_from_meta(app_meta),
                symbol,
                item.get("id"),
            )

            rows.append(
                PositionRecord(
                    source_type="onchain",
                    source="zerion",
                    chain=str(chain) if chain else None,
                    account_key=account_key,
                    account_label=wallet.label,
                    account_type="wallet_defi" if position_type != "wallet" else "wallet",
                    wallet_address=wallet.address,
                    position_kind="defi" if position_type != "wallet" else "token",
                    asset_uid=f"zerion:{item.get('id')}",
                    asset_symbol=symbol,
                    asset_name=name,
                    token_address=fungible_ref.get("id"),
                    amount_raw=decimal_or_none(quantity.get("int")),
                    decimals=None,
                    amount=amount,
                    price_usd=price_usd,
                    price_source="zerion:positions" if price_usd is not None else None,
                    price_as_of=collected_at if price_usd is not None else None,
                    usd_value=usd_value,
                    is_verified=None,
                    is_spam=False,
                    metadata={
                        "position_type": position_type,
                        "wallet_address": wallet.address,
                        "embedded_fungible": embedded_fungible,
                        "fungible": fungible_attrs,
                        "chain": chain_attrs,
                        "attributes": attributes,
                    },
                    collected_at=collected_at,
                )
            )
        return rows

    async def _get(self, path_or_url: str, params: dict[str, str] | None = None) -> tuple[dict[str, Any], int]:
        url = path_or_url if path_or_url.startswith("http") else f"{self._config.base_url}{path_or_url}"
        last_exc: Exception | None = None

        for attempt in range(self._max_retries + 1):
            async with self._request_lock:
                response = await self._client.get(
                    url,
                    params=params,
                    headers={
                        "Authorization": f"Basic {self._basic_token()}",
                        "accept": "application/json",
                    },
                )
                await asyncio.sleep(self._min_interval_seconds)

            if response.status_code != 429:
                response.raise_for_status()
                return response.json(), response.status_code

            retry_after = response.headers.get("Retry-After")
            wait_seconds = self._retry_wait_seconds(retry_after, attempt)
            LOG.warning("zerion rate limited for url=%s, retry in %.1fs (attempt %s/%s)", url, wait_seconds, attempt + 1, self._max_retries + 1)
            last_exc = httpx.HTTPStatusError(
                f"Client error '429 Too Many Requests' for url '{url}'",
                request=response.request,
                response=response,
            )
            if attempt >= self._max_retries:
                break
            await asyncio.sleep(wait_seconds)

        if last_exc is not None:
            raise last_exc
        raise RuntimeError("zerion request failed without exception")

    def _basic_token(self) -> str:
        return base64.b64encode(f"{self._config.api_key}:".encode()).decode()

    def _is_solana_address(self, address: str) -> bool:
        return not address.lower().startswith("0x")

    def _path_from_next_link(self, next_link: str | None) -> str | None:
        if not next_link:
            return None
        if next_link.startswith(self._config.base_url):
            return next_link
        if next_link.startswith("/"):
            return next_link
        return None

    def _retry_wait_seconds(self, retry_after: str | None, attempt: int) -> float:
        if retry_after:
            try:
                return max(float(retry_after), self._min_interval_seconds)
            except ValueError:
                pass
        return max(self._min_interval_seconds, 2 ** attempt)
