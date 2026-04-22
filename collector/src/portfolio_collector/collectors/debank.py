from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from ..config import DebankConfig, EvmWalletConfig
from ..models import CollectionResult, PositionRecord, RawIngestionRecord, SummaryRecord
from ..utils import decimal_or_none, utc_now
from .base import Collector


LOG = logging.getLogger(__name__)


def _asset_uid_from_chain_and_id(chain: str | None, item_id: str | None, symbol: str | None) -> str:
    suffix = item_id or symbol or "unknown"
    return f"debank:{chain or 'unknown'}:{suffix}"


class DebankCollector(Collector):
    name = "debank"

    def __init__(self, config: DebankConfig, wallets: list[EvmWalletConfig], client: httpx.AsyncClient) -> None:
        self._config = config
        self._wallets = wallets
        self._client = client
        self._limit = asyncio.Semaphore(4)

    async def collect(self) -> CollectionResult:
        result = CollectionResult()
        gathered = await asyncio.gather(*(self._collect_wallet(wallet) for wallet in self._wallets), return_exceptions=True)
        for item in gathered:
            if isinstance(item, Exception):
                LOG.exception("debank collector task failed", exc_info=item)
                result.warnings.append(str(item))
                continue
            result.extend(item)
        return result

    async def _collect_wallet(self, wallet: EvmWalletConfig) -> CollectionResult:
        async with self._limit:
            result = CollectionResult()
            account_key = wallet.label or wallet.address
            collected_at = utc_now()

            total_balance, status = await self._get("/v1/user/total_balance", {"id": wallet.address})
            result.raw_ingestions.append(
                RawIngestionRecord(
                    source_type="onchain",
                    source="debank",
                    account_key=account_key,
                    account_label=wallet.label,
                    endpoint="/v1/user/total_balance",
                    payload=total_balance,
                    request_params={"id": wallet.address},
                    http_status=status,
                )
            )

            total_usd = decimal_or_none(total_balance.get("total_usd_value"))
            if total_usd is not None:
                result.source_summaries.append(
                    SummaryRecord(
                        source_type="onchain",
                        source="debank",
                        account_key=account_key,
                        account_label=wallet.label,
                        account_type="wallet",
                        metric_code="total_usd_value",
                        metric_unit="USD",
                        metric_value=total_usd,
                        collected_at=collected_at,
                        metadata={"wallet_address": wallet.address},
                    )
                )

            chain_list = total_balance.get("chain_list") or []
            for chain_item in chain_list:
                chain = chain_item.get("id")
                usd_value = decimal_or_none(chain_item.get("usd_value"))
                if chain and usd_value is not None:
                    result.source_summaries.append(
                        SummaryRecord(
                            source_type="onchain",
                            source="debank",
                            account_key=account_key,
                            account_label=wallet.label,
                            account_type="wallet_chain",
                            metric_code="chain_usd_value",
                            metric_unit="USD",
                            metric_value=usd_value,
                            collected_at=collected_at,
                            metadata={
                                "wallet_address": wallet.address,
                                "chain": chain,
                                "name": chain_item.get("name"),
                            },
                        )
                    )

            all_tokens, status = await self._get("/v1/user/all_token_list", {"id": wallet.address, "is_all": "true"})
            result.raw_ingestions.append(
                RawIngestionRecord(
                    source_type="onchain",
                    source="debank",
                    account_key=account_key,
                    account_label=wallet.label,
                    endpoint="/v1/user/all_token_list",
                    payload=all_tokens,
                    request_params={"id": wallet.address, "is_all": "true"},
                    http_status=status,
                )
            )

            for token in all_tokens if isinstance(all_tokens, list) else []:
                amount = decimal_or_none(token.get("amount"))
                price_usd = decimal_or_none(token.get("price"))
                usd_value = decimal_or_none(token.get("usd_value"))
                chain = token.get("chain")
                result.positions.append(
                    PositionRecord(
                        source_type="onchain",
                        source="debank",
                        chain=chain,
                        account_key=account_key,
                        account_label=wallet.label,
                        account_type="wallet",
                        wallet_address=wallet.address,
                        position_kind="token",
                        asset_uid=_asset_uid_from_chain_and_id(chain, token.get("id"), token.get("symbol")),
                        asset_symbol=token.get("symbol"),
                        asset_name=token.get("name"),
                        token_address=token.get("id"),
                        amount_raw=None,
                        decimals=int(token["decimals"]) if token.get("decimals") is not None else None,
                        amount=amount,
                        price_usd=price_usd,
                        price_source="debank:all_token_list" if price_usd is not None else None,
                        price_as_of=collected_at if price_usd is not None else None,
                        usd_value=usd_value,
                        is_verified=None,
                        is_spam=None,
                        metadata={
                            "is_core": token.get("is_core"),
                            "is_verified": token.get("is_verified"),
                            "logo_url": token.get("logo_url"),
                            "optimized_symbol": token.get("optimized_symbol"),
                            "protocol_id": token.get("protocol_id"),
                        },
                        collected_at=collected_at,
                    )
                )

            complex_protocols, status = await self._get("/v1/user/all_complex_protocol_list", {"id": wallet.address})
            result.raw_ingestions.append(
                RawIngestionRecord(
                    source_type="onchain",
                    source="debank",
                    account_key=account_key,
                    account_label=wallet.label,
                    endpoint="/v1/user/all_complex_protocol_list",
                    payload=complex_protocols,
                    request_params={"id": wallet.address},
                    http_status=status,
                )
            )

            for protocol in complex_protocols if isinstance(complex_protocols, list) else []:
                portfolio_items = protocol.get("portfolio_item_list") or []
                protocol_id = protocol.get("id")
                protocol_name = protocol.get("name")
                chain = protocol.get("chain")
                for index, item in enumerate(portfolio_items):
                    stats = item.get("stats") or {}
                    detail_types = []
                    detail = item.get("detail")
                    if isinstance(detail, dict):
                        detail_types = sorted(detail.keys())
                    usd_value = decimal_or_none(item.get("stats", {}).get("net_usd_value"))
                    if usd_value is None:
                        usd_value = decimal_or_none(item.get("usd_value"))

                    result.positions.append(
                        PositionRecord(
                            source_type="onchain",
                            source="debank",
                            chain=chain,
                            account_key=account_key,
                            account_label=wallet.label,
                            account_type="wallet_defi",
                            wallet_address=wallet.address,
                            position_kind="defi",
                            asset_uid=f"debank:defi:{protocol_id or 'unknown'}:{index}",
                            asset_symbol=protocol_name,
                            asset_name=item.get("name") or protocol_name,
                            token_address=None,
                            amount_raw=None,
                            decimals=None,
                            amount=None,
                            price_usd=None,
                            price_source=None,
                            price_as_of=None,
                            usd_value=usd_value,
                            is_verified=None,
                            is_spam=None,
                            metadata={
                                "protocol_id": protocol_id,
                                "protocol_name": protocol_name,
                                "protocol_logo_url": protocol.get("logo_url"),
                                "detail_types": detail_types,
                                "proxy_detail": detail,
                                "stats": stats,
                            },
                            collected_at=collected_at,
                        )
                    )

            return result

    async def _get(self, path: str, params: dict[str, str]) -> tuple[Any, int]:
        response = await self._client.get(
            f"{self._config.base_url}{path}",
            params=params,
            headers={"AccessKey": self._config.access_key},
        )
        response.raise_for_status()
        return response.json(), response.status_code
