from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from ..config import EvmWalletConfig, Settings, SolWalletConfig
from ..models import CollectionResult, PositionRecord, PriceRecord, RawIngestionRecord, SummaryRecord
from ..utils import compute_amount, decimal_or_none, utc_now
from .base import Collector


LOG = logging.getLogger(__name__)
EVM_BASE_URL = "https://deep-index.moralis.io/api/v2.2"
SOL_BASE_URL = "https://solana-gateway.moralis.io"
WRAPPED_SOL_MINT = "So11111111111111111111111111111111111111112"


def _list_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("result", "data", "tokens"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


def _json_or_text(response: httpx.Response) -> Any:
    try:
        return response.json()
    except ValueError:
        return {"text": response.text}


def _is_truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _should_filter_evm_token(token: dict[str, Any]) -> tuple[bool, str | None]:
    if _is_truthy(token.get("possible_spam")):
        return True, "possible_spam"

    if _is_truthy(token.get("native_token")):
        return False, None

    price_usd = decimal_or_none(token.get("usd_price"))
    verified = _is_truthy(token.get("verified_contract"))
    has_symbol = bool(str(token.get("symbol") or "").strip())
    balance = decimal_or_none(token.get("balance_formatted")) or compute_amount(
        token.get("balance"),
        token.get("decimals"),
    )

    if has_symbol and not verified and balance is not None and balance > 0 and (price_usd is None or price_usd <= 0):
        return True, "unverified_zero_price"

    return False, None


class MoralisCollector(Collector):
    name = "moralis"

    def __init__(self, settings: Settings, client: httpx.AsyncClient, *, collect_evm: bool = True, collect_sol: bool = True) -> None:
        self._settings = settings
        self._client = client
        self._collect_evm = collect_evm
        self._collect_sol = collect_sol
        self._price_limit = asyncio.Semaphore(5)

    async def collect(self) -> CollectionResult:
        result = CollectionResult()
        tasks = []

        if self._collect_evm:
            for wallet in self._settings.evm_wallets:
                for chain in wallet.chains:
                    tasks.append(self._collect_evm_wallet(wallet, chain))
        if self._collect_sol:
            for wallet in self._settings.sol_wallets:
                tasks.append(self._collect_sol_wallet(wallet))

        if not tasks:
            return result

        gathered = await asyncio.gather(*tasks, return_exceptions=True)
        for item in gathered:
            if isinstance(item, Exception):
                LOG.exception("moralis collector task failed", exc_info=item)
                result.warnings.append(str(item))
                continue
            result.extend(item)
        return result

    async def _collect_evm_wallet(self, wallet: EvmWalletConfig, chain: str) -> CollectionResult:
        result = CollectionResult()
        account_key = wallet.label or wallet.address
        collected_at = utc_now()

        cursor: str | None = None
        while True:
            params: dict[str, Any] = {
                "chain": chain,
                "exclude_spam": "true",
                "limit": 100,
            }
            if cursor:
                params["cursor"] = cursor

            payload, status = await self._get_evm(
                f"/wallets/{wallet.address}/tokens",
                params=params,
            )
            result.raw_ingestions.append(
                RawIngestionRecord(
                    source_type="onchain",
                    source="moralis",
                    account_key=account_key,
                    account_label=wallet.label,
                    endpoint=f"/wallets/{wallet.address}/tokens",
                    payload=payload,
                    request_params=params,
                    http_status=status,
                )
            )

            for token in _list_payload(payload):
                should_filter, filter_reason = _should_filter_evm_token(token)
                if should_filter:
                    continue

                amount = decimal_or_none(token.get("balance_formatted")) or compute_amount(
                    token.get("balance"),
                    token.get("decimals"),
                )
                price_usd = decimal_or_none(token.get("usd_price"))
                price_as_of = collected_at if price_usd is not None else None
                usd_value = decimal_or_none(token.get("usd_value"))
                token_address = None if token.get("native_token") else token.get("token_address")
                asset_uid = (
                    f"evm:{chain}:native"
                    if token.get("native_token")
                    else f"evm:{chain}:{token_address}"
                )

                result.positions.append(
                    PositionRecord(
                        source_type="onchain",
                        source="moralis",
                        chain=chain,
                        account_key=account_key,
                        account_label=wallet.label,
                        account_type="wallet",
                        subaccount=None,
                        wallet_address=wallet.address,
                        position_kind="native" if token.get("native_token") else "token",
                        asset_uid=asset_uid,
                        asset_symbol=token.get("symbol"),
                        asset_name=token.get("name"),
                        token_address=token_address,
                        amount_raw=decimal_or_none(token.get("balance")),
                        decimals=int(token["decimals"]) if token.get("decimals") is not None else None,
                        amount=amount,
                        price_usd=price_usd,
                        price_source="moralis:wallet_tokens" if price_usd is not None else None,
                        price_as_of=price_as_of,
                        usd_value=usd_value,
                        is_verified=token.get("verified_contract"),
                        is_spam=token.get("possible_spam"),
                        metadata={
                            "native_token": bool(token.get("native_token")),
                            "portfolio_percentage": token.get("portfolio_percentage"),
                            "logo": token.get("logo"),
                            "thumbnail": token.get("thumbnail"),
                        },
                        collected_at=collected_at,
                    )
                )

                if price_usd is not None:
                    result.prices.append(
                        PriceRecord(
                            asset_uid=asset_uid,
                            symbol=token.get("symbol"),
                            chain=chain,
                            token_address=token_address,
                            price_source="moralis:wallet_tokens",
                            price_usd=price_usd,
                            quoted_at=collected_at,
                            metadata={"wallet_address": wallet.address},
                        )
                    )

            cursor = payload.get("cursor")
            if not cursor:
                break

        if wallet.include_defi:
            skip_defi_positions = False
            try:
                defi_summary, status = await self._get_evm(
                    f"/wallets/{wallet.address}/defi/summary",
                    params={"chain": chain},
                )
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 400:
                    LOG.info("skip moralis defi summary for wallet=%s chain=%s due to http 400", wallet.address, chain)
                    result.raw_ingestions.append(
                        RawIngestionRecord(
                            source_type="onchain",
                            source="moralis",
                            account_key=account_key,
                            account_label=wallet.label,
                            endpoint=f"/wallets/{wallet.address}/defi/summary",
                            payload=_json_or_text(exc.response),
                            request_params={"chain": chain},
                            http_status=exc.response.status_code,
                        )
                    )
                    skip_defi_positions = True
                    defi_summary = {}
                    status = exc.response.status_code
                else:
                    raise

            if not skip_defi_positions:
                result.raw_ingestions.append(
                    RawIngestionRecord(
                        source_type="onchain",
                        source="moralis",
                        account_key=account_key,
                        account_label=wallet.label,
                        endpoint=f"/wallets/{wallet.address}/defi/summary",
                        payload=defi_summary,
                        request_params={"chain": chain},
                        http_status=status,
                    )
                )
                for metric_code, raw_value, unit in (
                    ("defi_total_usd_value", defi_summary.get("total_usd_value"), "USD"),
                    ("defi_total_unclaimed_usd_value", defi_summary.get("total_unclaimed_usd_value"), "USD"),
                    ("defi_active_protocols", defi_summary.get("active_protocols"), "count"),
                    ("defi_total_positions", defi_summary.get("total_positions"), "count"),
                ):
                    metric_value = decimal_or_none(raw_value)
                    if metric_value is None:
                        continue
                    result.source_summaries.append(
                        SummaryRecord(
                            source_type="onchain",
                            source="moralis",
                            account_key=account_key,
                            account_label=wallet.label,
                            account_type="wallet_defi",
                            metric_code=metric_code,
                            metric_unit=unit,
                            metric_value=metric_value,
                            collected_at=collected_at,
                            metadata={"chain": chain, "wallet_address": wallet.address},
                        )
                    )

                try:
                    defi_positions, status = await self._get_evm(
                        f"/wallets/{wallet.address}/defi/positions",
                        params={"chain": chain},
                    )
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code == 400:
                        LOG.info("skip moralis defi positions for wallet=%s chain=%s due to http 400", wallet.address, chain)
                        result.raw_ingestions.append(
                            RawIngestionRecord(
                                source_type="onchain",
                                source="moralis",
                                account_key=account_key,
                                account_label=wallet.label,
                                endpoint=f"/wallets/{wallet.address}/defi/positions",
                                payload=_json_or_text(exc.response),
                                request_params={"chain": chain},
                                http_status=exc.response.status_code,
                            )
                        )
                    else:
                        raise
                else:
                    result.raw_ingestions.append(
                        RawIngestionRecord(
                            source_type="onchain",
                            source="moralis",
                            account_key=account_key,
                            account_label=wallet.label,
                            endpoint=f"/wallets/{wallet.address}/defi/positions",
                            payload=defi_positions,
                            request_params={"chain": chain},
                            http_status=status,
                        )
                    )

                    for index, protocol in enumerate(_list_payload(defi_positions)):
                        position = protocol.get("position", {})
                        result.positions.append(
                            PositionRecord(
                                source_type="onchain",
                                source="moralis",
                                chain=chain,
                                account_key=account_key,
                                account_label=wallet.label,
                                account_type="wallet_defi",
                                wallet_address=wallet.address,
                                position_kind="defi",
                                asset_uid=f"defi:{chain}:{protocol.get('protocol_id')}:{index}",
                                asset_symbol=protocol.get("protocol_name"),
                                asset_name=position.get("label"),
                                token_address=position.get("address"),
                                amount=None,
                                amount_raw=None,
                                decimals=None,
                                price_usd=None,
                                price_source=None,
                                price_as_of=None,
                                usd_value=decimal_or_none(position.get("balance_usd")),
                                is_verified=None,
                                is_spam=None,
                                metadata={
                                    "chain": chain,
                                    "protocol_id": protocol.get("protocol_id"),
                                    "protocol_url": protocol.get("protocol_url"),
                                    "protocol_logo": protocol.get("protocol_logo"),
                                    "total_unclaimed_usd_value": position.get("total_unclaimed_usd_value"),
                                    "tokens": position.get("tokens", []),
                                    "position_details": position.get("position_details", {}),
                                },
                                collected_at=collected_at,
                            )
                        )

        return result

    async def _collect_sol_wallet(self, wallet: SolWalletConfig) -> CollectionResult:
        result = CollectionResult()
        account_key = wallet.label or wallet.address
        collected_at = utc_now()

        portfolio, status = await self._get_sol(f"/account/{wallet.network}/{wallet.address}/portfolio")
        result.raw_ingestions.append(
            RawIngestionRecord(
                source_type="onchain",
                source="moralis",
                account_key=account_key,
                account_label=wallet.label,
                endpoint=f"/account/{wallet.network}/{wallet.address}/portfolio",
                payload=portfolio,
                request_params={},
                http_status=status,
            )
        )

        tokens = _list_payload(portfolio)
        price_map = await self._collect_sol_prices(wallet, [token.get("mint") for token in tokens] + [WRAPPED_SOL_MINT])
        result.extend(price_map["result"])
        prices = price_map["prices"]

        native = portfolio.get("nativeBalance", {})
        sol_price = prices.get(WRAPPED_SOL_MINT)
        native_amount = decimal_or_none(native.get("solana"))
        native_price = decimal_or_none(sol_price.get("usdPrice") if sol_price else None)
        result.positions.append(
            PositionRecord(
                source_type="onchain",
                source="moralis",
                chain="solana",
                account_key=account_key,
                account_label=wallet.label,
                account_type="wallet",
                wallet_address=wallet.address,
                position_kind="native",
                asset_uid="sol:mainnet:native",
                asset_symbol="SOL",
                asset_name="Solana",
                token_address=None,
                amount_raw=decimal_or_none(native.get("lamports")),
                decimals=9,
                amount=native_amount,
                price_usd=native_price,
                price_source="moralis:solana_price" if native_price is not None else None,
                price_as_of=collected_at if native_price is not None else None,
                usd_value=(native_amount * native_price) if native_amount is not None and native_price is not None else None,
                is_verified=True,
                is_spam=False,
                metadata={"network": wallet.network},
                collected_at=collected_at,
            )
        )

        for token in tokens:
            mint = token.get("mint")
            price_payload = prices.get(mint, {})
            price_usd = decimal_or_none(price_payload.get("usdPrice"))
            amount = decimal_or_none(token.get("amount")) or compute_amount(
                token.get("amountRaw"),
                token.get("decimals"),
            )
            result.positions.append(
                PositionRecord(
                    source_type="onchain",
                    source="moralis",
                    chain="solana",
                    account_key=account_key,
                    account_label=wallet.label,
                    account_type="wallet",
                    wallet_address=wallet.address,
                    position_kind="token",
                    asset_uid=f"sol:{wallet.network}:{mint}",
                    asset_symbol=token.get("symbol"),
                    asset_name=token.get("name"),
                    token_address=mint,
                    amount_raw=decimal_or_none(token.get("amountRaw")),
                    decimals=int(token["decimals"]) if token.get("decimals") is not None else None,
                    amount=amount,
                    price_usd=price_usd,
                    price_source="moralis:solana_price" if price_usd is not None else None,
                    price_as_of=collected_at if price_usd is not None else None,
                    usd_value=(amount * price_usd) if amount is not None and price_usd is not None else None,
                    is_verified=token.get("isVerifiedContract"),
                    is_spam=token.get("possibleSpam"),
                    metadata={
                        "network": wallet.network,
                        "associated_token_address": token.get("associatedTokenAddress"),
                        "score": token.get("score"),
                        "logo": token.get("logo"),
                    },
                    collected_at=collected_at,
                )
            )

        return result

    async def _collect_sol_prices(self, wallet: SolWalletConfig, mints: list[str | None]) -> dict[str, Any]:
        unique_mints = [mint for mint in dict.fromkeys(mints) if mint]
        prices: dict[str, Any] = {}
        result = CollectionResult()
        account_key = wallet.label or wallet.address

        async def fetch_one(mint: str) -> None:
            async with self._price_limit:
                try:
                    payload, status = await self._get_sol(f"/token/{wallet.network}/{mint}/price")
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code == 404:
                        payload = {"error": "price_not_found"}
                        status = 404
                    else:
                        raise
                result.raw_ingestions.append(
                    RawIngestionRecord(
                        source_type="onchain",
                        source="moralis",
                        account_key=account_key,
                        account_label=wallet.label,
                        endpoint=f"/token/{wallet.network}/{mint}/price",
                        payload=payload,
                        request_params={},
                        http_status=status,
                    )
                )
                prices[mint] = payload
                usd_price = decimal_or_none(payload.get("usdPrice"))
                if usd_price is not None:
                    result.prices.append(
                        PriceRecord(
                            asset_uid=f"sol:{wallet.network}:{mint}",
                            symbol=payload.get("symbol"),
                            chain="solana",
                            token_address=mint,
                            price_source="moralis:solana_price",
                            price_usd=usd_price,
                            quoted_at=utc_now(),
                            metadata={"wallet_address": wallet.address},
                        )
                    )

        await asyncio.gather(*(fetch_one(mint) for mint in unique_mints))
        return {"prices": prices, "result": result}

    async def _get_evm(self, path: str, *, params: dict[str, Any] | None = None) -> tuple[dict[str, Any], int]:
        response = await self._client.get(
            f"{EVM_BASE_URL}{path}",
            params=params,
            headers={"X-API-Key": self._settings.moralis_api_key or ""},
        )
        response.raise_for_status()
        return response.json(), response.status_code

    async def _get_sol(self, path: str, *, params: dict[str, Any] | None = None) -> tuple[dict[str, Any], int]:
        response = await self._client.get(
            f"{SOL_BASE_URL}{path}",
            params=params,
            headers={"X-Api-Key": self._settings.moralis_api_key or ""},
        )
        response.raise_for_status()
        return response.json(), response.status_code
