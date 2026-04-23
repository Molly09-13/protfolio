from __future__ import annotations

import asyncio
import logging

import httpx

from . import db
from .collectors import BinanceCollector, DebankCollector, MoralisCollector, OkxCollector, ZerionCollector
from .config import Settings
from .models import CollectionResult


LOG = logging.getLogger(__name__)


class Orchestrator:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    async def run_once(self) -> int:
        async with httpx.AsyncClient(timeout=self._settings.request_timeout_seconds) as client:
            collectors = []
            if self._settings.zerion and self._settings.zerion_wallets:
                collectors.append(ZerionCollector(self._settings.zerion, self._settings.zerion_wallets, client))
            elif self._settings.debank and self._settings.evm_wallets:
                collectors.append(DebankCollector(self._settings.debank, self._settings.evm_wallets, client))
            if not self._settings.zerion and self._settings.moralis_api_key and self._settings.sol_wallets:
                collectors.append(MoralisCollector(self._settings, client, collect_evm=False, collect_sol=True))
            if self._settings.binance:
                collectors.append(BinanceCollector(self._settings.binance, client))
            if self._settings.okx:
                collectors.append(OkxCollector(self._settings.okx, client))

            if not collectors:
                LOG.warning("no collectors enabled; exiting without work")
                return 0

            conn = await db.connect(self._settings.postgres_dsn)
            try:
                snapshot_run_id = await db.create_snapshot_run(conn, base_currency=self._settings.base_currency)
                await conn.commit()

                gathered = await asyncio.gather(*(collector.collect() for collector in collectors), return_exceptions=True)
                combined = CollectionResult()
                errors: list[str] = []
                for collector, item in zip(collectors, gathered, strict=True):
                    if isinstance(item, Exception):
                        LOG.exception("%s collector failed", collector.name, exc_info=item)
                        errors.append(f"{collector.name}: {item}")
                        continue
                    combined.extend(item)
                    errors.extend(item.warnings)

                raw_count = await db.insert_raw_ingestions(conn, snapshot_run_id, combined.raw_ingestions)
                price_count = await db.insert_prices(conn, snapshot_run_id, combined.prices)
                position_count = await db.insert_positions(conn, snapshot_run_id, combined.positions)
                summary_count = await db.insert_source_summaries(conn, snapshot_run_id, combined.source_summaries)

                status = "success"
                if errors and (position_count or summary_count or price_count or raw_count):
                    status = "partial_success"
                elif errors:
                    status = "failed"

                await db.finish_snapshot_run(
                    conn,
                    snapshot_run_id,
                    status=status,
                    position_count=position_count,
                    summary_count=summary_count,
                    price_count=price_count,
                    error_count=len(errors),
                    notes="\n".join(errors[:20]) if errors else None,
                    metadata={
                        "collectors": [collector.name for collector in collectors],
                        "raw_count": raw_count,
                        "warnings": errors,
                    },
                )
                await conn.commit()

                LOG.info(
                    "snapshot %s finished with status=%s positions=%s summaries=%s prices=%s raws=%s warnings=%s",
                    snapshot_run_id,
                    status,
                    position_count,
                    summary_count,
                    price_count,
                    raw_count,
                    len(errors),
                )
                return snapshot_run_id
            except Exception:
                await conn.rollback()
                raise
            finally:
                await conn.close()
