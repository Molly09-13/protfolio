from __future__ import annotations

from typing import Iterable

from psycopg import AsyncConnection
from psycopg.rows import tuple_row
from psycopg.types.json import Jsonb

from .models import PositionRecord, PriceRecord, RawIngestionRecord, SummaryRecord


async def connect(dsn: str) -> AsyncConnection[tuple]:
    return await AsyncConnection.connect(dsn, row_factory=tuple_row)


async def create_snapshot_run(conn: AsyncConnection[tuple], *, base_currency: str) -> int:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO snapshot_runs (base_currency, status)
            VALUES (%s, 'running')
            RETURNING id
            """,
            (base_currency,),
        )
        row = await cur.fetchone()
        if row is None:
            raise RuntimeError("failed to create snapshot run")
        return int(row[0])


async def insert_raw_ingestions(
    conn: AsyncConnection[tuple],
    snapshot_run_id: int,
    rows: Iterable[RawIngestionRecord],
) -> int:
    payload = [
        (
            snapshot_run_id,
            row.received_at,
            row.source_type,
            row.source,
            row.account_key,
            row.account_label,
            row.endpoint,
            Jsonb(row.request_params),
            Jsonb(row.payload),
            row.http_status,
        )
        for row in rows
    ]
    if not payload:
        return 0
    async with conn.cursor() as cur:
        await cur.executemany(
            """
            INSERT INTO raw_ingestions (
                snapshot_run_id,
                received_at,
                source_type,
                source,
                account_key,
                account_label,
                endpoint,
                request_params,
                payload,
                http_status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            payload,
        )
    return len(payload)


async def insert_prices(
    conn: AsyncConnection[tuple],
    snapshot_run_id: int,
    rows: Iterable[PriceRecord],
) -> int:
    payload = [
        (
            snapshot_run_id,
            row.quoted_at,
            row.asset_uid,
            row.symbol,
            row.chain,
            row.token_address,
            row.price_source,
            row.price_usd,
            Jsonb(row.metadata),
        )
        for row in rows
    ]
    if not payload:
        return 0
    async with conn.cursor() as cur:
        await cur.executemany(
            """
            INSERT INTO prices (
                snapshot_run_id,
                quoted_at,
                asset_uid,
                symbol,
                chain,
                token_address,
                price_source,
                price_usd,
                metadata
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (snapshot_run_id, asset_uid, price_source)
            DO UPDATE SET
                quoted_at = EXCLUDED.quoted_at,
                price_usd = EXCLUDED.price_usd,
                metadata = EXCLUDED.metadata
            """,
            payload,
        )
    return len(payload)


async def insert_positions(
    conn: AsyncConnection[tuple],
    snapshot_run_id: int,
    rows: Iterable[PositionRecord],
) -> int:
    payload = [
        (
            snapshot_run_id,
            row.collected_at,
            row.source_type,
            row.source,
            row.chain,
            row.account_key,
            row.account_label,
            row.account_type,
            row.subaccount,
            row.wallet_address,
            row.position_kind,
            row.asset_uid,
            row.asset_symbol,
            row.asset_name,
            row.token_address,
            row.amount_raw,
            row.decimals,
            row.amount,
            row.price_usd,
            row.price_source,
            row.price_as_of,
            row.usd_value,
            row.is_verified,
            row.is_spam,
            Jsonb(row.metadata),
        )
        for row in rows
    ]
    if not payload:
        return 0
    async with conn.cursor() as cur:
        await cur.executemany(
            """
            INSERT INTO positions (
                snapshot_run_id,
                collected_at,
                source_type,
                source,
                chain,
                account_key,
                account_label,
                account_type,
                subaccount,
                wallet_address,
                position_kind,
                asset_uid,
                asset_symbol,
                asset_name,
                token_address,
                amount_raw,
                decimals,
                amount,
                price_usd,
                price_source,
                price_as_of,
                usd_value,
                is_verified,
                is_spam,
                metadata
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            payload,
        )
    return len(payload)


async def insert_source_summaries(
    conn: AsyncConnection[tuple],
    snapshot_run_id: int,
    rows: Iterable[SummaryRecord],
) -> int:
    payload = [
        (
            snapshot_run_id,
            row.collected_at,
            row.source_type,
            row.source,
            row.account_key,
            row.account_label,
            row.account_type,
            row.metric_code,
            row.metric_unit,
            row.metric_value,
            Jsonb(row.metadata),
        )
        for row in rows
    ]
    if not payload:
        return 0
    async with conn.cursor() as cur:
        await cur.executemany(
            """
            INSERT INTO source_summaries (
                snapshot_run_id,
                collected_at,
                source_type,
                source,
                account_key,
                account_label,
                account_type,
                metric_code,
                metric_unit,
                metric_value,
                metadata
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            payload,
        )
    return len(payload)


async def finish_snapshot_run(
    conn: AsyncConnection[tuple],
    snapshot_run_id: int,
    *,
    status: str,
    position_count: int,
    summary_count: int,
    price_count: int,
    error_count: int,
    notes: str | None,
    metadata: dict,
) -> None:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE snapshot_runs
            SET finished_at = now(),
                status = %s,
                position_count = %s,
                summary_count = %s,
                price_count = %s,
                error_count = %s,
                notes = %s,
                metadata = %s
            WHERE id = %s
            """,
            (
                status,
                position_count,
                summary_count,
                price_count,
                error_count,
                notes,
                Jsonb(metadata),
                snapshot_run_id,
            ),
        )

