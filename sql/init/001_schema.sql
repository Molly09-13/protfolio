CREATE TABLE IF NOT EXISTS snapshot_runs (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ NULL,
    status TEXT NOT NULL DEFAULT 'running',
    trigger_name TEXT NOT NULL DEFAULT 'scheduler',
    base_currency TEXT NOT NULL DEFAULT 'USD',
    position_count INTEGER NOT NULL DEFAULT 0,
    summary_count INTEGER NOT NULL DEFAULT 0,
    price_count INTEGER NOT NULL DEFAULT 0,
    error_count INTEGER NOT NULL DEFAULT 0,
    notes TEXT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS raw_ingestions (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    snapshot_run_id BIGINT NOT NULL REFERENCES snapshot_runs(id) ON DELETE CASCADE,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    source_type TEXT NOT NULL,
    source TEXT NOT NULL,
    account_key TEXT NOT NULL,
    account_label TEXT NULL,
    endpoint TEXT NOT NULL,
    request_params JSONB NOT NULL DEFAULT '{}'::jsonb,
    payload JSONB NOT NULL,
    http_status INTEGER NULL
);

CREATE TABLE IF NOT EXISTS prices (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    snapshot_run_id BIGINT NOT NULL REFERENCES snapshot_runs(id) ON DELETE CASCADE,
    quoted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    asset_uid TEXT NOT NULL,
    symbol TEXT NULL,
    chain TEXT NULL,
    token_address TEXT NULL,
    price_source TEXT NOT NULL,
    price_usd NUMERIC NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (snapshot_run_id, asset_uid, price_source)
);

CREATE TABLE IF NOT EXISTS positions (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    snapshot_run_id BIGINT NOT NULL REFERENCES snapshot_runs(id) ON DELETE CASCADE,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    source_type TEXT NOT NULL,
    source TEXT NOT NULL,
    chain TEXT NULL,
    account_key TEXT NOT NULL,
    account_label TEXT NULL,
    account_type TEXT NOT NULL,
    subaccount TEXT NULL,
    wallet_address TEXT NULL,
    position_kind TEXT NOT NULL,
    asset_uid TEXT NOT NULL,
    asset_symbol TEXT NULL,
    asset_name TEXT NULL,
    token_address TEXT NULL,
    amount_raw NUMERIC NULL,
    decimals INTEGER NULL,
    amount NUMERIC NULL,
    price_usd NUMERIC NULL,
    price_source TEXT NULL,
    price_as_of TIMESTAMPTZ NULL,
    usd_value NUMERIC NULL,
    is_verified BOOLEAN NULL,
    is_spam BOOLEAN NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS source_summaries (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    snapshot_run_id BIGINT NOT NULL REFERENCES snapshot_runs(id) ON DELETE CASCADE,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    source_type TEXT NOT NULL,
    source TEXT NOT NULL,
    account_key TEXT NOT NULL,
    account_label TEXT NULL,
    account_type TEXT NOT NULL,
    metric_code TEXT NOT NULL,
    metric_unit TEXT NOT NULL,
    metric_value NUMERIC NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_snapshot_runs_started_at
    ON snapshot_runs (started_at DESC);

CREATE INDEX IF NOT EXISTS idx_raw_ingestions_snapshot_run_id
    ON raw_ingestions (snapshot_run_id);

CREATE INDEX IF NOT EXISTS idx_positions_snapshot_run_id
    ON positions (snapshot_run_id);

CREATE INDEX IF NOT EXISTS idx_positions_source_account
    ON positions (source, account_type, account_key);

CREATE INDEX IF NOT EXISTS idx_positions_asset_uid
    ON positions (asset_uid);

CREATE INDEX IF NOT EXISTS idx_prices_snapshot_run_id
    ON prices (snapshot_run_id);

CREATE INDEX IF NOT EXISTS idx_source_summaries_snapshot_run_id
    ON source_summaries (snapshot_run_id);

CREATE OR REPLACE VIEW vw_latest_snapshot_run AS
SELECT *
FROM snapshot_runs
WHERE status IN ('success', 'partial_success')
ORDER BY id DESC
LIMIT 1;

