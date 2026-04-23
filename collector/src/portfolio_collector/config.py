from __future__ import annotations

import json
import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

DEFAULT_MORALIS_EVM_CHAINS = [
    "eth",
    "base",
    "arbitrum",
    "optimism",
    "polygon",
    "bsc",
]


def _parse_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _parse_json_list(name: str) -> list[object]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return []
    parsed = json.loads(raw)
    if not isinstance(parsed, list):
        raise ValueError(f"{name} must be a JSON array")
    return parsed


@dataclass(slots=True)
class EvmWalletConfig:
    address: str
    chains: list[str] = field(default_factory=lambda: list(DEFAULT_MORALIS_EVM_CHAINS))
    label: str | None = None
    include_defi: bool = True


@dataclass(slots=True)
class SolWalletConfig:
    address: str
    network: str = "mainnet"
    label: str | None = None


@dataclass(slots=True)
class ZerionWalletConfig:
    address: str
    label: str | None = None


@dataclass(slots=True)
class BinanceConfig:
    api_key: str
    api_secret: str
    base_url: str = "https://api.binance.com"
    quote_asset: str = "USDT"
    include_subaccounts: bool = True


@dataclass(slots=True)
class OkxConfig:
    api_key: str
    api_secret: str
    api_passphrase: str
    base_url: str = "https://www.okx.com"
    include_subaccounts: bool = True
    valuation_ccy: str = "USD"


@dataclass(slots=True)
class DebankConfig:
    access_key: str
    base_url: str = "https://pro-openapi.debank.com"


@dataclass(slots=True)
class ZerionConfig:
    api_key: str
    base_url: str = "https://api.zerion.io"
    sync: bool = False


@dataclass(slots=True)
class Settings:
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    base_currency: str = "USD"
    interval_seconds: int = 1800
    run_once: bool = False
    log_level: str = "INFO"
    request_timeout_seconds: int = 30
    moralis_api_key: str | None = None
    evm_wallets: list[EvmWalletConfig] = field(default_factory=list)
    sol_wallets: list[SolWalletConfig] = field(default_factory=list)
    debank: DebankConfig | None = None
    zerion: ZerionConfig | None = None
    zerion_wallets: list[ZerionWalletConfig] = field(default_factory=list)
    binance: BinanceConfig | None = None
    okx: OkxConfig | None = None

    @property
    def postgres_dsn(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @classmethod
    def from_env(cls) -> "Settings":
        load_dotenv()

        evm_wallets: list[EvmWalletConfig] = []
        for item in _parse_json_list("MORALIS_EVM_WALLETS"):
            if isinstance(item, str):
                evm_wallets.append(EvmWalletConfig(address=item))
                continue
            if not isinstance(item, dict):
                raise ValueError("MORALIS_EVM_WALLETS entries must be strings or objects")
            chains_value = item.get("chains")
            if chains_value is None and item.get("chain"):
                chains = [str(item["chain"])]
            elif chains_value is None:
                chains = list(DEFAULT_MORALIS_EVM_CHAINS)
            elif isinstance(chains_value, list):
                chains = [str(value) for value in chains_value if str(value).strip()]
            else:
                raise ValueError("MORALIS_EVM_WALLETS chains must be a list when provided")
            if not chains:
                raise ValueError("MORALIS_EVM_WALLETS chains cannot be empty")
            evm_wallets.append(
                EvmWalletConfig(
                    address=str(item["address"]),
                    chains=chains,
                    label=str(item["label"]) if item.get("label") else None,
                    include_defi=bool(item.get("include_defi", True)),
                )
            )

        sol_wallets: list[SolWalletConfig] = []
        for item in _parse_json_list("MORALIS_SOL_WALLETS"):
            if isinstance(item, str):
                sol_wallets.append(SolWalletConfig(address=item))
                continue
            if not isinstance(item, dict):
                raise ValueError("MORALIS_SOL_WALLETS entries must be strings or objects")
            sol_wallets.append(
                SolWalletConfig(
                    network=str(item.get("network", "mainnet")),
                    address=str(item["address"]),
                    label=str(item["label"]) if item.get("label") else None,
                )
            )

        zerion_wallets: list[ZerionWalletConfig] = []
        for item in _parse_json_list("ZERION_WALLETS"):
            if isinstance(item, str):
                zerion_wallets.append(ZerionWalletConfig(address=item))
                continue
            if not isinstance(item, dict):
                raise ValueError("ZERION_WALLETS entries must be strings or objects")
            zerion_wallets.append(
                ZerionWalletConfig(
                    address=str(item["address"]),
                    label=str(item["label"]) if item.get("label") else None,
                )
            )

        if not zerion_wallets:
            for wallet in evm_wallets:
                zerion_wallets.append(ZerionWalletConfig(address=wallet.address, label=wallet.label))
            for wallet in sol_wallets:
                zerion_wallets.append(ZerionWalletConfig(address=wallet.address, label=wallet.label))

        debank = None
        if os.getenv("DEBANK_ACCESS_KEY"):
            debank = DebankConfig(
                access_key=os.environ["DEBANK_ACCESS_KEY"],
                base_url=os.getenv("DEBANK_BASE_URL", "https://pro-openapi.debank.com"),
            )

        zerion = None
        if os.getenv("ZERION_API_KEY"):
            zerion = ZerionConfig(
                api_key=os.environ["ZERION_API_KEY"],
                base_url=os.getenv("ZERION_BASE_URL", "https://api.zerion.io"),
                sync=_parse_bool(os.getenv("ZERION_SYNC"), False),
            )

        binance = None
        if os.getenv("BINANCE_API_KEY") and os.getenv("BINANCE_API_SECRET"):
            binance = BinanceConfig(
                api_key=os.environ["BINANCE_API_KEY"],
                api_secret=os.environ["BINANCE_API_SECRET"],
                base_url=os.getenv("BINANCE_BASE_URL", "https://api.binance.com"),
                quote_asset=os.getenv("BINANCE_QUOTE_ASSET", "USDT"),
                include_subaccounts=_parse_bool(os.getenv("BINANCE_INCLUDE_SUBACCOUNTS"), True),
            )

        okx = None
        if os.getenv("OKX_API_KEY") and os.getenv("OKX_API_SECRET") and os.getenv("OKX_API_PASSPHRASE"):
            okx = OkxConfig(
                api_key=os.environ["OKX_API_KEY"],
                api_secret=os.environ["OKX_API_SECRET"],
                api_passphrase=os.environ["OKX_API_PASSPHRASE"],
                base_url=os.getenv("OKX_BASE_URL", "https://www.okx.com"),
                include_subaccounts=_parse_bool(os.getenv("OKX_INCLUDE_SUBACCOUNTS"), True),
                valuation_ccy=os.getenv("OKX_VALUATION_CCY", "USD"),
            )

        return cls(
            postgres_host=os.getenv("POSTGRES_HOST", "localhost"),
            postgres_port=int(os.getenv("POSTGRES_PORT", "5432")),
            postgres_db=os.getenv("POSTGRES_DB", "portfolio"),
            postgres_user=os.getenv("POSTGRES_USER", "portfolio"),
            postgres_password=os.getenv("POSTGRES_PASSWORD", "portfolio"),
            base_currency=os.getenv("COLLECTOR_BASE_CURRENCY", "USD"),
            interval_seconds=int(os.getenv("COLLECTOR_INTERVAL_SECONDS", "1800")),
            run_once=_parse_bool(os.getenv("COLLECTOR_RUN_ONCE"), False),
            log_level=os.getenv("COLLECTOR_LOG_LEVEL", "INFO").upper(),
            request_timeout_seconds=int(os.getenv("COLLECTOR_REQUEST_TIMEOUT_SECONDS", "30")),
            moralis_api_key=os.getenv("MORALIS_API_KEY") or None,
            evm_wallets=evm_wallets,
            sol_wallets=sol_wallets,
            debank=debank,
            zerion=zerion,
            zerion_wallets=zerion_wallets,
            binance=binance,
            okx=okx,
        )
