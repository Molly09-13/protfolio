from __future__ import annotations

import asyncio
import logging
import time

from .config import Settings
from .orchestrator import Orchestrator


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


async def async_main() -> None:
    settings = Settings.from_env()
    configure_logging(settings.log_level)
    orchestrator = Orchestrator(settings)

    if settings.run_once:
        await orchestrator.run_once()
        return

    while True:
        started = time.monotonic()
        try:
            await orchestrator.run_once()
        except Exception:
            logging.getLogger(__name__).exception("collector loop failed")
        elapsed = time.monotonic() - started
        sleep_for = max(settings.interval_seconds - int(elapsed), 1)
        await asyncio.sleep(sleep_for)


def main() -> None:
    asyncio.run(async_main())

