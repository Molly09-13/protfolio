from __future__ import annotations

from abc import ABC, abstractmethod

from ..models import CollectionResult


class Collector(ABC):
    name: str

    @abstractmethod
    async def collect(self) -> CollectionResult:
        raise NotImplementedError

