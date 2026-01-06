"""Base loader interface."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterator

from ..models.ghostfolio import GhostfolioActivity


class Loader(ABC):
    """Base class for loading transformed data to target."""

    @abstractmethod
    def load(self, activities: Iterator[GhostfolioActivity], path: Path) -> int:
        """Write activities to target.
        
        Returns count of written records.
        """
        pass
