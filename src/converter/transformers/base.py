"""Base transformer interface."""

from abc import ABC, abstractmethod
from typing import Any

from ..models.ghostfolio import GhostfolioActivity


class Transformer(ABC):
    """Base class for transforming source models to target models."""

    @abstractmethod
    def transform(self, source: Any) -> GhostfolioActivity | None:
        """Transform source model to Ghostfolio activity.
        
        Returns None to skip the record.
        """
        pass
