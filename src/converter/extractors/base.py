"""Base extractor interface."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterator, Any


class Extractor(ABC):
    """Base class for extracting data from source files."""

    @abstractmethod
    def extract(self, path: Path) -> Iterator[Any]:
        """Yield source model instances from file."""
        pass
