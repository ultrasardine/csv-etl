"""CSV loader for Ghostfolio format."""

import csv
from pathlib import Path
from typing import Iterator

from .base import Loader
from ..models.ghostfolio import GhostfolioActivity


class GhostfolioCsvLoader(Loader):
    """Load Ghostfolio activities to CSV file."""

    def load(self, activities: Iterator[GhostfolioActivity], path: Path) -> int:
        """Write activities to CSV file."""
        count = 0
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=GhostfolioActivity.fieldnames())
            writer.writeheader()
            for activity in activities:
                writer.writerow(activity.to_dict())
                count += 1
        return count
