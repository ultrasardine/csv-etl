"""Revolut Crypto extractor."""

import csv
from pathlib import Path
from typing import Iterator

from .base import Extractor
from ..models.revolut_crypto import RevolutCryptoActivity


class RevolutCryptoExtractor(Extractor):
    """Extract activities from Revolut crypto CSV export."""

    def extract(self, path: Path) -> Iterator[RevolutCryptoActivity]:
        """Parse Revolut crypto CSV and yield activities."""
        with path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield RevolutCryptoActivity(
                    symbol=row.get("Symbol", "").strip(),
                    type=row.get("Type", "").strip(),
                    quantity=row.get("Quantity", "").strip(),
                    price=row.get("Price", "").strip(),
                    value=row.get("Value", "").strip(),
                    fees=row.get("Fees", "").strip(),
                    date=row.get("Date", "").strip(),
                )
