"""Revolut Stocks extractor."""

import csv
from pathlib import Path
from typing import Iterator

from .base import Extractor
from ..models.revolut_stocks import RevolutStocksActivity


class RevolutStocksExtractor(Extractor):
    """Extract activities from Revolut stocks CSV export."""

    def extract(self, path: Path) -> Iterator[RevolutStocksActivity]:
        """Parse Revolut stocks CSV and yield activities."""
        with path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield RevolutStocksActivity(
                    date=row.get("Date", "").strip(),
                    ticker=row.get("Ticker", "").strip(),
                    type=row.get("Type", "").strip(),
                    quantity=row.get("Quantity", "").strip(),
                    price_per_share=row.get("Price per share", "").strip(),
                    total_amount=row.get("Total Amount", "").strip(),
                    currency=row.get("Currency", "").strip() or "USD",
                )
