"""Revolut Stocks source format model."""

from dataclasses import dataclass


@dataclass
class RevolutStocksActivity:
    """Source format from Revolut stocks CSV export."""

    date: str
    ticker: str
    type: str
    quantity: str
    price_per_share: str
    total_amount: str
    currency: str
