"""Revolut Crypto source format model."""

from dataclasses import dataclass


@dataclass
class RevolutCryptoActivity:
    """Source format from Revolut crypto CSV export."""

    symbol: str
    type: str
    quantity: str
    price: str
    value: str
    fees: str
    date: str
