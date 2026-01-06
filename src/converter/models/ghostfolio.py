"""Ghostfolio target format models."""

from dataclasses import dataclass
from enum import Enum


class GhostfolioActivityType(Enum):
    """Valid Ghostfolio activity types."""

    BUY = "BUY"
    SELL = "SELL"
    DIVIDEND = "DIVIDEND"
    FEE = "FEE"
    INTEREST = "INTEREST"
    LIABILITY = "LIABILITY"


@dataclass
class GhostfolioActivity:
    """Target format for Ghostfolio import."""

    date: str  # YYYY-MM-DD
    symbol: str
    type: GhostfolioActivityType
    quantity: float
    unitPrice: float
    fee: float
    currency: str
    account: str
    dataSource: str = ""  # YAHOO, COINGECKO, MANUAL, etc.

    def to_dict(self) -> dict:
        """Convert to dictionary for CSV export."""
        result = {
            "date": self.date,
            "symbol": self.symbol,
            "type": self.type.value,
            "quantity": self._format_number(self.quantity),
            "unitPrice": self._format_number(self.unitPrice),
            "fee": self._format_number(self.fee),
            "currency": self.currency,
            "account": self.account,
        }
        if self.dataSource:
            result["dataSource"] = self.dataSource
        return result

    @staticmethod
    def _format_number(value: float) -> str:
        """Format number removing trailing zeros."""
        return f"{value:.8f}".rstrip("0").rstrip(".")

    @classmethod
    def fieldnames(cls) -> list[str]:
        """CSV column names."""
        return [
            "date",
            "symbol",
            "type",
            "quantity",
            "unitPrice",
            "fee",
            "currency",
            "account",
            "dataSource",
        ]
