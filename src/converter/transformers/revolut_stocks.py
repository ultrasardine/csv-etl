"""Revolut Stocks transformer."""

from datetime import datetime

from .base import Transformer
from ..models.ghostfolio import GhostfolioActivity, GhostfolioActivityType
from ..models.revolut_stocks import RevolutStocksActivity


class RevolutStocksTransformer(Transformer):
    """Transform Revolut stock activities to Ghostfolio format."""

    TYPE_MAP = {
        "BUY - MARKET": GhostfolioActivityType.BUY,
        "BUY - LIMIT": GhostfolioActivityType.BUY,
        "SELL - MARKET": GhostfolioActivityType.SELL,
        "SELL - LIMIT": GhostfolioActivityType.SELL,
        "DIVIDEND": GhostfolioActivityType.DIVIDEND,
    }

    SKIP_TYPES = {"CASH TOP-UP", "CASH WITHDRAWAL", "CUSTODY FEE", "STOCK SPLIT"}

    # Map currency to Yahoo Finance exchange suffix
    CURRENCY_TO_SUFFIX = {
        "EUR": ".DE",  # Default to XETRA for EUR stocks
        "GBP": ".L",   # London Stock Exchange
        "GBX": ".L",   # London (pence)
    }

    # Known symbol mappings for specific tickers
    SYMBOL_MAP = {
        "4P41": "P911.DE",  # Porsche on XETRA
    }

    def __init__(self, account_name: str = "Revolut Stocks"):
        self.account_name = account_name

    def transform(self, source: RevolutStocksActivity) -> GhostfolioActivity | None:
        type_upper = source.type.upper()
        if type_upper in self.SKIP_TYPES:
            return None

        gf_type = self._map_type(type_upper)
        if gf_type is None:
            return None

        if not source.ticker:
            return None

        symbol = self._map_symbol(source.ticker, source.currency)

        return GhostfolioActivity(
            date=self._parse_date(source.date),
            symbol=symbol,
            type=gf_type,
            quantity=self._parse_float(source.quantity),
            unitPrice=self._parse_price(source.price_per_share),
            fee=0.0,
            currency=source.currency,
            account=self.account_name,
        )

    def _map_symbol(self, ticker: str, currency: str) -> str:
        """Map ticker to Yahoo Finance symbol format."""
        if ticker in self.SYMBOL_MAP:
            return self.SYMBOL_MAP[ticker]

        if currency == "USD":
            return ticker

        suffix = self.CURRENCY_TO_SUFFIX.get(currency, "")
        if suffix and not ticker.endswith(suffix):
            return f"{ticker}{suffix}"

        return ticker

    def _map_type(self, revolut_type: str) -> GhostfolioActivityType | None:
        if revolut_type in self.TYPE_MAP:
            return self.TYPE_MAP[revolut_type]
        if revolut_type.startswith("BUY"):
            return GhostfolioActivityType.BUY
        if revolut_type.startswith("SELL"):
            return GhostfolioActivityType.SELL
        return None

    @staticmethod
    def _parse_date(value: str) -> str:
        """Convert to YYYY-MM-DD format."""
        if not value:
            return ""
        for fmt in [
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d",
            "%d/%m/%Y",
        ]:
            try:
                return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return value

    @staticmethod
    def _parse_float(value: str) -> float:
        if not value:
            return 0.0
        try:
            return float(value.replace(",", ""))
        except ValueError:
            return 0.0

    @staticmethod
    def _parse_price(value: str) -> float:
        """Parse price like 'USD 3.71' or '3.71'."""
        if not value:
            return 0.0
        v = value.strip()
        for prefix in ("USD ", "EUR ", "GBP "):
            if v.startswith(prefix):
                v = v[len(prefix):]
                break
        try:
            return float(v.replace(",", ""))
        except ValueError:
            return 0.0
