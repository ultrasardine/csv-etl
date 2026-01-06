"""Revolut Crypto transformer."""

from datetime import datetime

from .base import Transformer
from ..models.ghostfolio import GhostfolioActivity, GhostfolioActivityType
from ..models.revolut_crypto import RevolutCryptoActivity


class RevolutCryptoTransformer(Transformer):
    """Transform Revolut crypto activities to Ghostfolio format."""

    TYPE_MAP = {
        "BUY": GhostfolioActivityType.BUY,
        "SELL": GhostfolioActivityType.SELL,
    }

    SKIP_TYPES = {"PAYMENT", "STAKE", "UNSTAKE", "SEND", "RECEIVE"}

    # Map crypto symbols to Yahoo Finance format (SYMBOL-USD)
    SYMBOL_MAP = {
        "BTC": "BTC-USD",
        "ETH": "ETH-USD",
        "DOGE": "DOGE-USD",
        "SHIB": "SHIB-USD",
        "XRP": "XRP-USD",
        "DOT": "DOT-USD",
        "ADA": "ADA-USD",
        "SOL": "SOL-USD",
        "MATIC": "MATIC-USD",
        "LINK": "LINK-USD",
        "UNI": "UNI-USD",
        "AVAX": "AVAX-USD",
        "ATOM": "ATOM-USD",
        "LTC": "LTC-USD",
        "XLM": "XLM-USD",
        "ALGO": "ALGO-USD",
        "VET": "VET-USD",
        "FIL": "FIL-USD",
        "AAVE": "AAVE-USD",
        "GRT": "GRT-USD",
        "SAND": "SAND-USD",
        "MANA": "MANA-USD",
        "AXS": "AXS-USD",
        "ENJ": "ENJ-USD",
        "CHZ": "CHZ-USD",
        "GALA": "GALA-USD",
        "PEPE": "PEPE-USD",
        "SPELL": "SPELL-USD",
        "SUSHI": "SUSHI-USD",
        "ANKR": "ANKR-USD",
        "SKL": "SKL-USD",
        "ACH": "ACH-USD",
        "AMP": "AMP-USD",
        "OGN": "OGN-USD",
        "REN": "REN-USD",
        "CTSI": "CTSI-USD",
        "FIDA": "FIDA-USD",
        "BLZ": "BLZ-USD",
        "XCN": "XCN-USD",
    }

    SKIP_SYMBOLS: set[str] = set()

    def __init__(self, account_name: str = "Revolut Crypto"):
        self.account_name = account_name

    def transform(self, source: RevolutCryptoActivity) -> GhostfolioActivity | None:
        type_upper = source.type.upper()
        if type_upper in self.SKIP_TYPES:
            return None

        gf_type = self.TYPE_MAP.get(type_upper)
        if gf_type is None:
            return None

        if not source.symbol:
            return None

        symbol_upper = source.symbol.upper()
        if symbol_upper in self.SKIP_SYMBOLS:
            return None

        currency = self._detect_currency(source.price or source.value)
        symbol = self.SYMBOL_MAP.get(symbol_upper, f"{symbol_upper}-USD")

        return GhostfolioActivity(
            date=self._parse_date(source.date),
            symbol=symbol,
            type=gf_type,
            quantity=self._parse_float(source.quantity),
            unitPrice=self._parse_money(source.price),
            fee=self._parse_money(source.fees),
            currency=currency,
            account=self.account_name,
            dataSource="YAHOO",
        )

    @staticmethod
    def _detect_currency(value: str) -> str:
        """Detect currency from value like '€100' or '$50'."""
        if not value:
            return "EUR"
        v = value.strip()
        if v.startswith("€"):
            return "EUR"
        if v.startswith("$"):
            return "USD"
        if v.startswith("£"):
            return "GBP"
        return "EUR"

    @staticmethod
    def _parse_date(value: str) -> str:
        """Convert 'Feb 3, 2020, 9:18:39 AM' to YYYY-MM-DD."""
        if not value:
            return ""
        for fmt in [
            "%b %d, %Y, %I:%M:%S %p",
            "%B %d, %Y, %I:%M:%S %p",
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%d",
        ]:
            try:
                return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return value

    @staticmethod
    def _parse_float(value: str) -> float:
        """Parse quantity like '0.00116742' or '835,721.7759'."""
        if not value:
            return 0.0
        try:
            return float(value.replace(",", ""))
        except ValueError:
            return 0.0

    @staticmethod
    def _parse_money(value: str) -> float:
        """Parse money like '€8,565.88' or '$100.00'."""
        if not value:
            return 0.0
        v = value.strip()
        for symbol in ("€", "$", "£"):
            v = v.replace(symbol, "")
        try:
            return abs(float(v.replace(",", "")))
        except ValueError:
            return 0.0
