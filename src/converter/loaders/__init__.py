"""Loaders for writing output files."""

from .base import Loader
from .csv_loader import GhostfolioCsvLoader

__all__ = [
    "Loader",
    "GhostfolioCsvLoader",
]
