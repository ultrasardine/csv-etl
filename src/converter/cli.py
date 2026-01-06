"""Command-line interface for Ghostfolio Converter."""

import sys
from pathlib import Path

from .extractors import RevolutStocksExtractor, RevolutCryptoExtractor
from .transformers import RevolutStocksTransformer, RevolutCryptoTransformer
from .loaders import GhostfolioCsvLoader
from .pipeline import ETLPipeline


# Base directories
DATA_DIR = Path("data")
INPUT_DIR = DATA_DIR / "in"
OUTPUT_DIR = DATA_DIR / "out"


def process_revolut_stocks(account_name: str | None = None) -> int:
    """Process Revolut stocks CSV files."""
    input_dir = INPUT_DIR / "revolut_stocks"
    output_dir = OUTPUT_DIR / "ghostfolio"

    if not input_dir.exists():
        return 0

    output_dir.mkdir(parents=True, exist_ok=True)
    total_count = 0

    pipeline = ETLPipeline(
        extractor=RevolutStocksExtractor(),
        transformer=RevolutStocksTransformer(account_name=account_name or "Revolut Stocks"),
        loader=GhostfolioCsvLoader(),
    )

    for csv_file in input_dir.glob("*.csv"):
        output_file = output_dir / f"{csv_file.stem}_ghostfolio.csv"
        print(f"Processing (stocks): {csv_file.name}")

        count = pipeline.run(csv_file, output_file)
        total_count += count
        print(f"  -> {output_file.name} ({count} activities)")

    return total_count


def process_revolut_crypto(account_name: str | None = None) -> int:
    """Process Revolut crypto CSV files."""
    input_dir = INPUT_DIR / "revolut_crypto"
    output_dir = OUTPUT_DIR / "ghostfolio"

    if not input_dir.exists():
        return 0

    output_dir.mkdir(parents=True, exist_ok=True)
    total_count = 0

    pipeline = ETLPipeline(
        extractor=RevolutCryptoExtractor(),
        transformer=RevolutCryptoTransformer(account_name=account_name or "Revolut Crypto"),
        loader=GhostfolioCsvLoader(),
    )

    for csv_file in input_dir.glob("*.csv"):
        output_file = output_dir / f"{csv_file.stem}_ghostfolio.csv"
        print(f"Processing (crypto): {csv_file.name}")

        count = pipeline.run(csv_file, output_file)
        total_count += count
        print(f"  -> {output_file.name} ({count} activities)")

    return total_count


def ensure_directories() -> None:
    """Create data directory structure if it doesn't exist."""
    dirs = [
        INPUT_DIR / "revolut_stocks",
        INPUT_DIR / "revolut_crypto",
        OUTPUT_DIR / "ghostfolio",
    ]
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)


def main() -> None:
    """Main entry point."""
    ensure_directories()

    account_name = sys.argv[1] if len(sys.argv) > 1 else None

    print("Ghostfolio Converter")
    print("=" * 40)
    print(f"Input:  {INPUT_DIR.absolute()}")
    print(f"Output: {OUTPUT_DIR.absolute()}")
    print("=" * 40)

    total = 0
    total += process_revolut_stocks(account_name)
    total += process_revolut_crypto(account_name)

    print("=" * 40)
    print(f"Total activities converted: {total}")


if __name__ == "__main__":
    main()
