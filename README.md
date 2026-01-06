# CSV-ETL

A configurable ETL (Extract, Transform, Load) pipeline for converting CSV files between different formats. Define your source and destination file structures, create mappings with transformations, and process files through a web dashboard.

## Features

- **Web Dashboard** - Upload, process, and download files through a browser interface
- **Configurable Sources & Destinations** - Define CSV file structures with column names and types
- **Flexible Mappings** - Map source fields to destination fields with transformations
- **Transform Types** - Direct copy, constants, date formatting, value lookups, prefix/suffix, formulas
- **Filter Rules** - Skip rows based on conditions (e.g., exclude deposits/withdrawals)
- **Template Generation** - Auto-generate example CSV files when creating specs

## Quick Start

### Using Docker (Recommended)

```bash
# Build and start the dashboard
make build
make run-docker

# Open http://localhost:5001
```

### Local Development

```bash
# Install dependencies
make install

# Run in development mode
make dev

# Open http://localhost:5000
```

## Architecture

```
csv-etl/
├── src/
│   ├── converter/              # Core ETL library
│   │   ├── models/             # Data models (source records, destination records)
│   │   ├── extractors/         # CSV parsers for each source type
│   │   ├── transformers/       # Format converters
│   │   │   └── dynamic.py      # Generic transformer using mapping configs
│   │   ├── loaders/            # Output writers
│   │   └── pipeline.py         # ETL orchestration
│   │
│   └── converter_dashboard/    # Flask web application
│       ├── app.py              # Routes and application factory
│       ├── models.py           # Spec and mapping storage (JSON-based)
│       ├── run.py              # Server entry point
│       └── templates/          # HTML templates
│
├── data/
│   ├── in/                     # Input files organized by source
│   │   ├── revolut_stocks/
│   │   └── revolut_crypto/
│   ├── out/                    # Output files organized by destination
│   │   └── ghostfolio/
│   └── config/                 # JSON configuration files
│       ├── sources.json
│       ├── destinations.json
│       └── mappings.json
│
├── Makefile                    # Development and deployment tasks
├── docker-compose.yml          # Docker services
└── pyproject.toml              # Python project configuration
```

## Usage Guide

### 1. Create a Source Spec

A source defines the structure of your input CSV files.

1. Go to **Sources** → **New Source**
2. Fill in:
   - **Name**: Human-readable name (e.g., "Revolut Stocks")
   - **Default Directory**: Folder name under `data/in/` (e.g., "revolut_stocks")
   - **Columns**: Define each column with name and type

Column types:
- `string` - Text values
- `integer` - Whole numbers
- `float` - Decimal numbers
- `date` - Date only (YYYY-MM-DD)
- `datetime` - Date and time
- `money` - Currency amounts
- `boolean` - True/false values

### 2. Create a Destination Spec

A destination defines the structure of your output CSV files.

1. Go to **Destinations** → **New Destination**
2. Define the output format columns and types

### 3. Create a Mapping

A mapping connects a source to a destination and defines how fields are transformed.

1. Go to **Mappings** → **New Mapping**
2. Select the **Source** and **Destination**
3. Add **Field Mappings**:

| Transform Type | Description | Config Example |
|---------------|-------------|----------------|
| `direct` | Copy value as-is | (none needed) |
| `constant` | Use a fixed value | `{"value": "USD"}` |
| `date_format` | Convert date format | `{"input_format": "%Y-%m-%dT%H:%M:%S", "output_format": "%Y-%m-%d"}` |
| `lookup` | Map values | `{"BUY": "B", "SELL": "S", "_default": null}` |
| `suffix` | Add text after value | `{"value": "-USD"}` |
| `prefix` | Add text before value | `{"value": "STK_"}` |
| `formula` | Calculate from fields | `{"expression": "Quantity * Price"}` |

4. Add **Filter Rules** to skip unwanted rows:
   - Field: The source column to check
   - Operator: `equals`, `not_equals`, `in`, `not_in`, `is_empty`, `contains`
   - Values: Comma-separated list for `in`/`not_in`

### 4. Process Files

1. Upload CSV files to a source on the **Dashboard**
2. Select a mapping from the dropdown
3. Click **Process**
4. Download the converted files from the destination section

## Configuration Storage

All configuration is stored as JSON files in `data/config/`:

- `sources.json` - Source file specifications
- `destinations.json` - Destination file specifications  
- `mappings.json` - ETL mappings with field transforms and filters

This makes it easy to version control your configurations or share them between environments.

## Example: Revolut to Ghostfolio

The project includes pre-configured mappings for converting Revolut export files to Ghostfolio import format:

**Revolut Stocks → Ghostfolio**
- Converts date format from ISO to YYYY-MM-DD
- Maps transaction types (BUY, SELL, DIVIDEND)
- Filters out deposits, withdrawals, custody fees, stock splits
- Sets data source to YAHOO

**Revolut Crypto → Ghostfolio**
- Adds `-USD` suffix to crypto symbols (e.g., `BTC` → `BTC-USD`)
- Converts date from "Jan 15, 2024, 10:30:00 AM" format
- Filters out transfers

## API Endpoints

The dashboard exposes REST APIs for integration:

```
GET  /api/sources                    # List all sources
GET  /api/sources/<id>/columns       # Get source columns
GET  /api/destinations               # List all destinations
GET  /api/destinations/<id>/columns  # Get destination columns
GET  /api/mappings                   # List all mappings
```

## Development

```bash
# Run linter
make lint

# Format code
make format

# Run tests
make test

# Open Python shell with app loaded
make shell

# Clean up caches
make clean
```

## Docker

```bash
# Build images
make build

# Start dashboard
make run-docker

# View logs
make logs

# Stop containers
make stop

# Restart
make restart
```

## License

MIT
