"""Database models for specs configuration."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any
import json
from pathlib import Path


class ColumnType(str, Enum):
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    DATE = "date"
    DATETIME = "datetime"
    BOOLEAN = "boolean"
    MONEY = "money"


class TransformType(str, Enum):
    """Types of transformations that can be applied to field values."""
    DIRECT = "direct"              # Direct copy, no transformation
    DATE_FORMAT = "date_format"    # Convert date format
    LOOKUP = "lookup"              # Map values using a lookup table
    SUFFIX = "suffix"              # Add suffix to value
    PREFIX = "prefix"              # Add prefix to value
    FORMULA = "formula"            # Apply a formula (e.g., "quantity * price")
    CONSTANT = "constant"          # Use a constant value
    CONDITIONAL = "conditional"    # Apply value based on condition


@dataclass
class ColumnSpec:
    """Specification for a single column."""
    name: str
    type: ColumnType = ColumnType.STRING
    source_name: str | None = None  # Original column name in source file
    max_length: int | None = None
    required: bool = False
    default: Any = None
    date_format: str | None = None  # For date/datetime columns

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "type": self.type.value,
            "source_name": self.source_name,
            "max_length": self.max_length,
            "required": self.required,
            "default": self.default,
            "date_format": self.date_format,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ColumnSpec":
        return cls(
            name=data["name"],
            type=ColumnType(data.get("type", "string")),
            source_name=data.get("source_name"),
            max_length=data.get("max_length"),
            required=data.get("required", False),
            default=data.get("default"),
            date_format=data.get("date_format"),
        )


@dataclass
class FileSpec:
    """Specification for a file format (source or destination)."""
    id: str
    name: str
    description: str = ""
    default_directory: str = ""
    columns: list[ColumnSpec] = field(default_factory=list)
    delimiter: str = ","
    encoding: str = "utf-8"
    has_header: bool = True

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "default_directory": self.default_directory,
            "columns": [c.to_dict() for c in self.columns],
            "delimiter": self.delimiter,
            "encoding": self.encoding,
            "has_header": self.has_header,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "FileSpec":
        return cls(
            id=data["id"],
            name=data["name"],
            description=data.get("description", ""),
            default_directory=data.get("default_directory", ""),
            columns=[ColumnSpec.from_dict(c) for c in data.get("columns", [])],
            delimiter=data.get("delimiter", ","),
            encoding=data.get("encoding", "utf-8"),
            has_header=data.get("has_header", True),
        )


@dataclass
class FieldMapping:
    """Mapping between a source field and destination field."""
    destination_field: str
    source_field: str | None = None  # None if using constant/formula
    transform_type: TransformType = TransformType.DIRECT
    transform_config: dict = field(default_factory=dict)
    # transform_config examples:
    # - date_format: {"input_format": "%Y-%m-%dT%H:%M:%S", "output_format": "%Y-%m-%d"}
    # - lookup: {"BUY": "BUY", "SELL": "SELL", "DIVIDEND": "DIVIDEND", "_default": None}
    # - suffix: {"value": ".DE", "condition": "currency == 'EUR'"}
    # - formula: {"expression": "quantity * unit_price"}
    # - constant: {"value": "YAHOO"}
    # - conditional: {"conditions": [{"if": "type == 'crypto'", "then": "YAHOO"}, {"else": "MANUAL"}]}

    def to_dict(self) -> dict:
        return {
            "destination_field": self.destination_field,
            "source_field": self.source_field,
            "transform_type": self.transform_type.value,
            "transform_config": self.transform_config,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "FieldMapping":
        return cls(
            destination_field=data["destination_field"],
            source_field=data.get("source_field"),
            transform_type=TransformType(data.get("transform_type", "direct")),
            transform_config=data.get("transform_config", {}),
        )


@dataclass
class ETLMapping:
    """Complete mapping configuration between a source and destination."""
    id: str
    name: str
    source_id: str
    destination_id: str
    description: str = ""
    field_mappings: list[FieldMapping] = field(default_factory=list)
    filter_rules: list[dict] = field(default_factory=list)  # Rules to skip rows
    # filter_rules example: [{"field": "Type", "operator": "not_in", "values": ["DEPOSIT", "WITHDRAWAL"]}]

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "source_id": self.source_id,
            "destination_id": self.destination_id,
            "description": self.description,
            "field_mappings": [m.to_dict() for m in self.field_mappings],
            "filter_rules": self.filter_rules,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ETLMapping":
        return cls(
            id=data["id"],
            name=data["name"],
            source_id=data["source_id"],
            destination_id=data["destination_id"],
            description=data.get("description", ""),
            field_mappings=[FieldMapping.from_dict(m) for m in data.get("field_mappings", [])],
            filter_rules=data.get("filter_rules", []),
        )


class SpecStore:
    """Simple JSON-based storage for specs."""

    def __init__(self, config_dir: Path):
        self.config_dir = config_dir
        self.sources_file = config_dir / "sources.json"
        self.destinations_file = config_dir / "destinations.json"
        self.mappings_file = config_dir / "mappings.json"
        config_dir.mkdir(parents=True, exist_ok=True)

    def _load_file(self, path: Path) -> dict[str, FileSpec]:
        if not path.exists():
            return {}
        with path.open() as f:
            data = json.load(f)
        return {k: FileSpec.from_dict(v) for k, v in data.items()}

    def _save_file(self, path: Path, specs: dict[str, FileSpec]) -> None:
        with path.open("w") as f:
            json.dump({k: v.to_dict() for k, v in specs.items()}, f, indent=2)

    def _load_mappings_file(self) -> dict[str, ETLMapping]:
        if not self.mappings_file.exists():
            return {}
        with self.mappings_file.open() as f:
            data = json.load(f)
        return {k: ETLMapping.from_dict(v) for k, v in data.items()}

    def _save_mappings_file(self, mappings: dict[str, ETLMapping]) -> None:
        with self.mappings_file.open("w") as f:
            json.dump({k: v.to_dict() for k, v in mappings.items()}, f, indent=2)

    # Sources
    def get_sources(self) -> dict[str, FileSpec]:
        return self._load_file(self.sources_file)

    def get_source(self, spec_id: str) -> FileSpec | None:
        return self.get_sources().get(spec_id)

    def save_source(self, spec: FileSpec) -> None:
        specs = self.get_sources()
        specs[spec.id] = spec
        self._save_file(self.sources_file, specs)

    def delete_source(self, spec_id: str) -> bool:
        specs = self.get_sources()
        if spec_id in specs:
            del specs[spec_id]
            self._save_file(self.sources_file, specs)
            return True
        return False

    # Destinations
    def get_destinations(self) -> dict[str, FileSpec]:
        return self._load_file(self.destinations_file)

    def get_destination(self, spec_id: str) -> FileSpec | None:
        return self.get_destinations().get(spec_id)

    def save_destination(self, spec: FileSpec) -> None:
        specs = self.get_destinations()
        specs[spec.id] = spec
        self._save_file(self.destinations_file, specs)

    def delete_destination(self, spec_id: str) -> bool:
        specs = self.get_destinations()
        if spec_id in specs:
            del specs[spec_id]
            self._save_file(self.destinations_file, specs)
            return True
        return False

    # Mappings
    def get_mappings(self) -> dict[str, ETLMapping]:
        return self._load_mappings_file()

    def get_mapping(self, mapping_id: str) -> ETLMapping | None:
        return self.get_mappings().get(mapping_id)

    def get_mappings_for_source(self, source_id: str) -> list[ETLMapping]:
        return [m for m in self.get_mappings().values() if m.source_id == source_id]

    def save_mapping(self, mapping: ETLMapping) -> None:
        mappings = self.get_mappings()
        mappings[mapping.id] = mapping
        self._save_mappings_file(mappings)

    def delete_mapping(self, mapping_id: str) -> bool:
        mappings = self.get_mappings()
        if mapping_id in mappings:
            del mappings[mapping_id]
            self._save_mappings_file(mappings)
            return True
        return False
