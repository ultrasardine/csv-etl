"""Dynamic transformer that uses ETL mapping configuration."""

import csv
import re
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any


class DynamicTransformer:
    """Transforms records based on ETLMapping configuration."""

    def __init__(self, mapping_config: dict):
        """
        Initialize with mapping configuration.
        
        Args:
            mapping_config: ETLMapping.to_dict() output
        """
        self.mapping = mapping_config
        self.field_mappings = mapping_config.get("field_mappings", [])
        self.filter_rules = mapping_config.get("filter_rules", [])

    def should_skip(self, row: dict) -> bool:
        """Check if row should be filtered out."""
        for rule in self.filter_rules:
            field = rule.get("field")
            operator = rule.get("operator")
            values = rule.get("values", [])
            value = rule.get("value")

            row_value = row.get(field)

            if operator == "equals" and row_value == value:
                return True
            elif operator == "not_equals" and row_value != value:
                return True
            elif operator == "in" and row_value in values:
                return True
            elif operator == "not_in" and row_value not in values:
                return True
            elif operator == "is_empty" and not row_value:
                return True
            elif operator == "is_not_empty" and row_value:
                return True
            elif operator == "contains" and value and value in str(row_value):
                return True

        return False

    def transform_value(self, row: dict, field_mapping: dict) -> Any:
        """Transform a single field value based on mapping config."""
        transform_type = field_mapping.get("transform_type", "direct")
        source_field = field_mapping.get("source_field")
        config = field_mapping.get("transform_config", {})

        # Get source value
        source_value = row.get(source_field) if source_field else None

        if transform_type == "direct":
            return source_value

        elif transform_type == "constant":
            return config.get("value")

        elif transform_type == "date_format":
            if not source_value:
                return None
            input_fmt = config.get("input_format", "%Y-%m-%dT%H:%M:%S")
            output_fmt = config.get("output_format", "%Y-%m-%d")
            try:
                # Handle ISO format with timezone
                if "T" in str(source_value) and "Z" in str(source_value):
                    source_value = source_value.replace("Z", "")
                dt = datetime.strptime(str(source_value).split(".")[0], input_fmt)
                return dt.strftime(output_fmt)
            except ValueError:
                return source_value

        elif transform_type == "lookup":
            lookup_table = config
            default = lookup_table.get("_default")
            return lookup_table.get(source_value, default)

        elif transform_type == "suffix":
            if not source_value:
                return None
            suffix = config.get("value", "")
            condition = config.get("condition")
            if condition:
                if not self._evaluate_condition(row, condition):
                    return source_value
            return f"{source_value}{suffix}"

        elif transform_type == "prefix":
            if not source_value:
                return None
            prefix = config.get("value", "")
            condition = config.get("condition")
            if condition:
                if not self._evaluate_condition(row, condition):
                    return source_value
            return f"{prefix}{source_value}"

        elif transform_type == "formula":
            expression = config.get("expression", "")
            return self._evaluate_formula(row, expression)

        elif transform_type == "conditional":
            conditions = config.get("conditions", [])
            for cond in conditions:
                if "if" in cond:
                    if self._evaluate_condition(row, cond["if"]):
                        return cond.get("then")
                elif "else" in cond:
                    return cond["else"]
            return None

        return source_value

    def _evaluate_condition(self, row: dict, condition: str) -> bool:
        """Evaluate a simple condition string."""
        # Support: field == 'value', field != 'value', field in ['a', 'b']
        match = re.match(r"(\w+)\s*(==|!=|in)\s*(.+)", condition.strip())
        if not match:
            return False

        field, operator, value_str = match.groups()
        row_value = row.get(field)

        if operator == "==":
            expected = value_str.strip("'\"")
            return str(row_value) == expected
        elif operator == "!=":
            expected = value_str.strip("'\"")
            return str(row_value) != expected
        elif operator == "in":
            # Parse list like ['a', 'b']
            values = re.findall(r"'([^']*)'|\"([^\"]*)\"", value_str)
            values = [v[0] or v[1] for v in values]
            return str(row_value) in values

        return False

    def _evaluate_formula(self, row: dict, expression: str) -> Any:
        """Evaluate a simple formula expression."""
        # Replace field names with values
        result = expression
        for field, value in row.items():
            if field in result:
                try:
                    num_value = float(value) if value else 0
                    result = result.replace(field, str(num_value))
                except (ValueError, TypeError):
                    result = result.replace(field, "0")

        try:
            # Safe eval for simple math
            return eval(result, {"__builtins__": {}}, {})
        except Exception:
            return None

    def transform_row(self, row: dict) -> dict | None:
        """Transform a single row using field mappings."""
        if self.should_skip(row):
            return None

        result = {}
        for field_mapping in self.field_mappings:
            dest_field = field_mapping.get("destination_field")
            value = self.transform_value(row, field_mapping)
            result[dest_field] = value

        return result

    def transform_file(self, input_path: Path, output_path: Path) -> int:
        """Transform an entire CSV file."""
        results = []

        with input_path.open(encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                transformed = self.transform_row(row)
                if transformed:
                    results.append(transformed)

        if results:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with output_path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=results[0].keys())
                writer.writeheader()
                writer.writerows(results)

        return len(results)
