"""Dynamic transformer that uses ETL mapping configuration."""

import csv
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class RowError:
    """Represents an error that occurred while processing a row."""
    line_number: int
    field: str
    error_message: str
    source_value: Any = None
    row_data: dict = field(default_factory=dict)


@dataclass
class TransformResult:
    """Result of a file transformation."""
    success_count: int = 0
    skipped_count: int = 0
    error_count: int = 0
    errors: list[RowError] = field(default_factory=list)
    log_messages: list[str] = field(default_factory=list)

    def add_log(self, message: str):
        self.log_messages.append(message)
        logger.info(message)

    def add_error(self, error: RowError):
        self.errors.append(error)
        self.error_count += 1
        logger.error(f"Line {error.line_number}: {error.error_message} (field: {error.field}, value: {error.source_value})")


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

    def transform_value(self, row: dict, field_mapping: dict, line_number: int = 0) -> tuple[Any, RowError | None]:
        """Transform a single field value based on mapping config.
        
        Returns:
            Tuple of (transformed_value, error_or_none)
        """
        transform_type = field_mapping.get("transform_type", "direct")
        source_field = field_mapping.get("source_field")
        dest_field = field_mapping.get("destination_field", "unknown")
        config = field_mapping.get("transform_config", {})

        # Get source value
        source_value = row.get(source_field) if source_field else None

        try:
            if transform_type == "direct":
                return source_value, None

            elif transform_type == "constant":
                return config.get("value"), None

            elif transform_type == "date_format":
                if not source_value:
                    return None, None
                input_fmt = config.get("input_format", "%Y-%m-%dT%H:%M:%S")
                output_fmt = config.get("output_format", "%Y-%m-%d")
                try:
                    # Handle ISO format with timezone
                    if "T" in str(source_value) and "Z" in str(source_value):
                        source_value = source_value.replace("Z", "")
                    dt = datetime.strptime(str(source_value).split(".")[0], input_fmt)
                    return dt.strftime(output_fmt), None
                except ValueError as e:
                    error = RowError(
                        line_number=line_number,
                        field=dest_field,
                        error_message=f"Date format error: {e}",
                        source_value=source_value,
                        row_data=dict(row)
                    )
                    return source_value, error

            elif transform_type == "lookup":
                lookup_table = config
                default = lookup_table.get("_default")
                result = lookup_table.get(source_value, default)
                if result is None and source_value is not None and source_value not in lookup_table:
                    error = RowError(
                        line_number=line_number,
                        field=dest_field,
                        error_message=f"Lookup value not found in mapping table",
                        source_value=source_value,
                        row_data=dict(row)
                    )
                    return result, error
                return result, None

            elif transform_type == "suffix":
                if not source_value:
                    return None, None
                suffix = config.get("value", "")
                condition = config.get("condition")
                if condition:
                    if not self._evaluate_condition(row, condition):
                        return source_value, None
                return f"{source_value}{suffix}", None

            elif transform_type == "prefix":
                if not source_value:
                    return None, None
                prefix = config.get("value", "")
                condition = config.get("condition")
                if condition:
                    if not self._evaluate_condition(row, condition):
                        return source_value, None
                return f"{prefix}{source_value}", None

            elif transform_type == "formula":
                expression = config.get("expression", "")
                result = self._evaluate_formula(row, expression)
                if result is None:
                    error = RowError(
                        line_number=line_number,
                        field=dest_field,
                        error_message=f"Formula evaluation failed: {expression}",
                        source_value=source_value,
                        row_data=dict(row)
                    )
                    return result, error
                return result, None

            elif transform_type == "conditional":
                conditions = config.get("conditions", [])
                for cond in conditions:
                    if "if" in cond:
                        if self._evaluate_condition(row, cond["if"]):
                            return cond.get("then"), None
                    elif "else" in cond:
                        return cond["else"], None
                return None, None

            return source_value, None
            
        except Exception as e:
            error = RowError(
                line_number=line_number,
                field=dest_field,
                error_message=f"Unexpected error: {str(e)}",
                source_value=source_value,
                row_data=dict(row)
            )
            return None, error

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

    def transform_row(self, row: dict, line_number: int = 0) -> tuple[dict | None, list[RowError]]:
        """Transform a single row using field mappings.
        
        Returns:
            Tuple of (transformed_row_or_none, list_of_errors)
        """
        if self.should_skip(row):
            return None, []

        result = {}
        errors = []
        for field_mapping in self.field_mappings:
            dest_field = field_mapping.get("destination_field")
            value, error = self.transform_value(row, field_mapping, line_number)
            result[dest_field] = value
            if error:
                errors.append(error)

        return result, errors

    def validate_file(self, input_path: Path) -> TransformResult:
        """Validate a CSV file without writing output (dry-run mode).
        
        Returns:
            TransformResult with validation results and row-level error details
        """
        return self._process_file(input_path, output_path=None, dry_run=True)

    def transform_file(self, input_path: Path, output_path: Path, fail_on_error: bool = True) -> TransformResult:
        """Transform an entire CSV file with verbose logging.
        
        Args:
            input_path: Source CSV file
            output_path: Destination CSV file
            fail_on_error: If True, don't write output if any errors occur
        
        Returns:
            TransformResult with success/error counts and detailed error info
        """
        return self._process_file(input_path, output_path, dry_run=False, fail_on_error=fail_on_error)

    def _process_file(self, input_path: Path, output_path: Path | None, dry_run: bool = False, fail_on_error: bool = True) -> TransformResult:
        """Internal method to process a CSV file.
        
        Args:
            input_path: Source CSV file
            output_path: Destination CSV file (None for dry-run)
            dry_run: If True, only validate without writing
            fail_on_error: If True, don't write output if any errors occur
        """
        result = TransformResult()
        results = []

        mode = "Validating" if dry_run else "Transforming"
        result.add_log(f"{mode} {input_path.name}")
        result.add_log(f"Using mapping: {self.mapping.get('name', 'Unknown')}")
        result.add_log(f"Field mappings: {len(self.field_mappings)}, Filter rules: {len(self.filter_rules)}")

        try:
            with input_path.open(encoding="utf-8") as f:
                reader = csv.DictReader(f)
                result.add_log(f"Source columns: {reader.fieldnames}")
                
                for line_number, row in enumerate(reader, start=2):  # Start at 2 (1 for header)
                    try:
                        transformed, row_errors = self.transform_row(row, line_number)
                        
                        for error in row_errors:
                            result.add_error(error)
                        
                        if transformed:
                            results.append(transformed)
                            if not row_errors:
                                result.success_count += 1
                                result.add_log(f"Line {line_number}: {'Valid' if dry_run else 'Transformed successfully'}")
                            else:
                                result.add_log(f"Line {line_number}: Has {len(row_errors)} error(s)")
                        else:
                            result.skipped_count += 1
                            result.add_log(f"Line {line_number}: Skipped (filtered out)")
                            
                    except Exception as e:
                        error = RowError(
                            line_number=line_number,
                            field="*",
                            error_message=f"Row processing failed: {str(e)}",
                            row_data=dict(row)
                        )
                        result.add_error(error)

        except Exception as e:
            result.add_log(f"ERROR: Failed to read input file: {str(e)}")
            return result

        # Write output only if not dry-run and (no errors or fail_on_error is False)
        if not dry_run and output_path and results:
            if result.error_count > 0 and fail_on_error:
                result.add_log(f"OUTPUT SKIPPED: {result.error_count} errors found. Fix errors before converting.")
            else:
                output_path.parent.mkdir(parents=True, exist_ok=True)
                with output_path.open("w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=results[0].keys())
                    writer.writeheader()
                    writer.writerows(results)
                result.add_log(f"Output written to {output_path.name}")

        status = "Validation" if dry_run else "Transformation"
        result.add_log(f"{status} complete: {result.success_count} valid, {result.skipped_count} skipped, {result.error_count} errors")
        
        return result
