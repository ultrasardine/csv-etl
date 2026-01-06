"""Flask application for Converter dashboard."""

import os
import sys
from pathlib import Path

# Add src to path for sibling package imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from flask import Flask, render_template, request, redirect, url_for, flash, send_file, jsonify
from werkzeug.utils import secure_filename

from .models import (
    SpecStore, FileSpec, ColumnSpec, ColumnType,
    ETLMapping, FieldMapping, TransformType
)
from converter.transformers.dynamic import DynamicTransformer


def create_app(config: dict | None = None) -> Flask:
    """Application factory."""
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-key-change-in-prod")

    # Configuration - use DATA_DIR env var or default to 'data' relative to working dir
    data_dir = os.environ.get("DATA_DIR")
    if data_dir:
        base_dir = Path(data_dir)
    elif config and config.get("BASE_DIR"):
        base_dir = Path(config.get("BASE_DIR"))
    else:
        base_dir = Path.cwd() / "data"
    
    app.config["INPUT_DIR"] = base_dir / "in"
    app.config["OUTPUT_DIR"] = base_dir / "out"
    app.config["CONFIG_DIR"] = base_dir / "config"

    # Ensure directories exist
    app.config["INPUT_DIR"].mkdir(parents=True, exist_ok=True)
    app.config["OUTPUT_DIR"].mkdir(parents=True, exist_ok=True)
    app.config["CONFIG_DIR"].mkdir(parents=True, exist_ok=True)

    # Initialize spec store
    spec_store = SpecStore(app.config["CONFIG_DIR"])

    # Create default specs and mappings if none exist
    _init_default_specs(spec_store, app.config["INPUT_DIR"], app.config["OUTPUT_DIR"])

    # =========================================================================
    # ROUTES - Dashboard
    # =========================================================================

    @app.route("/")
    def index():
        """Dashboard home page."""
        from flask import session
        
        sources = spec_store.get_sources()
        destinations = spec_store.get_destinations()
        mappings = spec_store.get_mappings()

        # Get files for each source
        source_files = {}
        for spec_id, spec in sources.items():
            dir_path = app.config["INPUT_DIR"] / spec.default_directory
            if dir_path.exists():
                source_files[spec_id] = list(dir_path.glob("*.csv"))
            else:
                source_files[spec_id] = []

        # Get output files
        output_files = {}
        for spec_id, spec in destinations.items():
            dir_path = app.config["OUTPUT_DIR"] / spec.default_directory
            if dir_path.exists():
                output_files[spec_id] = list(dir_path.glob("*.csv"))
            else:
                output_files[spec_id] = []

        # Get available mappings for each source
        source_mappings = {}
        for source_id in sources:
            source_mappings[source_id] = spec_store.get_mappings_for_source(source_id)

        # Get last process result from session
        last_process_result = session.pop("last_process_result", None)

        return render_template(
            "index.html",
            sources=sources,
            destinations=destinations,
            mappings=mappings,
            source_files=source_files,
            output_files=output_files,
            source_mappings=source_mappings,
            last_process_result=last_process_result,
        )

    # =========================================================================
    # ROUTES - File Operations
    # =========================================================================

    @app.route("/upload/<source_id>", methods=["POST"])
    def upload_file(source_id: str):
        """Upload a file to a source directory."""
        spec = spec_store.get_source(source_id)
        if not spec:
            flash(f"Source '{source_id}' not found", "error")
            return redirect(url_for("index"))

        if "file" not in request.files:
            flash("No file selected", "error")
            return redirect(url_for("index"))

        file = request.files["file"]
        if file.filename == "":
            flash("No file selected", "error")
            return redirect(url_for("index"))

        if file and file.filename.endswith(".csv"):
            filename = secure_filename(file.filename)
            upload_dir = app.config["INPUT_DIR"] / spec.default_directory
            upload_dir.mkdir(parents=True, exist_ok=True)
            file.save(upload_dir / filename)
            flash(f"File '{filename}' uploaded successfully", "success")
        else:
            flash("Only CSV files are allowed", "error")

        return redirect(url_for("index"))

    @app.route("/download/<path:filepath>")
    def download_file(filepath: str):
        """Download a processed file."""
        file_path = app.config["OUTPUT_DIR"] / filepath
        if file_path.exists():
            return send_file(file_path, as_attachment=True)
        flash("File not found", "error")
        return redirect(url_for("index"))

    @app.route("/delete/<file_type>/<path:filepath>", methods=["POST"])
    def delete_file(file_type: str, filepath: str):
        """Delete a file."""
        if file_type == "input":
            base_dir = app.config["INPUT_DIR"]
        else:
            base_dir = app.config["OUTPUT_DIR"]

        file_path = base_dir / filepath
        if file_path.exists():
            file_path.unlink()
            flash("File deleted", "success")
        else:
            flash("File not found", "error")

        return redirect(url_for("index"))

    # =========================================================================
    # ROUTES - Processing with Dynamic Mappings
    # =========================================================================

    @app.route("/process/<source_id>", methods=["POST"])
    def process_files(source_id: str):
        """Process all files for a source using selected mapping."""
        mapping_id = request.form.get("mapping_id")
        if not mapping_id:
            flash("Please select a mapping", "error")
            return redirect(url_for("index"))

        mapping = spec_store.get_mapping(mapping_id)
        if not mapping:
            flash(f"Mapping '{mapping_id}' not found", "error")
            return redirect(url_for("index"))

        source = spec_store.get_source(mapping.source_id)
        destination = spec_store.get_destination(mapping.destination_id)

        if not source or not destination:
            flash("Invalid source or destination in mapping", "error")
            return redirect(url_for("index"))

        input_dir = app.config["INPUT_DIR"] / source.default_directory
        output_dir = app.config["OUTPUT_DIR"] / destination.default_directory
        output_dir.mkdir(parents=True, exist_ok=True)

        # Use dynamic transformer with mapping config
        transformer = DynamicTransformer(mapping.to_dict())

        total_success = 0
        total_skipped = 0
        total_errors = 0
        all_errors = []
        all_logs = []

        for csv_file in input_dir.glob("*.csv"):
            output_file = output_dir / f"{csv_file.stem}_{destination.id}.csv"
            result = transformer.transform_file(csv_file, output_file, fail_on_error=True)
            total_success += result.success_count
            total_skipped += result.skipped_count
            total_errors += result.error_count
            
            # Collect errors with file context
            for error in result.errors:
                all_errors.append({
                    "file": csv_file.name,
                    "line": error.line_number,
                    "field": error.field,
                    "message": error.error_message,
                    "value": str(error.source_value) if error.source_value else "",
                    "row_data": error.row_data
                })
            
            all_logs.extend([f"[{csv_file.name}] {log}" for log in result.log_messages])

        # Store results in session for display
        from flask import session
        session["last_process_result"] = {
            "mapping_name": mapping.name,
            "success_count": total_success,
            "skipped_count": total_skipped,
            "error_count": total_errors,
            "errors": all_errors[:50],  # Limit to 50 errors for display
            "logs": all_logs[-100:],  # Keep last 100 log entries
            "has_more_errors": len(all_errors) > 50
        }

        if total_errors > 0:
            flash(f"Processed {total_success} records with {total_errors} errors using '{mapping.name}'", "warning")
        else:
            flash(f"Processed {total_success} records successfully using '{mapping.name}'", "success")
        
        return redirect(url_for("index"))

    # =========================================================================
    # ROUTES - Source Specs
    # =========================================================================

    @app.route("/sources")
    def list_sources():
        """List all source specs."""
        sources = spec_store.get_sources()
        return render_template("specs/list.html", specs=sources, spec_type="source")

    @app.route("/sources/new", methods=["GET", "POST"])
    def new_source():
        """Create a new source spec."""
        if request.method == "POST":
            spec = _spec_from_form(request.form, app.config["INPUT_DIR"])
            spec_store.save_source(spec)
            _create_spec_directory_and_template(spec, app.config["INPUT_DIR"])
            flash(f"Source '{spec.name}' created", "success")
            return redirect(url_for("list_sources"))
        return render_template("specs/edit.html", spec=None, spec_type="source", column_types=ColumnType)

    @app.route("/sources/<spec_id>/edit", methods=["GET", "POST"])
    def edit_source(spec_id: str):
        """Edit a source spec."""
        spec = spec_store.get_source(spec_id)
        if not spec:
            flash("Source not found", "error")
            return redirect(url_for("list_sources"))

        if request.method == "POST":
            updated = _spec_from_form(request.form, app.config["INPUT_DIR"])
            updated.id = spec_id
            spec_store.save_source(updated)
            _create_spec_directory_and_template(updated, app.config["INPUT_DIR"])
            flash(f"Source '{updated.name}' updated", "success")
            return redirect(url_for("list_sources"))

        return render_template("specs/edit.html", spec=spec, spec_type="source", column_types=ColumnType)

    @app.route("/sources/<spec_id>/delete", methods=["POST"])
    def delete_source(spec_id: str):
        """Delete a source spec."""
        if spec_store.delete_source(spec_id):
            flash("Source deleted", "success")
        else:
            flash("Source not found", "error")
        return redirect(url_for("list_sources"))

    # =========================================================================
    # ROUTES - Destination Specs
    # =========================================================================

    @app.route("/destinations")
    def list_destinations():
        """List all destination specs."""
        destinations = spec_store.get_destinations()
        return render_template("specs/list.html", specs=destinations, spec_type="destination")

    @app.route("/destinations/new", methods=["GET", "POST"])
    def new_destination():
        """Create a new destination spec."""
        if request.method == "POST":
            spec = _spec_from_form(request.form, app.config["OUTPUT_DIR"])
            spec_store.save_destination(spec)
            _create_spec_directory_and_template(spec, app.config["OUTPUT_DIR"])
            flash(f"Destination '{spec.name}' created", "success")
            return redirect(url_for("list_destinations"))
        return render_template("specs/edit.html", spec=None, spec_type="destination", column_types=ColumnType)

    @app.route("/destinations/<spec_id>/edit", methods=["GET", "POST"])
    def edit_destination(spec_id: str):
        """Edit a destination spec."""
        spec = spec_store.get_destination(spec_id)
        if not spec:
            flash("Destination not found", "error")
            return redirect(url_for("list_destinations"))

        if request.method == "POST":
            updated = _spec_from_form(request.form, app.config["OUTPUT_DIR"])
            updated.id = spec_id
            spec_store.save_destination(updated)
            _create_spec_directory_and_template(updated, app.config["OUTPUT_DIR"])
            flash(f"Destination '{updated.name}' updated", "success")
            return redirect(url_for("list_destinations"))

        return render_template("specs/edit.html", spec=spec, spec_type="destination", column_types=ColumnType)

    @app.route("/destinations/<spec_id>/delete", methods=["POST"])
    def delete_destination(spec_id: str):
        """Delete a destination spec."""
        if spec_store.delete_destination(spec_id):
            flash("Destination deleted", "success")
        else:
            flash("Destination not found", "error")
        return redirect(url_for("list_destinations"))

    # =========================================================================
    # ROUTES - ETL Mappings
    # =========================================================================

    @app.route("/mappings")
    def list_mappings():
        """List all ETL mappings."""
        mappings = spec_store.get_mappings()
        sources = spec_store.get_sources()
        destinations = spec_store.get_destinations()
        return render_template(
            "mappings/list.html",
            mappings=mappings,
            sources=sources,
            destinations=destinations
        )

    @app.route("/mappings/new", methods=["GET", "POST"])
    def new_mapping():
        """Create a new ETL mapping."""
        sources = spec_store.get_sources()
        destinations = spec_store.get_destinations()

        if request.method == "POST":
            mapping = _mapping_from_form(request.form)
            spec_store.save_mapping(mapping)
            flash(f"Mapping '{mapping.name}' created", "success")
            return redirect(url_for("list_mappings"))

        return render_template(
            "mappings/edit.html",
            mapping=None,
            sources=sources,
            destinations=destinations,
            transform_types=TransformType
        )

    @app.route("/mappings/<mapping_id>/edit", methods=["GET", "POST"])
    def edit_mapping(mapping_id: str):
        """Edit an ETL mapping."""
        mapping = spec_store.get_mapping(mapping_id)
        if not mapping:
            flash("Mapping not found", "error")
            return redirect(url_for("list_mappings"))

        sources = spec_store.get_sources()
        destinations = spec_store.get_destinations()

        if request.method == "POST":
            updated = _mapping_from_form(request.form)
            updated.id = mapping_id
            spec_store.save_mapping(updated)
            flash(f"Mapping '{updated.name}' updated", "success")
            return redirect(url_for("list_mappings"))

        return render_template(
            "mappings/edit.html",
            mapping=mapping,
            sources=sources,
            destinations=destinations,
            transform_types=TransformType
        )

    @app.route("/mappings/<mapping_id>/delete", methods=["POST"])
    def delete_mapping(mapping_id: str):
        """Delete an ETL mapping."""
        if spec_store.delete_mapping(mapping_id):
            flash("Mapping deleted", "success")
        else:
            flash("Mapping not found", "error")
        return redirect(url_for("list_mappings"))

    @app.route("/mappings/<mapping_id>/visual", methods=["GET"])
    def visual_mapping(mapping_id: str):
        """Visual drag-and-drop mapping editor."""
        mapping = spec_store.get_mapping(mapping_id)
        if not mapping:
            flash("Mapping not found", "error")
            return redirect(url_for("list_mappings"))

        source = spec_store.get_source(mapping.source_id)
        destination = spec_store.get_destination(mapping.destination_id)

        if not source or not destination:
            flash("Invalid source or destination in mapping", "error")
            return redirect(url_for("list_mappings"))

        return render_template(
            "mappings/visual.html",
            mapping=mapping,
            source=source,
            destination=destination,
            transform_types=TransformType
        )

    @app.route("/api/mappings/<mapping_id>/save", methods=["POST"])
    def api_save_mapping(mapping_id: str):
        """Save mapping from visual editor."""
        mapping = spec_store.get_mapping(mapping_id)
        if not mapping:
            return jsonify({"error": "Mapping not found"}), 404

        data = request.get_json()
        field_mappings = data.get("field_mappings", [])
        filter_rules = data.get("filter_rules", [])

        # Update mapping
        mapping.field_mappings = [
            FieldMapping(
                destination_field=fm["destination_field"],
                source_field=fm.get("source_field"),
                transform_type=TransformType(fm.get("transform_type", "direct")),
                transform_config=fm.get("transform_config", {}),
            )
            for fm in field_mappings
        ]
        mapping.filter_rules = filter_rules

        spec_store.save_mapping(mapping)
        return jsonify({"success": True, "message": "Mapping saved"})

    # =========================================================================
    # API Routes
    # =========================================================================

    @app.route("/api/sources")
    def api_sources():
        """Get all sources as JSON."""
        sources = spec_store.get_sources()
        return jsonify({k: v.to_dict() for k, v in sources.items()})

    @app.route("/api/sources/<source_id>/columns")
    def api_source_columns(source_id: str):
        """Get columns for a source."""
        source = spec_store.get_source(source_id)
        if not source:
            return jsonify({"error": "Source not found"}), 404
        return jsonify([c.to_dict() for c in source.columns])

    @app.route("/api/destinations")
    def api_destinations():
        """Get all destinations as JSON."""
        destinations = spec_store.get_destinations()
        return jsonify({k: v.to_dict() for k, v in destinations.items()})

    @app.route("/api/destinations/<dest_id>/columns")
    def api_destination_columns(dest_id: str):
        """Get columns for a destination."""
        dest = spec_store.get_destination(dest_id)
        if not dest:
            return jsonify({"error": "Destination not found"}), 404
        return jsonify([c.to_dict() for c in dest.columns])

    @app.route("/api/mappings")
    def api_mappings():
        """Get all mappings as JSON."""
        mappings = spec_store.get_mappings()
        return jsonify({k: v.to_dict() for k, v in mappings.items()})

    # =========================================================================
    # ROUTES - File Preview & Edit
    # =========================================================================

    @app.route("/preview/<source_id>/<filename>")
    def preview_file(source_id: str, filename: str):
        """Preview a source file with validation."""
        source = spec_store.get_source(source_id)
        if not source:
            flash("Source not found", "error")
            return redirect(url_for("index"))

        file_path = app.config["INPUT_DIR"] / source.default_directory / filename
        if not file_path.exists():
            flash("File not found", "error")
            return redirect(url_for("index"))

        # Get available mappings for this source
        mappings = spec_store.get_mappings_for_source(source_id)

        return render_template(
            "preview.html",
            source=source,
            filename=filename,
            mappings=mappings,
        )

    @app.route("/api/preview/<source_id>/<filename>")
    def api_preview_file(source_id: str, filename: str):
        """Get file data with optional validation results."""
        import csv as csv_module
        
        source = spec_store.get_source(source_id)
        if not source:
            return jsonify({"error": "Source not found"}), 404

        file_path = app.config["INPUT_DIR"] / source.default_directory / filename
        if not file_path.exists():
            return jsonify({"error": "File not found"}), 404

        # Read CSV data
        rows = []
        columns = []
        try:
            with file_path.open(encoding="utf-8") as f:
                reader = csv_module.DictReader(f)
                columns = reader.fieldnames or []
                for line_num, row in enumerate(reader, start=2):
                    rows.append({
                        "_line": line_num,
                        **row
                    })
        except Exception as e:
            return jsonify({"error": f"Failed to read file: {str(e)}"}), 500

        # Optionally validate with a mapping
        mapping_id = request.args.get("mapping_id")
        errors_by_line = {}
        validation_result = None
        
        if mapping_id:
            mapping = spec_store.get_mapping(mapping_id)
            if mapping:
                transformer = DynamicTransformer(mapping.to_dict())
                result = transformer.validate_file(file_path)
                validation_result = {
                    "success_count": result.success_count,
                    "skipped_count": result.skipped_count,
                    "error_count": result.error_count,
                    "logs": result.log_messages[-50:],
                }
                # Group errors by line number
                for error in result.errors:
                    if error.line_number not in errors_by_line:
                        errors_by_line[error.line_number] = []
                    errors_by_line[error.line_number].append({
                        "field": error.field,
                        "message": error.error_message,
                        "value": str(error.source_value) if error.source_value else "",
                    })

        return jsonify({
            "columns": columns,
            "rows": rows,
            "total": len(rows),
            "errors_by_line": errors_by_line,
            "validation": validation_result,
        })

    @app.route("/api/preview/<source_id>/<filename>/update", methods=["POST"])
    def api_update_row(source_id: str, filename: str):
        """Update a single row in the CSV file."""
        import csv as csv_module
        
        source = spec_store.get_source(source_id)
        if not source:
            return jsonify({"error": "Source not found"}), 404

        file_path = app.config["INPUT_DIR"] / source.default_directory / filename
        if not file_path.exists():
            return jsonify({"error": "File not found"}), 404

        data = request.get_json()
        line_number = data.get("line")
        updated_row = data.get("row")

        if not line_number or not updated_row:
            return jsonify({"error": "Missing line number or row data"}), 400

        # Read all rows
        rows = []
        columns = []
        try:
            with file_path.open(encoding="utf-8") as f:
                reader = csv_module.DictReader(f)
                columns = reader.fieldnames or []
                for line_num, row in enumerate(reader, start=2):
                    if line_num == line_number:
                        # Update this row
                        for col in columns:
                            if col in updated_row:
                                row[col] = updated_row[col]
                    rows.append(row)
        except Exception as e:
            return jsonify({"error": f"Failed to read file: {str(e)}"}), 500

        # Write back
        try:
            with file_path.open("w", newline="", encoding="utf-8") as f:
                writer = csv_module.DictWriter(f, fieldnames=columns)
                writer.writeheader()
                writer.writerows(rows)
        except Exception as e:
            return jsonify({"error": f"Failed to write file: {str(e)}"}), 500

        return jsonify({"success": True, "message": f"Row {line_number} updated"})

    @app.route("/api/preview/<source_id>/<filename>/convert", methods=["POST"])
    def api_convert_file(source_id: str, filename: str):
        """Convert a single file after validation passes."""
        source = spec_store.get_source(source_id)
        if not source:
            return jsonify({"error": "Source not found"}), 404

        file_path = app.config["INPUT_DIR"] / source.default_directory / filename
        if not file_path.exists():
            return jsonify({"error": "File not found"}), 404

        data = request.get_json()
        mapping_id = data.get("mapping_id")

        if not mapping_id:
            return jsonify({"error": "Missing mapping_id"}), 400

        mapping = spec_store.get_mapping(mapping_id)
        if not mapping:
            return jsonify({"error": "Mapping not found"}), 404

        destination = spec_store.get_destination(mapping.destination_id)
        if not destination:
            return jsonify({"error": "Destination not found"}), 404

        output_dir = app.config["OUTPUT_DIR"] / destination.default_directory
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"{file_path.stem}_{destination.id}.csv"

        transformer = DynamicTransformer(mapping.to_dict())
        result = transformer.transform_file(file_path, output_file, fail_on_error=True)

        if result.error_count > 0:
            return jsonify({
                "success": False,
                "message": f"Conversion failed with {result.error_count} errors",
                "errors": [
                    {
                        "line": e.line_number,
                        "field": e.field,
                        "message": e.error_message,
                    }
                    for e in result.errors[:20]
                ],
                "logs": result.log_messages,
            }), 400

        return jsonify({
            "success": True,
            "message": f"Successfully converted {result.success_count} records",
            "output_file": str(output_file.name),
            "logs": result.log_messages,
        })

    return app


def _spec_from_form(form: dict, base_dir: Path) -> FileSpec:
    """Create a FileSpec from form data."""
    spec_id = form.get("id") or form.get("name", "").lower().replace(" ", "_")

    columns = []
    col_names = form.getlist("col_name[]")
    col_types = form.getlist("col_type[]")
    col_sources = form.getlist("col_source[]")
    col_lengths = form.getlist("col_length[]")
    col_required = form.getlist("col_required[]")

    for i, name in enumerate(col_names):
        if name.strip():
            columns.append(ColumnSpec(
                name=name.strip(),
                type=ColumnType(col_types[i]) if i < len(col_types) else ColumnType.STRING,
                source_name=col_sources[i].strip() if i < len(col_sources) and col_sources[i].strip() else None,
                max_length=int(col_lengths[i]) if i < len(col_lengths) and col_lengths[i].strip() else None,
                required=str(i) in col_required,
            ))

    return FileSpec(
        id=spec_id,
        name=form.get("name", ""),
        description=form.get("description", ""),
        default_directory=form.get("default_directory", ""),
        columns=columns,
        delimiter=form.get("delimiter", ","),
        encoding=form.get("encoding", "utf-8"),
        has_header=form.get("has_header") == "on",
    )


def _create_spec_directory_and_template(spec: FileSpec, base_dir: Path) -> None:
    """Create directory and template CSV file for a spec."""
    import csv
    
    if not spec.default_directory:
        return
    
    dir_path = base_dir / spec.default_directory
    dir_path.mkdir(parents=True, exist_ok=True)
    
    # Create template file if columns are defined
    if spec.columns:
        template_file = dir_path / f"_template_{spec.id}.csv"
        
        # Generate dummy data based on column types
        def get_dummy_value(col: ColumnSpec) -> str:
            type_examples = {
                ColumnType.STRING: f"example_{col.name.lower().replace(' ', '_')}",
                ColumnType.INTEGER: "123",
                ColumnType.FLOAT: "123.45",
                ColumnType.DATE: "2024-01-15",
                ColumnType.DATETIME: "2024-01-15T10:30:00",
                ColumnType.BOOLEAN: "true",
                ColumnType.MONEY: "99.99",
            }
            return type_examples.get(col.type, "example")
        
        with template_file.open("w", newline="", encoding=spec.encoding) as f:
            writer = csv.writer(f, delimiter=spec.delimiter)
            # Header row
            writer.writerow([col.name for col in spec.columns])
            # Example data row
            writer.writerow([get_dummy_value(col) for col in spec.columns])


def _mapping_from_form(form: dict) -> ETLMapping:
    """Create an ETLMapping from form data."""
    import json
    
    mapping_id = form.get("id") or form.get("name", "").lower().replace(" ", "_")

    # Parse field mappings
    field_mappings = []
    dest_fields = form.getlist("dest_field[]")
    source_fields = form.getlist("source_field[]")
    transform_types = form.getlist("transform_type[]")
    transform_configs = form.getlist("transform_config[]")

    for i, dest_field in enumerate(dest_fields):
        if dest_field.strip():
            config = {}
            if i < len(transform_configs) and transform_configs[i].strip():
                try:
                    config = json.loads(transform_configs[i])
                except json.JSONDecodeError:
                    config = {"value": transform_configs[i]}

            field_mappings.append(FieldMapping(
                destination_field=dest_field.strip(),
                source_field=source_fields[i].strip() if i < len(source_fields) and source_fields[i].strip() else None,
                transform_type=TransformType(transform_types[i]) if i < len(transform_types) else TransformType.DIRECT,
                transform_config=config,
            ))

    # Parse filter rules
    filter_rules = []
    filter_fields = form.getlist("filter_field[]")
    filter_operators = form.getlist("filter_operator[]")
    filter_values = form.getlist("filter_value[]")

    for i, field in enumerate(filter_fields):
        if field.strip():
            value_str = filter_values[i] if i < len(filter_values) else ""
            # Try to parse as list
            if value_str.startswith("["):
                try:
                    values = json.loads(value_str)
                except json.JSONDecodeError:
                    values = [v.strip() for v in value_str.strip("[]").split(",")]
            else:
                values = [v.strip() for v in value_str.split(",")]

            filter_rules.append({
                "field": field.strip(),
                "operator": filter_operators[i] if i < len(filter_operators) else "equals",
                "values": values,
                "value": values[0] if len(values) == 1 else None,
            })

    return ETLMapping(
        id=mapping_id,
        name=form.get("name", ""),
        source_id=form.get("source_id", ""),
        destination_id=form.get("destination_id", ""),
        description=form.get("description", ""),
        field_mappings=field_mappings,
        filter_rules=filter_rules,
    )


def _init_default_specs(store: SpecStore, input_dir: Path, output_dir: Path) -> None:
    """Initialize default specs and mappings if none exist."""
    if not store.get_sources():
        # Revolut Stocks
        store.save_source(FileSpec(
            id="revolut_stocks",
            name="Revolut Stocks",
            description="Revolut stock trading CSV export",
            default_directory="revolut_stocks",
            columns=[
                ColumnSpec(name="Date", type=ColumnType.DATETIME),
                ColumnSpec(name="Ticker", type=ColumnType.STRING),
                ColumnSpec(name="Type", type=ColumnType.STRING),
                ColumnSpec(name="Quantity", type=ColumnType.FLOAT),
                ColumnSpec(name="Price per share", type=ColumnType.MONEY),
                ColumnSpec(name="Total Amount", type=ColumnType.MONEY),
                ColumnSpec(name="Currency", type=ColumnType.STRING, max_length=3),
            ],
        ))
        (input_dir / "revolut_stocks").mkdir(parents=True, exist_ok=True)

        # Revolut Crypto
        store.save_source(FileSpec(
            id="revolut_crypto",
            name="Revolut Crypto",
            description="Revolut crypto trading CSV export",
            default_directory="revolut_crypto",
            columns=[
                ColumnSpec(name="Symbol", type=ColumnType.STRING),
                ColumnSpec(name="Type", type=ColumnType.STRING),
                ColumnSpec(name="Quantity", type=ColumnType.FLOAT),
                ColumnSpec(name="Price", type=ColumnType.MONEY),
                ColumnSpec(name="Value", type=ColumnType.MONEY),
                ColumnSpec(name="Fees", type=ColumnType.MONEY),
                ColumnSpec(name="Date", type=ColumnType.DATETIME),
            ],
        ))
        (input_dir / "revolut_crypto").mkdir(parents=True, exist_ok=True)

    if not store.get_destinations():
        store.save_destination(FileSpec(
            id="ghostfolio",
            name="Ghostfolio",
            description="Ghostfolio import format",
            default_directory="ghostfolio",
            columns=[
                ColumnSpec(name="date", type=ColumnType.DATE),
                ColumnSpec(name="symbol", type=ColumnType.STRING, required=True),
                ColumnSpec(name="type", type=ColumnType.STRING, required=True),
                ColumnSpec(name="quantity", type=ColumnType.FLOAT, required=True),
                ColumnSpec(name="unitPrice", type=ColumnType.FLOAT, required=True),
                ColumnSpec(name="fee", type=ColumnType.FLOAT),
                ColumnSpec(name="currency", type=ColumnType.STRING, max_length=3),
                ColumnSpec(name="account", type=ColumnType.STRING),
                ColumnSpec(name="dataSource", type=ColumnType.STRING),
            ],
        ))
        (output_dir / "ghostfolio").mkdir(parents=True, exist_ok=True)

    # Create default mappings if none exist
    if not store.get_mappings():
        # Revolut Stocks -> Ghostfolio mapping
        store.save_mapping(ETLMapping(
            id="revolut_stocks_to_ghostfolio",
            name="Revolut Stocks → Ghostfolio",
            source_id="revolut_stocks",
            destination_id="ghostfolio",
            description="Convert Revolut stock trades to Ghostfolio format",
            field_mappings=[
                FieldMapping(
                    destination_field="date",
                    source_field="Date",
                    transform_type=TransformType.DATE_FORMAT,
                    transform_config={"input_format": "%Y-%m-%dT%H:%M:%S", "output_format": "%Y-%m-%d"}
                ),
                FieldMapping(
                    destination_field="symbol",
                    source_field="Ticker",
                    transform_type=TransformType.DIRECT
                ),
                FieldMapping(
                    destination_field="type",
                    source_field="Type",
                    transform_type=TransformType.LOOKUP,
                    transform_config={"BUY": "BUY", "SELL": "SELL", "DIVIDEND": "DIVIDEND", "_default": None}
                ),
                FieldMapping(
                    destination_field="quantity",
                    source_field="Quantity",
                    transform_type=TransformType.DIRECT
                ),
                FieldMapping(
                    destination_field="unitPrice",
                    source_field="Price per share",
                    transform_type=TransformType.DIRECT
                ),
                FieldMapping(
                    destination_field="fee",
                    source_field=None,
                    transform_type=TransformType.CONSTANT,
                    transform_config={"value": 0}
                ),
                FieldMapping(
                    destination_field="currency",
                    source_field="Currency",
                    transform_type=TransformType.DIRECT
                ),
                FieldMapping(
                    destination_field="account",
                    source_field=None,
                    transform_type=TransformType.CONSTANT,
                    transform_config={"value": "Revolut"}
                ),
                FieldMapping(
                    destination_field="dataSource",
                    source_field=None,
                    transform_type=TransformType.CONSTANT,
                    transform_config={"value": "YAHOO"}
                ),
            ],
            filter_rules=[
                {"field": "Type", "operator": "not_in", "values": ["DEPOSIT", "WITHDRAWAL", "CUSTODY FEE", "STOCK SPLIT"]}
            ]
        ))

        # Revolut Crypto -> Ghostfolio mapping
        store.save_mapping(ETLMapping(
            id="revolut_crypto_to_ghostfolio",
            name="Revolut Crypto → Ghostfolio",
            source_id="revolut_crypto",
            destination_id="ghostfolio",
            description="Convert Revolut crypto trades to Ghostfolio format",
            field_mappings=[
                FieldMapping(
                    destination_field="date",
                    source_field="Date",
                    transform_type=TransformType.DATE_FORMAT,
                    transform_config={"input_format": "%b %d, %Y, %I:%M:%S %p", "output_format": "%Y-%m-%d"}
                ),
                FieldMapping(
                    destination_field="symbol",
                    source_field="Symbol",
                    transform_type=TransformType.SUFFIX,
                    transform_config={"value": "-USD"}
                ),
                FieldMapping(
                    destination_field="type",
                    source_field="Type",
                    transform_type=TransformType.LOOKUP,
                    transform_config={"Buy": "BUY", "Sell": "SELL", "_default": None}
                ),
                FieldMapping(
                    destination_field="quantity",
                    source_field="Quantity",
                    transform_type=TransformType.DIRECT
                ),
                FieldMapping(
                    destination_field="unitPrice",
                    source_field="Price",
                    transform_type=TransformType.DIRECT
                ),
                FieldMapping(
                    destination_field="fee",
                    source_field="Fees",
                    transform_type=TransformType.DIRECT
                ),
                FieldMapping(
                    destination_field="currency",
                    source_field=None,
                    transform_type=TransformType.CONSTANT,
                    transform_config={"value": "USD"}
                ),
                FieldMapping(
                    destination_field="account",
                    source_field=None,
                    transform_type=TransformType.CONSTANT,
                    transform_config={"value": "Revolut Crypto"}
                ),
                FieldMapping(
                    destination_field="dataSource",
                    source_field=None,
                    transform_type=TransformType.CONSTANT,
                    transform_config={"value": "YAHOO"}
                ),
            ],
            filter_rules=[
                {"field": "Type", "operator": "not_in", "values": ["Transfer"]}
            ]
        ))
