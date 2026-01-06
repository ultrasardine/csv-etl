# CSV-ETL Makefile
# A configurable ETL pipeline for converting CSV files between formats

.PHONY: help install dev run run-docker build clean test lint format

# Default target
help:
	@echo ""
	@echo "CSV-ETL - Configurable CSV ETL Pipeline"
	@echo "========================================"
	@echo ""
	@echo "Development:"
	@echo "  make install      Install dependencies using uv"
	@echo "  make dev          Run dashboard in development mode (hot reload)"
	@echo "  make test         Run tests"
	@echo "  make lint         Run linter (ruff)"
	@echo "  make format       Format code (ruff)"
	@echo ""
	@echo "Docker:"
	@echo "  make build        Build Docker images"
	@echo "  make run-docker   Start dashboard in Docker"
	@echo "  make stop         Stop Docker containers"
	@echo "  make logs         View Docker logs"
	@echo "  make restart      Restart Docker containers"
	@echo ""
	@echo "Utilities:"
	@echo "  make clean        Remove generated files and caches"
	@echo "  make shell        Open Python shell with project loaded"
	@echo ""

# =============================================================================
# Development
# =============================================================================

install:
	@echo "Installing dependencies..."
	uv pip install -e .

dev:
	@echo "Starting dashboard in development mode..."
	PYTHONPATH=src FLASK_DEBUG=1 python -m converter_dashboard.run

test:
	@echo "Running tests..."
	PYTHONPATH=src python -m pytest tests/ -v

lint:
	@echo "Running linter..."
	ruff check src/

format:
	@echo "Formatting code..."
	ruff format src/

# =============================================================================
# Docker
# =============================================================================

build:
	@echo "Building Docker images..."
	docker-compose build csv-etl-dashboard

run-docker:
	@echo "Starting dashboard in Docker..."
	docker-compose up -d csv-etl-dashboard
	@echo ""
	@echo "Dashboard running at http://localhost:5001"

stop:
	@echo "Stopping Docker containers..."
	docker-compose down

logs:
	@echo "Viewing Docker logs..."
	docker-compose logs -f csv-etl-dashboard

restart:
	@echo "Restarting Docker containers..."
	docker-compose restart csv-etl-dashboard

# =============================================================================
# Utilities
# =============================================================================

clean:
	@echo "Cleaning up..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	rm -rf .ruff_cache 2>/dev/null || true
	@echo "Done."

shell:
	@echo "Opening Python shell..."
	PYTHONPATH=src python -i -c "from converter_dashboard.app import create_app; app = create_app(); print('App loaded. Use app.test_client() to test.')"
