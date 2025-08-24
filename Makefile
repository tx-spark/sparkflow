format:
	isort .
	black .

sync:
	uv sync

# Testing commands
test:
	@echo "Running all unit tests..."
	uv run pytest tests/ -m "not integration" --tb=short

test-all:
	@echo "Running all tests including integration tests..."
	uv run pytest tests/ --tb=short

test-unit:
	@echo "Running unit tests only..."
	uv run pytest tests/ -m "unit or not integration" --tb=short

test-integration:
	@echo "Running integration tests only..."
	uv run pytest tests/ -m "integration" --tb=short

test-verbose:
	@echo "Running tests with verbose output..."
	uv run pytest tests/ -v -m "not integration"

test-coverage:
	@echo "Running tests with coverage report..."
	uv run pytest tests/ -m "not integration" --cov=pipelines --cov-report=html --cov-report=term-missing

test-fast:
	@echo "Running fast tests only..."
	uv run pytest tests/ -m "not slow and not integration" --tb=line

test-watch:
	@echo "Running tests in watch mode (requires pytest-watch)..."
	uv run ptw tests/ -- -m "not integration"

test-file:
	@echo "Running tests for a specific file (usage: make test-file FILE=test_utils.py)..."
	uv run pytest tests/$(FILE) -v

test-function:
	@echo "Running a specific test function (usage: make test-function FUNC=test_function_name)..."
	uv run pytest tests/ -k "$(FUNC)" -v

# Test setup and maintenance
test-setup:
	@echo "Setting up test environment..."
	uv sync --group dev
	@echo "Test environment ready!"

test-clean:
	@echo "Cleaning test artifacts..."
	rm -rf .pytest_cache/
	rm -rf htmlcov/
	rm -rf .coverage
	find . -name "*.pyc" -delete
	find . -name "__pycache__" -type d -exec rm -rf {} +
	@echo "Test artifacts cleaned!"

# Code quality checks
lint:
	@echo "Running linting checks..."
	uv run ruff check pipelines/ tests/
	uv run black --check pipelines/ tests/
	uv run isort --check-only pipelines/ tests/

lint-fix:
	@echo "Fixing linting issues..."
	uv run ruff check --fix pipelines/ tests/
	uv run black pipelines/ tests/
	uv run isort pipelines/ tests/

# Combined quality check
check: lint test
	@echo "All quality checks completed!"

# Help target
help:
	@echo "Available test commands:"
	@echo "  test           - Run unit tests only (default)"
	@echo "  test-all       - Run all tests including integration"
	@echo "  test-unit      - Run unit tests explicitly"
	@echo "  test-integration - Run integration tests only"
	@echo "  test-verbose   - Run tests with verbose output"
	@echo "  test-coverage  - Run tests with coverage report"
	@echo "  test-fast      - Run fast tests only"
	@echo "  test-watch     - Run tests in watch mode"
	@echo "  test-file      - Run tests for specific file"
	@echo "  test-function  - Run specific test function"
	@echo "  test-setup     - Setup test environment"
	@echo "  test-clean     - Clean test artifacts"
	@echo "  lint           - Run code quality checks"
	@echo "  lint-fix       - Fix linting issues"
	@echo "  check          - Run lint + tests"
	@echo "  help           - Show this help message"

.PHONY: format sync test test-all test-unit test-integration test-verbose test-coverage test-fast test-watch test-file test-function test-setup test-clean lint lint-fix check help
