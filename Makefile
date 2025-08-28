format:
	isort .
	black .

test:
	uv run pytest tests

test-verbose:
	uv run pytest -vv tests

.PHONY: format test
