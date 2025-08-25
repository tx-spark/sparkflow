format:
	isort .
	black .

test:
	uv run pytest tests

.PHONY: format test
