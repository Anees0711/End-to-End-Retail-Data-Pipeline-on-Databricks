.PHONY: lint format test clean

lint:
	pylint src/ tests/
	black --check src/ tests/
	isort --check src/ tests/

format:
	black src/ tests/
	isort src/ tests/

test:
	pytest tests/ -v

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type d -name .pytest_cache -exec rm -rf {} +
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf spark-warehouse/ metastore_db/ derby.log