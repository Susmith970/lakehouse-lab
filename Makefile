.PHONY: install lint fmt test ingest clean

install:
	python -m pip install -e ".[dev]"

lint:
	ruff check src tests

fmt:
	ruff check --fix src tests
	ruff format src tests

test:
	pytest -q --cov=lakehouse --cov-report=term-missing

ingest:
	python -m lakehouse.jobs.ingest_taxi --config conf/local.yaml --month 2024-01

clean:
	rm -rf warehouse spark-warehouse metastore_db derby.log .pytest_cache .ruff_cache
