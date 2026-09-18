.PHONY: install lint fmt check test ingest dbt-run dbt-test clean

install:
	python -m pip install -e ".[dev]"

lint:
	ruff check src tests

check:
	ruff check src tests
	ruff format --check src tests

fmt:
	ruff check --fix src tests
	ruff format src tests

test:
	pytest -q --cov=lakehouse --cov-report=term-missing

ingest:
	python -m lakehouse.jobs.ingest_taxi --config conf/local.yaml --month 2024-01

dbt-run:
	cd dbt && dbt run --profiles-dir .

dbt-test:
	cd dbt && dbt test --profiles-dir .

clean:
	rm -rf warehouse spark-warehouse metastore_db derby.log .pytest_cache .ruff_cache
