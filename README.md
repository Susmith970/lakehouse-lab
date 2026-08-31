# lakehouse-lab

A working reference implementation of an open lakehouse: **PySpark → Apache Iceberg → Dremio / Snowflake**, running locally on a laptop and on AWS with the same code.

The goal is to demonstrate the patterns real data platforms need  layered configuration, testable transforms, idempotent writes, table maintenance  rather than a one-file notebook demo.

## Architecture

```
NYC TLC parquet
      │
      ▼
┌─────────────┐   pure DataFrame transforms
│   bronze    │   raw payload + lineage columns
│  (Iceberg)  │   _source_uri, _run_id, _ingested_at
└──────┬──────┘
       ▼
┌─────────────┐   typed, deduplicated, quality-gated
│   silver    │
└──────┬──────┘
       ▼
┌─────────────┐   aggregates served to BI
│    gold     │
└──────┬──────┘
       ▼
  Dremio / Snowflake
```

Storage is Iceberg throughout, so the query engine is a swappable detail: Spark writes, Dremio and Snowflake read the same tables.

## Quickstart

```bash
make install
make test
make ingest          # ingests 2024-01 into ./warehouse
```

Then inspect what landed:

```bash
python -c "
from lakehouse.config import load_config
from lakehouse.session import build_session
cfg = load_config('conf/local.yaml')
spark = build_session(cfg, master='local[*]')
spark.sql(f'SELECT * FROM {cfg.table(\"yellow_trips_bronze\")} LIMIT 5').show()
"
```

Requires Python 3.10+ and a JDK (11 or 17).

## Configuration

YAML first, environment second. Nested keys use a double underscore:

```bash
LAKEHOUSE_CATALOG__WAREHOUSE=s3://my-bucket/warehouse \
LAKEHOUSE_SHUFFLE_PARTITIONS=200 \
python -m lakehouse.jobs.ingest_taxi --config conf/aws.yaml --month 2024-01
```

Swapping `conf/local.yaml` for `conf/aws.yaml` moves the same job from a local Hadoop catalog to AWS Glue + S3. No code changes.

## Design notes

**Transforms are pure functions.** `normalize_columns`, `add_audit_columns` and `transform` take a DataFrame and return a DataFrame. Only `run()` performs IO. That means the test suite exercises real transform logic against a plain local SparkSession — no Iceberg jars, no network, no warehouse — and finishes in seconds.

**Config never reads `os.environ` from inside a job.** It is resolved once into a frozen `JobConfig` dataclass, so jobs are deterministic and testable, and misconfiguration fails at load time instead of halfway through a write.

**Layout is set at table creation only.** `tables.append()` applies partitioning when it creates a table and appends thereafter. Changing the layout of an existing table should be a deliberate `ALTER TABLE`, not a silent consequence of a write picking up new config.

**Backfills are idempotent.** Bronze is partitioned by `_partition_month`, so re-running a month uses `overwritePartitions()` rather than accumulating duplicates.

## Layout

```
src/lakehouse/
├── config.py           # YAML + env config, frozen dataclasses
├── session.py          # SparkSession + Iceberg wiring
├── tables.py           # Iceberg write helpers
├── quality.py          # quality gate framework (enforce, not_empty, null_rate, …)
└── jobs/
    ├── ingest_taxi.py  # bronze ingestion
    └── silver_taxi.py  # silver: cast, quality rules, dedup, quality gate
conf/                   # local.yaml (Hadoop catalog) / aws.yaml (Glue)
tests/                  # 31 tests, no external dependencies
```

## Roadmap

- [x] Bronze ingestion with lineage columns and Iceberg partitioning
- [x] Silver layer: typing, dedup, trip-level validity rules
- [x] Data quality gates that fail the job on contract violations
- [ ] Gold aggregates: zone-level revenue and trip duration percentiles
- [ ] dbt models over the Iceberg tables
- [ ] Dagster assets replacing the CLI entry points
- [ ] Table maintenance: compaction, snapshot expiry, orphan file cleanup
- [ ] Terraform: S3 + Glue catalog + IAM
- [ ] Dremio reflections over gold

## License

MIT
