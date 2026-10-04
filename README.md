# University Chapters Medallion Pipeline

An Azure-oriented Bronze → Silver → Gold data pipeline that ingests university chapter data from a public ArcGIS FeatureServer and publishes it as a data product.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  PUBLIC ArcGIS FeatureServer (CA / OR / WA)                         │
│  https://services2.arcgis.com/.../UniversityChapters_Public/...     │
└──────────────────────────────┬──────────────────────────────────────┘
                               │  HTTP GET (paginated, JSON)
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│  BRONZE   lake/bronze/university_chapters/<run_id>/data.json         │
│  • Raw API payload as received + ingest metadata                     │
│  • Fixture rows injected here for DQ testing                         │
│  • Immutable run-level history (new dir per run_id)                  │
└──────────────────────────────┬───────────────────────────────────────┘
                               │  PySpark (local / Databricks)
                               ▼
         ┌─────────────────────────────────────────┐
         │           DQ rules applied               │
         │                                          │
         │  DQ-Q1 (hard fail)  ──────────────────►  lake/quarantine/
         │  invalid coordinates                     university_chapters/<run_id>/
         │                                          (Parquet, includes raw payload)
         │  DQ-W1 (warning)                         │
         │  missing/UNKNOWN city                    │
         │  → dq_status='WARNING'                   │
         └──────────────┬──────────────────────────┘
                        │ clean + warned rows
                        ▼
┌──────────────────────────────────────────────────────────────────────┐
│  SILVER   lake/silver/university_chapters/  (Parquet, overwrite)     │
│  • Typed, flattened, deduped to chapter grain                        │
│  • dq_status / dq_warnings present on every row                      │
│  • NO quarantined rows                                               │
└──────────────────────────────┬───────────────────────────────────────┘
                               │  PySpark deduplicate + CA guard
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│  GOLD   lake/gold/university_chapters/v1/  (Parquet, overwrite)      │
│  • Consumer-facing data product                                      │
│  • Stable schema (see Data Product Contract)                         │
│  • WARNING rows included; quarantined rows excluded                  │
│  • Fails loudly if CA rows = 0 or whole batch is empty              │
└──────────────────────────────────────────────────────────────────────┘
```

### Technology choices

| Concern           | Choice                        | Rationale                                         |
|-------------------|-------------------------------|---------------------------------------------------|
| Compute           | **PySpark (local mode)**      | Spark-shaped transforms; promotion to Databricks / Synapse Spark requires changing only `.master()` |
| Storage layout    | ADLS-style local paths        | Mirrors `abfss://` lake structure; swap `LAKE_ROOT` for a cloud path to deploy |
| Ingest            | `requests` (Python)           | Public REST API, no auth, simple pagination       |
| Serialisation     | JSON (Bronze), Parquet (Silver/Gold/Quarantine) | Raw fidelity in Bronze; columnar efficiency for downstream |
| Idempotency       | Overwrite partition per run   | Re-running produces the same Gold; no duplicate accumulation |

---

## Prerequisites

- Python 3.10+
- Java 11+ (required by PySpark — `java -version` to confirm)

---

## Quick start

### 1. Clone & install

```bash
git clone <repo-url>
cd university-chapters-pipeline
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS/Linux
source .venv/bin/activate

pip install -r requirements.txt
```

### 2. Run the full pipeline (live API + fixtures)

```bash
python src/pipeline.py
```

This will:
1. Call the ArcGIS FeatureServer for CA / OR / WA
2. Inject 4 synthetic fixture rows from `fixtures/bronze_extras.json`
3. Write Bronze → Silver (with DQ) → Gold
4. Print DQ counts: `rows_in / rows_quarantined / rows_warned / rows_ok`

Outputs land under `lake/`:

```
lake/
  bronze/university_chapters/<run_id>/data.json
  silver/university_chapters/*.parquet
  gold/university_chapters/v1/*.parquet
  quarantine/university_chapters/<run_id>/*.parquet
```

### 3. Run without fixture injection (live data only)

```bash
python src/pipeline.py --no-fixtures
```

### 4. Re-process an existing bronze run

```bash
python src/pipeline.py --bronze-run-id 20240101T060000Z_abc12345
```

### 5. Run tests (fully offline — no API calls, no JVM required)

```bash
pytest tests/ -v
```

Expected output:

```
tests/test_dq_rules.py::TestDQQ1Quarantine::test_bad_coords_not_in_silver        PASSED
tests/test_dq_rules.py::TestDQQ1Quarantine::test_bad_coords_in_quarantine        PASSED
tests/test_dq_rules.py::TestDQQ1Quarantine::test_quarantine_has_reason_code      PASSED
tests/test_dq_rules.py::TestDQQ1Quarantine::test_null_geometry_quarantined       PASSED
tests/test_dq_rules.py::TestDQQ1Quarantine::test_clean_row_not_quarantined       PASSED
tests/test_dq_rules.py::TestDQQ1Quarantine::test_longitude_boundary_valid        PASSED
tests/test_dq_rules.py::TestDQQ1Quarantine::test_longitude_out_of_range_quarantined  PASSED
tests/test_dq_rules.py::TestDQW1Warning::test_null_city_gets_warning             PASSED
tests/test_dq_rules.py::TestDQW1Warning::test_unknown_city_gets_warning          PASSED
tests/test_dq_rules.py::TestDQW1Warning::test_blank_city_gets_warning            PASSED
tests/test_dq_rules.py::TestDQW1Warning::test_warning_row_has_reason_code        PASSED
tests/test_dq_rules.py::TestDQW1Warning::test_warning_row_present_in_silver      PASSED
tests/test_dq_rules.py::TestDQW1Warning::test_clean_row_is_ok                    PASSED
tests/test_dq_rules.py::TestDQW1Warning::test_unknown_case_insensitive           PASSED
tests/test_dq_rules.py::TestGoldNeverContainsQuarantineRows::test_bad_rows_absent_from_gold  PASSED
tests/test_dq_rules.py::TestGoldNeverContainsQuarantineRows::test_all_quarantined_raises     PASSED

16 passed in 0.05s
```

> **Note on Spark and Java compatibility** — PySpark 3.5 requires Java 8–17.
> Java 23+ removed `javax.security.auth.Subject.getSubject()` entirely (JEP 486),
> which Hadoop 3.3.x calls unconditionally. The tests therefore use pure-Python
> mirrors of the DQ logic so they run without a JVM on any machine.
> The production pipeline (`src/pipeline.py`) uses PySpark and should be run with
> Java 8/11/17, or directly on Databricks / Synapse Spark which manage their own JVM.

---

## Repository layout

```
university-chapters-pipeline/
├── src/
│   ├── pipeline.py               # Orchestrator — Bronze → Silver → Gold
│   ├── ingest/
│   │   └── bronze_ingest.py      # API fetch + raw JSON land
│   └── transforms/
│       ├── silver_transform.py   # DQ-Q1/DQ-W1 + Parquet write
│       └── gold_transform.py     # Deduplicate + CA guard + publish
├── tests/
│   └── test_dq_rules.py          # Offline DQ tests (no API calls)
├── fixtures/
│   └── bronze_extras.json        # Synthetic rows for DQ testing
├── docs/
│   └── data_product_contract.md  # Gold schema, SLA, DQ rules, versioning
├── lake/                         # Generated (gitignored) — pipeline output
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── quarantine/
├── conftest.py                   # pytest sys.path setup
├── requirements.txt
└── .gitignore
```

---

## Data Product Contract

Full contract: [`docs/data_product_contract.md`](docs/data_product_contract.md)

Summary of Gold schema:

| Column         | Type   | Notes                                       |
|----------------|--------|---------------------------------------------|
| `chapter_id`   | STRING | Stable business key — grain of the table    |
| `chapter_name` | STRING | Display name                                |
| `city`         | STRING | May be null for WARNING rows                |
| `state`        | STRING | `CA` / `OR` / `WA`                          |
| `longitude`    | DOUBLE | WGS84, validated                            |
| `latitude`     | DOUBLE | WGS84, validated                            |
| `dq_status`    | STRING | `'OK'` or `'WARNING'`                       |
| `dq_warnings`  | STRING | Pipe-delimited reason codes; `''` for OK    |
| `ingest_run_id`| STRING | Lineage back to the bronze run              |

---

## DQ rules summary

| Rule   | Severity   | Condition                                  | Outcome                        |
|--------|------------|--------------------------------------------|--------------------------------|
| DQ-Q1  | Quarantine | Invalid / missing coordinates              | Excluded from Gold; in quarantine path |
| DQ-W1  | Warning    | City is null, blank, or `UNKNOWN`          | Included in Gold with `dq_status=WARNING` |

Batch guards:
- Whole batch empty → hard fail (never silently publish empty Gold)
- CA rows drop to zero after DQ → hard fail

---

## Trade-offs and production next steps

### Chosen trade-offs

| Decision                       | Rationale                                                     | Production alternative                        |
|--------------------------------|---------------------------------------------------------------|-----------------------------------------------|
| Local Spark (`local[*]`)       | Zero-infrastructure setup for a take-home; identical code path | Change `.master()` / remove for Databricks/Synapse |
| Overwrite idempotency          | Simple; safe for a single-table data product                  | Delta Lake `MERGE` on `chapter_id` for SCD2 history |
| Bronze as JSON                 | Preserves exact API response fidelity                         | Delta Lake Bronze for ACID history + time-travel |
| Single Silver parquet write    | Sufficient for the volume                                     | Partition by `state` or `ingest_date` for large-scale pruning |
| Manual trigger                 | In scope per assignment; pipeline is runnable from README     | Azure Data Factory trigger or Databricks job schedule |

### What production would add

1. **Orchestration** — Azure Data Factory pipeline or Databricks Workflow with retry and alerting
2. **Delta Lake** throughout — ACID transactions, time-travel, schema enforcement, and `MERGE`-based idempotency
3. **Unity Catalog / Purview** — register Gold as a catalogued data product with lineage
4. **Monitoring** — DQ count metrics to Azure Monitor; alert if `rows_quarantined / rows_in > 5%`
5. **Schema evolution** — Delta automatic schema merging + contract version bump process
6. **Partitioned Bronze** — `ingest_date=YYYY-MM-DD` for cost-effective historical queries
7. **CI/CD** — GitHub Actions running `pytest` on every PR before merge
