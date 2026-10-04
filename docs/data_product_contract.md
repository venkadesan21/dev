# Data Product Contract
# University Chapters — West Coast

---

## 1. Name & Ownership

| Field            | Value                                                    |
|------------------|----------------------------------------------------------|
| **Product name** | `university_chapters_west_coast`                         |
| **Version**      | v1                                                       |
| **Technical owner** | Data Engineering team                                 |
| **Source**       | ArcGIS FeatureServer (public, no auth required)          |
| **Classification** | PUBLIC — no PII; all data sourced from a public ArcGIS API |
| **Consumer use cases** | Analytics dashboards, chapter-level reporting, geographic mapping of active university chapters |

> This data set covers **California (CA), Oregon (OR), and Washington (WA)** only.
> The upstream view already filters `Status = 'ACTIVE'`.

---

## 2. Interface

### Table / path

```
lake/gold/university_chapters/v1/   (Parquet, overwrite on each run)
```

For Databricks / Synapse environments register as an external table:
```sql
CREATE TABLE gold.university_chapters_v1
USING PARQUET
LOCATION 'abfss://gold@<storage>.dfs.core.windows.net/university_chapters/v1/';
```

### Schema

| Column         | Type    | Nullable | Description                                                  |
|----------------|---------|----------|--------------------------------------------------------------|
| `chapter_id`   | STRING  | No       | Stable business key (e.g. `CA-0355`). Grain of the table.   |
| `chapter_name` | STRING  | Yes      | Display name of the university chapter.                      |
| `city`         | STRING  | Yes      | City; may be null/UNKNOWN for rows with `dq_status=WARNING`. |
| `state`        | STRING  | No       | USPS 2-letter code. Values: `CA`, `OR`, `WA`.               |
| `longitude`    | DOUBLE  | No       | WGS84 longitude (validated: −180 ≤ lon ≤ 180).              |
| `latitude`     | DOUBLE  | No       | WGS84 latitude (validated: −90 ≤ lat ≤ 90).                 |
| `dq_status`    | STRING  | No       | `'OK'` or `'WARNING'`. Never `null`.                         |
| `dq_warnings`  | STRING  | No       | Pipe-delimited DQ reason codes; `''` (empty) for OK rows.    |
| `ingest_run_id`| STRING  | No       | Lineage key — links back to the bronze run that produced this row. |

### Grain

One row per **chapter_id**. If the same chapter_id appears in multiple ingest
runs (e.g. after a re-run), only the most recent copy is kept in Gold (deduplicated
by latest `ingest_run_id`).

### `dq_warnings` values

| Code                       | Meaning                                         |
|----------------------------|-------------------------------------------------|
| `MISSING_OR_UNKNOWN_CITY`  | `city` was null, blank, or the literal `UNKNOWN` |

---

## 3. Freshness SLA

| Metric               | Target                                              |
|----------------------|-----------------------------------------------------|
| Refresh cadence      | Daily                                               |
| Delivery deadline    | 06:00 UTC each day                                  |
| Latency (API → Gold) | < 30 minutes under normal conditions                |

> Current implementation is manually triggered (`python src/pipeline.py`).
> Production deployment would add an Azure Data Factory trigger or Databricks
> job schedule to meet the SLA automatically.

---

## 4. Data Quality Rules

### DQ-Q1 — Invalid Coordinates (QUARANTINE / hard fail)

| Property       | Value                                                                               |
|----------------|-------------------------------------------------------------------------------------|
| **Rule ID**    | DQ-Q1                                                                               |
| **Severity**   | Quarantine — the row is **excluded** from Silver and Gold                           |
| **Condition**  | `longitude` or `latitude` is null, non-numeric, or outside valid ranges (lon ∉ [−180,180] or lat ∉ [−90,90]) |
| **Action**     | Row written to `lake/quarantine/university_chapters/<run_id>/` with `dq_reason_code = 'INVALID_COORDINATES'`, `ingest_run_id`, and full raw payload for debugging |
| **Consumer impact** | Quarantined rows **never** appear in Gold                                      |

### DQ-W1 — Missing or Unknown City (WARNING / soft fail)

| Property       | Value                                                                               |
|----------------|-------------------------------------------------------------------------------------|
| **Rule ID**    | DQ-W1                                                                               |
| **Severity**   | Warning — the row **is published** to Silver and Gold                               |
| **Condition**  | `city` is null, blank, or the case-insensitive literal `UNKNOWN`                   |
| **Action**     | Row published with `dq_status = 'WARNING'` and `dq_warnings = 'MISSING_OR_UNKNOWN_CITY'` |
| **Consumer impact** | Row is available but consumers should be aware the city field is unreliable    |

### Batch-level rules

| Rule                          | Behaviour                                                                        |
|-------------------------------|----------------------------------------------------------------------------------|
| OR / WA rows = 0              | **Acceptable** — treated as a data fact, not a pipeline failure                  |
| Whole batch empty             | **Hard fail** — pipeline aborts; empty Gold is never silently published           |
| CA rows drop to 0 after DQ    | **Hard fail** — `assert_ca_present()` raises `RuntimeError` before Gold write    |

---

## 5. Versioning

Breaking schema changes (column removal, rename, type change) increment the
path version: `gold/university_chapters/v2/`.

- Additive changes (new nullable columns) are **non-breaking** and applied in-place.
- Consumers should reference the versioned path (`/v1/`) rather than the parent directory.
- A deprecation notice will be communicated at least **30 days** before v1 is removed.

---

## 6. Classification

| Property               | Value                                             |
|------------------------|---------------------------------------------------|
| Data sensitivity       | **PUBLIC**                                        |
| PII present            | No                                                |
| Source licence         | Public ArcGIS FeatureServer (Ducks Unlimited GIS) |
| Retention              | Bronze: unlimited (immutable run history); Gold: latest run (overwrite) |
| Secrets / credentials  | None required — public API, no authentication     |
