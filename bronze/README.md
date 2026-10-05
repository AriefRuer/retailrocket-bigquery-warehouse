# Bronze — Ingestion Scripts (Audit Record)

This folder documents the initial load of RetailRocket raw CSVs from GCS into BigQuery. It is a reproducibility and audit artifact — the pipeline's downstream transformations (bronze → silver → gold) are handled by Dataform.

## What Was Loaded

| Table | Source | Row count | Expected (Kaggle) |
|---|---|---|---|
| events | `gs://retailrocket_raw/retailrocket/events.csv` | 2,756,101 | 2,756,101 ✅ |
| category_tree | `gs://retailrocket_raw/retailrocket/category_tree.csv` | 1,669 | 1,669 ✅ |
| item_properties | `gs://retailrocket_raw/retailrocket/item_properties_part*.csv` | 20,275,902 | 20,275,902 ✅ |

All three tables loaded via the BigQuery Console GUI on 17 September 2026.

## Load Job Metadata

| Job ID | Destination | Rows | Status |
|---|---|---|---|
| `bquxjob_56283a17_1a0b039537b` | events | 2,756,101 | SUCCESS |
| `bquxjob_42e85b52_1a0b03ab819` | category_tree | 1,669 | SUCCESS |
| `bquxjob_7d3f4894_1a0b03b784c` | item_properties | 20,275,902 | SUCCESS |

## Historical Notes

**Destination dataset:** Load jobs recorded `retailrocket_bronze` as the destination, reflecting the state at job run time. The tables were subsequently copied to `retailrocket_landing`, which is where they now live. The job metadata is historical and immutable — the mismatch is expected.

**Table names:** Two tables were initially created with spaces (`"category tree"`, `"item properties"`) instead of underscores. The current dataset contains the intended underscore names.

**Bucket path:** The actual GCS path is `gs://retailrocket_raw/retailrocket/`. Earlier drafts of the migration plan referenced `gs://retailrocket-raw/` — the underscore-vs-hyphen difference is a documentation correction, not a data issue.

**Header row handling:** The BigQuery GUI was configured to skip the header row. Confirmed by two independent checks:
1. Row counts match Kaggle exactly (no extra row)
2. Schema types are `INTEGER` for numeric columns (header text would have forced `STRING`)

**Write disposition:** `WRITE_EMPTY` — the load would fail if tables already existed. This was appropriate for the one-time initial load.

## Files

| File | Purpose |
|---|---|
| `load_bronze.sh` | Reproduces the GUI load via `bq load` — reflects the intended state, not the exact GUI clicks |
| `verify_counts.sql` | Validates all three tables against Kaggle-documented row counts |

## Re-running

`load_bronze.sh` uses `WRITE_EMPTY` — it will fail if the target tables exist. To re-run, drop the tables first or change the write disposition to `WRITE_TRUNCATE`.