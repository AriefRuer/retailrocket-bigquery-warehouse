# RetailRocket Data Warehouse on Google BigQuery

An end-to-end data warehouse built on Google Cloud, using the [RetailRocket e-commerce dataset](https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset). The pipeline ingests the raw CSVs into BigQuery, transforms them through a medallion architecture (bronze → silver → gold) with **Dataform**, and models the result as a **Kimball star schema** (5 dimensions + 1 fact) ready for BI consumption. A Power BI conversion-funnel dashboard implementation is currently in the works.

**Why BigQuery:** this project began on Azure (see the predecessor: [azure-databricks-datawarehouse](https://github.com/AriefRuer/azure-databricks-datawarehouse)). When the Azure for Students subscription expired, it cut off my access to manage the entire project, hence the entire warehouse was rebuilt serverless on Google Cloud; no clusters to provision, no idle cost, and it runs entirely inside the GCP free tier (**1 TiB of queries + 10 GiB of storage per month**.

**Stack:** Google Cloud Storage · BigQuery · Dataform · GitHub Actions (Workload Identity Federation) · Power BI *(WIP)*

---

## Architecture

```
Kaggle RetailRocket CSV
        │
        ▼  manual upload
Google Cloud Storage  (gs://retailrocket_raw/retailrocket/)
        │
        ▼  bq load  (bronze/load_landing.sh)
Landing  ──  raw tables, schema autodetected, header skipped
        │
        ▼  Dataform: bronze → silver → gold + quality assertions
Gold     ──  dim_date, dim_users, dim_items, dim_categories,
        │     dim_event_type, fact_events
        ▼
Power BI  (conversion-funnel dashboard — in the works)
```

---

## Dataset: What RetailRocket Is

[RetailRocket](https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset) is an anonymized **clickstream log from a real-world e-commerce website**, published by Retail Rocket (retailrocket.io), a real-time product-recommendation platform, for one stated purpose: *"to motivate researchers in the field of recommender systems with implicit feedback."* In plain terms: it records the actions visitors actually **did** on a shop; which pages they viewed, what they added to cart, what they bought. It is the raw signal a recommender system would learn from.

The log is **raw — no content transformations — with all values hashed for confidentiality.** Only two properties survive readable: `categoryid` and `available`. Everything else (prices, text, brands) is hashed (`n5.000`-style numbers, stemmed-then-hashed words), so no price or product-name analysis is possible which is by design of the publisher, not of this pipeline. About 90% of events have matching rows in the properties file.

**Three files, 987.5 MB total (Kaggle v2):**

| File                          | Rows       | Size   | What it holds                                                                                                                                             |
| ----------------------------- | ---------- | ------ | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `events.csv`                  | 2,756,101  | 94 MB  | One row per visitor action: `timestamp, visitorid, event, itemid, transactionid`                                                                          |
| `item_properties_part1+2.csv` | 20,275,902 | 893 MB | **Change log** of item attributes over time (originally weekly snapshots, >200M rows — the publisher merged consecutive constant values, cutting it ~10×) |
| `category_tree.csv`           | 1,669      | 14 KB  | Category hierarchy in child→parent form (empty parent = root)                                                                                             |

**Event types:** `view` (2,664,312) · `addtocart` (69,332) · `transaction` (22,457)
**Coverage:** May 3 – September 18, 2015 (4.5 months) -> 139 calendar dates with activity (a 138-day elapsed span), 1,407,580 unique visitors, 417,053 unique items in the properties file.
**License:** [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/) — stated on the Kaggle dataset page.
**Publisher's own caveat:** their dataset tasks note that browsing logs can contain *"up to 40% abnormal traffic"* which is exactly why this pipeline profiles the heavy-visitor tail before any per-user analysis (see Analyst Notes).

---

## Star Schema

One fact table at the grain of a single user action, five conformed dimensions, surrogate keys from `FARM_FINGERPRINT`, `event_date`-partitioned fact with `user_sk` / `item_sk` clustering.

| Table            | Grain                     | Notable columns                                                                              |
| ---------------- | ------------------------- | -------------------------------------------------------------------------------------------- |
| `dim_date`       | One row per calendar date | 139 rows; year, month, day, day-of-week, `day_name`, `is_weekend`, month name, quarter      |
| `dim_users`      | One row per visitor       | `visitorid`, first/last event timestamps, `total_events`, `total_purchases`, `has_purchased` |
| `dim_items`      | One row per item          | `itemid`, `category_sk` (NULL when no category — see below), `is_available`                  |
| `dim_categories` | One row per category      | `category_id`, `parent_id`                                                                   |
| `dim_event_type` | One row per event type    | `view` / `addtocart` / `transaction`                                                         |
| `fact_events`    | One row per user action   | `transactionid` populated only for transactions                                              |

---

## Dataform Models: File by File

19 files: 3 source declarations, 12 table models (3 bronze, 3 silver, 6 gold), 4 assertion suites.

| Layer   | File                                 | What it does                                                                                                                     |
| ------- | ------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------- |
| sources | `sources/raw_events.sqlx`            | Declares the landing `events` table                                                                                              |
| sources | `sources/raw_category_tree.sqlx`     | Declares `category_tree` (physical table name contains a space — declared verbatim)                                              |
| sources | `sources/raw_item_properties.sqlx`   | Declares `item_properties` (same space-in-name caveat)                                                                           |
| bronze  | `bronze/events.sqlx`                 | Types timestamps (Unix **milliseconds** → `TIMESTAMP_MILLIS`), casts IDs to INT64, null-key filter + inline `nonNull` assertions |
| bronze  | `bronze/category_tree.sqlx`          | Exact pass-through — already clean at source                                                                                     |
| bronze  | `bronze/item_properties.sqlx`        | Typed change log; `timestamp` cast to INT64 only, all rows kept                                                                  |
| silver  | `silver/events.sqlx`                 | Null-time filter + deduplication on the natural event key (`QUALIFY ROW_NUMBER()`)                                               |
| silver  | `silver/category_tree.sqlx`          | Pass-through from bronze                                                                                                         |
| silver  | `silver/item_properties.sqlx`        | Keeps only the readable columns (`categoryid`, `available`) and deduplicates                                                     |
| gold    | `gold/dim_date.sqlx`                 | Date spine built from the data (139 dates) with derived calendar attributes                                                      |
| gold    | `gold/dim_users.sqlx`                | Per-visitor aggregates (events, purchases, `has_purchased`)                                                                      |
| gold    | `gold/dim_items.sqlx`                | All event items LEFT JOINed to their latest category (SCD Type 1)                                                                |
| gold    | `gold/dim_categories.sqlx`           | Category dimension with parent hierarchy                                                                                         |
| gold    | `gold/dim_event_type.sqlx`           | Three-row lookup                                                                                                                 |
| gold    | `gold/fact_events.sqlx`              | Surrogate-keyed fact; INNER JOINs to every dimension, partitioned + clustered                                                    |
| quality | `quality/row_counts.sqlx`            | Gold counts must equal silver counts                                                                                             |
| quality | `quality/unique_keys.sqlx`           | Every dimension key is unique                                                                                                    |
| quality | `quality/referential_integrity.sqlx` | Every fact key exists in its dimension                                                                                           |
| quality | `quality/business_logic.sqlx`        | Domain rules (e.g. `transactionid` only on transactions)                                                                         |

**Why INNER JOIN for the fact:** the dimensions are carved from the same silver `events` table, so when the build is correct both joins return identical rows. The paired row-count assertion makes the failure loud; a LEFT JOIN would silently emit NULL keys instead.

---

## Transformations by Layer: What and Why

Each layer answers one question. Landing: *did the files arrive intact?* Bronze: *are the types trustworthy?* Silver: *is every row one real, unique event?* Gold: *does this answer a business question?*

### Landing → Bronze: type enforcement, nothing dropped

| Transformation                                                                               | Why                                                                                                                                                                                                                                                                           |
| -------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `CAST(timestamp AS INT64)` + `TIMESTAMP_MILLIS()` → `event_timestamp` (`bronze/events.sqlx`) | CSV autodetect can land the 13-digit Unix-milliseconds value as STRING or FLOAT — either silently corrupts later date logic. Millis, not seconds, is the source's unit.                                                                                                       |
| `CAST(visitorid/itemid/transactionid AS INT64)`                                              | Same reason: IDs must never compare as strings (would break joins and sort orders).                                                                                                                                                                                           |
| `WHERE visitorid IS NOT NULL AND itemid IS NOT NULL` + inline `nonNull` assertions           | Rows without both keys cannot join to any dimension — dead weight that would fail downstream anyway. Removed early, loudly, with an assertion attached. **Verified effect: 0 rows removed** — the raw file has no null keys, so the guard costs nothing and documents intent. |
| `bronze/category_tree`: exact pass-through                                                   | Already clean at source — transforming it would add risk, not value.                                                                                                                                                                                                          |
| `bronze/item_properties`: `timestamp` cast to INT64 only                                     | Keeps the full 20.3M-row log intact at bronze (audit fidelity); filtering happens one layer later, where the analytic decision belongs.                                                                                                                                       |

### Bronze → Silver: one row = one real event

| Transformation                                                                                                                                  | Why                                                                                                                                                                                                                                                                                                                                                                           |
| ----------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `WHERE timestamp_ms IS NOT NULL`                                                                                                                | An event without a time cannot be placed on the date dimension, unusable for a time-series warehouse.                                                                                                                                                                                                                                                                        |
| Dedup: `QUALIFY ROW_NUMBER() OVER (PARTITION BY timestamp_ms, visitorid, itemid, event ORDER BY transactionid DESC) = 1` (`silver/events.sqlx`) | The natural identity of an event is *(when, who, what item, what action)* a repeat of that tuple is a logging artifact. **Verified: removes 460 rows (0.017%).** The `ORDER BY transactionid DESC` tie-break keeps the row that carries a `transactionid` over a null duplicate — this is why all 22,457 transactions survive dedup intact. |
| `WHERE property IN ('categoryid', 'available')` + dedup on `(itemid, property, timestamp_ms)` (`silver/item_properties.sqlx`)                   | Of 20,275,902 property rows, everything except these two keys is hashed and analytically opaque (publisher's design). Filtering keeps only what can be read: **2,291,853 rows (11.3%)** a 20M-row wall becomes a joinable attribute source.                                                                                                                                 |
| Inline assertions: `nonNull` on `visitorid, itemid, timestamp_ms, event`                                                                        | Guards the grain definition itself — a NULL in any of these four means the dedup key is broken, so the model refuses to build.                                                                                                                                                                                                                                                |

### Silver → Gold: star schema, deterministic keys, fail-loud joins

| Transformation                                                                                                                                                    | Why                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `dim_date`: `DISTINCT DATE(event_timestamp)` + calendar attributes (`is_weekend`, `quarter`, `day_name`…)                                                         | A calendar spine built **from the data** covers exactly the 139 dates with activity — no empty dates to filter in every chart. Derived attributes pre-computed so BI never re-derives them per query.                                                                                                                                                                                                                                                           |
| `dim_users`: `GROUP BY visitorid` → first/last event, `total_events`, `total_purchases`, `has_purchased`                                                          | Rolls session-level noise into an analyzable user grain — one row per visitor, ready for repeat-purchase segmentation.                                                                                                                                                                                                                                                                                                                                          |
| `FARM_FINGERPRINT(...)` for every surrogate key (`user_sk`, `item_sk`, `category_sk`, `event_type_sk`, `event_sk`)                                                | Deterministic: the same natural key always hashes to the same ID, so re-running a model is idempotent — facts and dimensions re-join correctly after every build. (The Azure predecessor used `monotonically_increasing_id()`, which regenerates keys on every Spark evaluation and silently breaks referential integrity — see Engineering Challenge #1 there; this migration removes that entire failure class.)                                              |
| `dim_items`: `ROW_NUMBER() ... ORDER BY timestamp_ms DESC = 1` picks each item's **latest** category, then starts from `DISTINCT itemid IN events` and LEFT JOINs | Two reasons: (1) SCD Type 1 — one row per item, latest category wins, because the dimension must stay at item grain; (2) starting from events (not the properties file) means every item the fact references exists in the dimension — **185,246 items (78.8%) get a category; the other 49,815 (21.2%) keep `category_sk = NULL` instead of being dropped**. Dropping them is the orphan-key bug from the Azure build; keeping them costs one nullable column. |
| `fact_events`: INNER JOIN to all four dimensions + `event_date` partitioning, `user_sk`/`item_sk` clustering                                                      | Dimensions are carved from the same silver `events` table, so a correct build ⇒ identical row counts on both sides — INNER JOIN cannot lose legitimate rows. If it does (proving a bug), the `row_counts` assertion fails the build instead of shipping silent NULL keys. Partition + cluster so a typical funnel query scans a day slice, not 2.76M rows — this is what keeps queries inside the 1 TiB free tier.                                              |
| `event_sk = FARM_FINGERPRINT(visitorid\|timestamp_ms\|itemid\|event)`                                                                                             | The fact's own key is the natural event identity hashed — stable across rebuilds, and directly traceable back to the source row for audit.                                                                                                                                                                                                                                                                                                                      |

### Gold → Quality: assertions as the contract

Four assertion models (`definitions/quality/`) run after every build: **`row_counts`** (gold count = silver count — catches any join that silently drops rows), **`unique_keys`** (every dimension key unique), **`referential_integrity`** (every fact key exists in its dimension), **`business_logic`** (domain rules — e.g. `transactionid` populated only on `transaction` events; verified: 22,457 non-null, zero violations). A failed assertion fails the GitHub Actions build — bad data never reaches the BI layer unnoticed.

---

## Data Integrity: Verified

Layer counts were **recomputed locally from the full Kagggle CSVs** (987.5 MB) using the same cleaning rules the SQL applies, then cross-checked against the production BigQuery/Azure SQL tables:

| Layer              | Rows      | Change from previous layer                                         |
| ------------------ | --------- | ------------------------------------------------------------------ |
| Landing (`events`) | 2,756,101 | — (matches Kaggle exactly)                                         |
| Bronze             | 2,756,101 | Null-key filter removed **0 rows** (the raw file has no null keys) |
| Silver             | 2,755,641 | **460 duplicate rows removed** (0.017%)                            |
| Gold `fact_events` | 2,755,641 | INNER JOINs + row-count assertion pass — no rows lost              |

All 22,457 transactions survive deduplication (the timestamp tie-break keeps the transaction row).

---

## What the Data Looks Like (Analyst Notes)

The raw 987.5 MB corpus was profiled before any transformation was written. What it looks like:

- **Timestamps are Unix milliseconds** (13-digit), not seconds — divide by 1000 before conversion.
- **`item_properties` is a change log, not a snapshot.** The same item appears many times as its attributes change. Of 20,275,902 rows, only **2,291,853 (11.3%)** carry readable values — 90%+ of property values are hashed for privacy, so silver keeps only `categoryid` and `available` (`available` is 0/1: 863,086 / 640,553).
- **1,104 distinct property keys** exist; `categoryid` is the only one that supports real analysis.
- **Category coverage is incomplete, by design.** Of 235,061 items seen in events, **185,246 (78.8%)** resolve to a latest category; **49,815 (21.2%)** keep `category_sk = NULL` rather than being dropped — losing them would have broken the fact table (this mirrors the orphan-key bug fixed in the Azure build).
- **`transactionid` discipline holds:** all 22,457 non-null values sit on transaction rows; zero violations (enforced by the `business_logic` assertion).
- **A small active-visitor tail distorts per-user stats:** median visitor has 1 event, p99 = 13, but the busiest visitor logged 7,757 events. The 30 visitors with >1,000 events account for **2.2% of all events** — worth filtering for per-user analysis. (The dataset publisher warns such logs can carry "up to 40% abnormal traffic".)
- **The funnel is steep:** 2.60% of views reach add-to-cart, 32.4% of carts reach purchase, 0.84% of views end in a purchase.

## What This Model Can Answer

The warehouse is designed for these questions, no findings are asserted here as of now (PowerBI Dashboard will answer an angle from these questions), but these are the capabilities the schema supports:

- View → add-to-cart → transaction funnel rates, by category, item, user segment, or time period
- Repeat-purchase behaviour per user (`has_purchased`, purchase counts in `dim_users`)
- Category performance drill-downs, including the 21.2% uncategorized tail as its own segment
- Event volume patterns across the 139-day window (daily/weekly rhythm, weekends via `is_weekend`)
- Item popularity and category mix shifts over the covered period

## CI/CD: Keyless Deployment

`.github/workflows/dataform-recompile.yml` recompiles and runs every Dataform model and assertion on each push to `main`, using **Workload Identity Federation (OIDC)** to authenticate to Google Cloud — no stored service-account keys, no secrets in the repository. Failed assertions fail the build.

## Repository Structure

```
retailrocket-bigquery-warehouse/
├── README.md
├── workflow_settings.yaml          # Dataform: project, datasets, core version
├── .github/workflows/
│   └── dataform-recompile.yml      # CI: recompile + assertions via OIDC (keyless)
├── bronze/                         # Audit record of the initial load
│   ├── load_landing.sh             #   bq load script (reproduces the GUI load)
│   ├── verify_counts.sql           #   landing counts vs Kaggle expectations
│   └── README.md                   #   job IDs, historical notes, gotchas
├── definitions/
│   ├── sources/                    # 3 source declarations
│   ├── bronze/                     # 3 typed raw models
│   ├── silver/                     # 3 cleansed/deduplicated models
│   ├── gold/                       # 6 star-schema models
│   └── quality/                    # 4 assertion suites
└── warehouse/schema/               # BigQuery DDL exported from the console
    ├── landing.sql  bronze.sql  silver.sql  gold.sql
    └── dataform_assertions.sql
```

## Getting Started

1. Upload the three CSVs from [Kaggle](https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset) to a GCS bucket
2. Set project/dataset/bucket in `bronze/load_landing.sh`, run it, then run `bronze/verify_counts.sql` (all three rows must say PASS)
3. Point `workflow_settings.yaml` at your project, `dataform compile`, then run the models in order
4. Push to `main` the GitHub Actions workflow recompiles everything (requires one-time Workload Identity Federation setup between GitHub and GCP)

## Cost

**$0** — the full corpus (987.5 MB storage, ~2.8M-row fact scans) sits inside the GCP free tier: 10 GiB storage + 1 TiB of queries per month (limits verified 8 October 2026).

## Related

The earlier Azure build of this warehouse — [azure-databricks-datawarehouse](https://github.com/AriefRuer/azure-databricks-datawarehouse) documents the same star schema on Databricks/PySpark, including the surrogate-key and orphan-item bugs whose fixes are carried forward here.
