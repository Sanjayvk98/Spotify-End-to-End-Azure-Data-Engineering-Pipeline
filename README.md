# Spotify End-to-End Azure Data Engineering Pipeline

**A metadata-driven, CDC-based Medallion architecture pipeline that ingests a Spotify-style streaming warehouse from Azure SQL, processes it through Bronze → Silver → Gold using Databricks Autoloader and Delta Live Tables, and models it as a Star Schema in Unity Catalog — with SCD Type 2 history, Git-based CI/CD, and automated failure alerting.**

![Azure](https://img.shields.io/badge/Azure-0078D4?style=flat&logo=microsoftazure&logoColor=white)
![Data Factory](https://img.shields.io/badge/Azure%20Data%20Factory-0062AD?style=flat&logo=azuredataexplorer&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)
![Delta Live Tables](https://img.shields.io/badge/Delta%20Live%20Tables-00A972?style=flat)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Azure SQL](https://img.shields.io/badge/Azure%20SQL-CC2927?style=flat&logo=microsoftsqlserver&logoColor=white)
![Logic Apps](https://img.shields.io/badge/Logic%20Apps-0062AD?style=flat&logo=microsoftazure&logoColor=white)

---

## 📌 Overview

This project builds a production-shaped data platform around a Spotify-style streaming warehouse — users, artists, tracks, dates, and a `FactStream` listen-event table — sitting in **Azure SQL Database** as the system of record. The goal was to go beyond a one-off ETL script and demonstrate the patterns real data engineering teams use: **CDC-based incremental loading**, a **metadata-driven pipeline framework** (one pipeline definition, looped over every table), **Databricks Autoloader** for schema-evolving stream ingestion, **Delta Live Tables** with declarative data-quality expectations, **SCD Type 2** history tracking, and a **Star Schema** served through **Unity Catalog** — all version-controlled and monitored.

> Source warehouse: 500 users, 500 artists, 500 tracks, 365 calendar dates, and 1,000+ stream events, loaded via an initial bulk load and a separate CDC-driven incremental load script.

---

## 🏗️ Architecture — Medallion (Bronze → Silver → Gold)

![System Design](assets/architecture-diagram.jpg)

The platform is organized into four zones: **Data Sources** (Azure SQL + Git-backed source control) feed **Azure Data Factory orchestration**, which validates and copies data into a **Medallion Data Lake** (Bronze → Silver → Gold, with Databricks Spark transforms and a Star Schema at the Gold layer), alerted end-to-end via Monitor → Logic Apps → Email. **Azure Key Vault**, **Azure Active Directory**, and **Unity Catalog** provide governance and auditing across every stage, and the curated Gold layer is exposed to **Synapse** and **Power BI** for consumption.

![Resource Group](assets/01-resource-group.png)

| Layer | Service | What happens |
|---|---|---|
| **Source** | **Azure SQL Database** (`azuredataengsqldb`) | System-of-record star schema: `DimUser`, `DimArtist`, `DimTrack`, `DimDate`, `FactStream`. Each dimension carries an `updated_at` (or `date`/`stream_timestamp`) column used as the CDC watermark. |
| **Ingestion** | **Azure Data Factory** (`azuredataengdatafact`) | A **metadata-driven, parameter-driven pipeline** (`incremental_ingestion`) takes `schema` + `table` as parameters, looks up the last CDC watermark, copies only new/changed rows via a dynamic Copy Activity, and updates the watermark on success. A wrapping **ForEach** pipeline (`incremental_ingestion_loop`) drives this same pipeline across all 5 tables from a single config, with a Web-based failure alert on error. |
| **Bronze** | **ADLS Gen2** (`azuredataengst`, `bronze` container) | Raw Parquet landing zone, one folder per table, written as-is from ADF. |
| **Silver** | **Databricks Autoloader (Structured Streaming)** | Reads Bronze with `cloudFiles` (Autoloader), evolves schema automatically (`addNewColumns`), deduplicates on business keys (`dropDuplicates`), applies light standardization (uppercasing, regex cleanup, a derived `durationflag` bucket on track duration), and writes out as Delta tables (`spotify_data.silver.*`) with per-table checkpointing and `trigger(once=True)` micro-batches. |
| **Gold** | **Delta Live Tables (DLT)** | Declarative streaming tables per dimension/fact, with **`dlt.expect_all_or_drop`** data-quality rules (e.g. reject rows with a null `user_id`) and **`create_auto_cdc_flow`** to merge Silver changes into Gold using each table's natural key and a sequencing column. |
| **Governance/Serving** | **Unity Catalog** (`spotify_data` catalog: `bronze` → `silver` → `gold` schemas) | Final Star Schema — `DimUser`, `DimArtist`, `DimTrack`, `DimDate` (dimensions) and `FactStream` (fact) — query-ready for BI tools. |
| **Alerting** | **Azure Logic Apps + Gmail connector** | Pipeline success/failure triggers an email alert from the ADF pipeline's Web activity. |
| **Secrets/Identity** | **Access Connector for Azure Databricks** (`azuredbaccess`) | Credential-less, managed identity-based access from Databricks to ADLS Gen2 — no storage keys in notebooks. |
| **Source control** | **GitHub** (`azure-dataeng`) | `dev` → `main` PR-based workflow; ADF resources (linked services, datasets, pipelines) tracked as JSON, promoted via pull request. |
| **Deployment** | **Databricks Asset Bundles (DAB)** | `databricks.yml` defines `dev`/`prod` targets (different workspace roots, permissions, and `catalog`/`schema` variables), so the same DLT pipeline and notebooks deploy identically to both environments. |

**Data flow:**
`Azure SQL (DimUser/DimArtist/DimTrack/DimDate/FactStream) → ADF metadata-driven CDC pipeline → ADLS Bronze (Parquet) → Databricks Autoloader (Structured Streaming) → ADLS Silver (Delta) → Delta Live Tables (SCD1/SCD2 CDC merge) → Unity Catalog Gold (Star Schema)`

---

## 🔧 Pipeline Walkthrough

### 1. Source warehouse & CDC watermarking
The source is a proper star schema in Azure SQL — `DimUser`, `DimArtist`, `DimTrack`, `DimDate`, `FactStream` — seeded with an initial bulk load (500 users / 500 artists / 500 tracks / 365 dates / 1,000 stream events) and a separate incremental load script that mimics new/changed rows arriving later. Each table's CDC column is tracked centrally in a loop config (`cdc_col`: `updated_at` for the dimensions, `date` for `DimDate`, `stream_timestamp` for `FactStream`), starting from a `1900-01-01` sentinel watermark for first-time full loads.

### 2. Metadata-driven, CDC-based ingestion (Azure Data Factory)
Rather than hand-building 5 separate copy pipelines, the project uses **one parameterized pipeline** (`incremental_ingestion`) that accepts `schema` and `table` as inputs:

- **Lookup** activity reads the last stored CDC watermark for that table
- **Copy Data** activity (`AzureSqlToLake`) pulls only rows newer than the watermark from Azure SQL into ADLS Bronze
- An **If Condition** branches on whether new data arrived: on **true**, it computes the new max watermark and updates the stored CDC value; on **false**, it cleans up the empty output file so Bronze never accumulates zero-byte artifacts

![Incremental Ingestion Pipeline](assets/06-incremental-ingestion-pipeline.png)

A second pipeline, **`incremental_ingestion_loop`**, wraps the above in a **ForEach** activity driven by a JSON array of `{schema, table, cdc_col}` — so adding a 6th table to the pipeline means adding one entry to a config array, not building new pipeline logic. A **Web** activity fires an alert on failure.

![Metadata-Driven ForEach Loop](assets/07-metadata-driven-foreach-loop.png)

### 3. Bronze → Silver: Databricks Autoloader
A Databricks notebook (`silver_dimsensions`) reads each Bronze folder using **Auto Loader** (`cloudFiles` format), which incrementally and efficiently discovers new files and evolves schema automatically as new columns appear. For each table it:
- Drops the Autoloader's internal `_rescued_data` column via a small reusable utility class (`reusable.dropColumns`)
- De-duplicates on the natural key (`user_id`, `artist_id`, `track_id`, etc.)
- Applies light business logic — e.g. uppercasing `user_name`, bucketing `DimTrack.duration_sec` into a `durationflag` (`low` / `medium` / `high`), cleaning `track_name` formatting with regex
- Writes the result as a Delta table under `spotify_data.silver.*` using `trigger(once=True)` so each run processes exactly the new microbatch and stops (cost-efficient, no always-on cluster)

### 4. Silver → Gold: Delta Live Tables + SCD
Each Gold entity is a small, declarative DLT script rather than imperative merge code:

```python
import dlt

expectations = {"rule_1": "user_id IS NOT NULL"}

@dlt.table
@dlt.expect_all_or_drop(expectations)
def dimuser_stg():
    return spark.readStream.table("spotify_data.silver.dimuser")

dlt.create_streaming_table(name="dimuser", expect_all_or_drop=expectations)

dlt.create_auto_cdc_flow(
    target="dimuser", source="dimuser_stg",
    keys=["user_id"], sequence_by="updated_at",
    stored_as_scd_type=2,
)
```

| Gold table | Key | Sequenced by | SCD type |
|---|---|---|---|
| `DimUser` | `user_id` | `updated_at` | **Type 2** (full history — subscription changes tracked over time) |
| `DimTrack` | `track_id` | `updated_at` | **Type 2** |
| `DimDate` | `date_key` | `date` | **Type 2** |
| `FactStream` | `stream_id` | `stream_timestamp` | **Type 1** (latest state only — fact table, no history needed) |

`DimUser` additionally enforces a **data-quality expectation** (`user_id IS NOT NULL`) that automatically drops violating rows before they reach Gold — a declarative alternative to hand-written validation code.

![DLT Pipeline Graph](assets/11-dlt-pipeline-graph.png)
![Unity Catalog — Medallion Schemas](assets/09-unity-catalog-medallion-schemas.png)

### 5. Metadata-driven joins with Jinja2
A separate notebook demonstrates a **templated query framework**: a Python list of `{table, alias, cols, condition}` dictionaries is rendered through a Jinja2 SQL template to dynamically build a multi-table `SELECT ... LEFT JOIN` across `FactStream`, `DimUser`, and `DimTrack` — the same pattern the CDC loop uses, applied to querying instead of ingestion, so new joins are configuration changes, not new SQL to write by hand.

### 6. CI/CD — GitHub + Databricks Asset Bundles
- **ADF** resources (pipelines, datasets, linked services, triggers) are Git-connected and promoted from `dev` to `main` via pull request (PR #1: 14 commits, 11 files changed, covering the SQL/Data Lake linked services, dynamic JSON/Parquet datasets, and both ingestion pipelines).
- **Databricks** logic ships as a **Databricks Asset Bundle** (`spotify_dab`), with `databricks.yml` defining separate `dev` (developer-prefixed, paused schedules) and `prod` (fixed workspace path, explicit permissions) targets sharing the same `catalog`/`schema` variables — so the identical DLT pipeline and notebooks deploy to both environments without code changes.

![GitHub Pull Request](assets/12-github-pull-request.png)

### 7. Alerting
The ADF pipeline calls a **Logic App** (Gmail connector) on both success and failure paths, so pipeline health is visible via email without needing to check the Azure or Databricks portal.

![Logic App Workflow](assets/16-logic-app-workflow.png)

---

## 🗂️ Repository Structure
```
spotify_azure_project/
├── source_scripts/
│   ├── spotify_initial_load.sql        # DDL + bulk seed (500 users/artists/tracks, 365 dates, 1000 streams)
│   └── spotify_incremental_load.sql    # Simulated incremental/CDC batch
├── cdc.json                            # CDC watermark sentinel (1900-01-01)
├── loop_input                          # Metadata array driving the ForEach ingestion loop
└── Databricks Code/
    └── spotify_dab.dbc                 # Exported Databricks notebooks archive

spotify_dab/                            # Databricks Asset Bundle
└── spotify_dab/
    ├── databricks.yml                  # dev/prod targets, catalog & schema variables
    ├── Jinja/jinja_notebook.py         # Metadata-driven join query framework
    ├── resources/
    │   └── spotify_dab_etl.pipeline.yml
    ├── src/
    │   ├── silver/silver_dimsensions.py     # Autoloader: Bronze -> Silver (Delta)
    │   └── gold/dlt/transformations/
    │       ├── DimUser.py               # SCD2 + data-quality expectation
    │       ├── DimTrack.py              # SCD2
    │       ├── DimDate.py               # SCD2
    │       └── FactStream.py            # SCD1
    └── utils/transformations.py         # Shared `reusable` helper class

Assets/                                 # Architecture & pipeline screenshots
```

---

## ⚙️ Tech Stack

**Orchestration:** Azure Data Factory (metadata-driven pipelines, ForEach, Lookup, dynamic datasets), Azure Logic Apps
**Source:** Azure SQL Database
**Storage:** Azure Data Lake Storage Gen2 (Bronze/Silver zones)
**Compute & Processing:** Azure Databricks — Structured Streaming, Autoloader, Delta Live Tables, Unity Catalog
**Data Modeling:** Star Schema (SCD Type 1 & 2), Kimball-style Dim/Fact design
**Templating:** Jinja2 (metadata-driven SQL generation)
**Governance:** Unity Catalog external access via Databricks Access Connector (no embedded storage keys)
**DevOps:** GitHub (PR-based promotion), Databricks Asset Bundles (dev/prod parity)

---

## 🚀 Reproducing This Project

1. Provision an **Azure SQL Database** and run `source_scripts/spotify_initial_load.sql` to create and seed the star schema.
2. Provision **ADF**, **ADLS Gen2** (with `bronze`/`silver` containers), a **Databricks workspace** (Premium, for Unity Catalog), an **Access Connector for Databricks**, and a **Logic App** with an email connector.
3. Build the `incremental_ingestion` pipeline (Lookup → dynamic Copy Data → If Condition → update/clean watermark), parameterized on `schema`/`table`.
4. Wrap it in `incremental_ingestion_loop` using a ForEach over a JSON array of tables (see `loop_input`), with a Web activity calling the Logic App on failure.
5. Deploy the Databricks Asset Bundle (`databricks bundle deploy -t dev`) to create the Silver Autoloader notebook and the Gold DLT pipeline.
6. Run the Silver notebook once, then run the DLT pipeline to populate `spotify_data.gold.*`.
7. Re-run `spotify_incremental_load.sql` against the source to simulate new data, re-trigger the ADF loop, and confirm SCD2 history accumulates in `DimUser`/`DimTrack`/`DimDate` while `FactStream` stays latest-state-only.

---

## 🔭 Future Work
- Add automated CI (GitHub Actions) to run `databricks bundle deploy -t prod` on merge to `main`
- Extend DLT expectations beyond `DimUser` to all Gold tables, with `expect_all_or_fail` on critical fields
- Add a BI layer (Power BI / Databricks SQL Dashboard) on top of the Gold Star Schema
- Introduce automated backfill tooling to reprocess historical CDC ranges on demand

---

## 📬 Contact
Built by **Sanjay** — [GitHub](https://github.com/Sanjayvk98) · open to AI/ML Engineer and Data Engineering roles.
