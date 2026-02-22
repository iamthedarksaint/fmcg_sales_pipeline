# FMCG Sales Data Pipeline

An end-to-end data engineering pipeline for a Nigerian FMCG company's 2-year sales dataset. Built with Python, PostgreSQL, dbt, Apache Airflow, and Docker.

---

## Architecture

```
Excel Workbook (.xlsx)
        │
        ▼
  Python ETL (Extract → Transform → Load)
        │
        ▼
  PostgreSQL — raw schema (star schema)
  ┌─────────────────────────────────────────┐
  │  dim_products      dim_distributors     │
  │  dim_salespersons  dim_date             │
  │  fct_transactions  fct_monthly_targets  │
  └─────────────────────────────────────────┘
        │
        ▼
    dbt Models
  ┌─────────────────────────────────────────┐
  │  staging/  — views (clean & cast)       │
  │  marts/    — tables (business logic)    │
  │  gold/     — tables (business questions)│
  └─────────────────────────────────────────┘
        │
        ▼
  Apache Airflow DAG (orchestration)
        │
        ▼
  Docker Compose (Airflow + PostgreSQL)
```

---

## Project Structure

```
fmcg-sales-pipeline/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env.example
├── .gitignore
├── README.md
│
├── dags/
│   ├── fmcg_sales_pipeline.py
│   └── include/
│       ├── etl/
│       │   ├── extract.py
│       │   ├── transform.py
│       │   └── load.py
│       ├── config.py
│       └── notifications/
│           └── notifications.py
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
|   ├── macros/
│       ├── generate_schema_name.sql
│   └── models/
│       ├── staging/
│       │   ├── stg_transactions.sql
│       │   ├── stg_products.sql
│       │   ├── stg_distributors.sql
│       │   ├── stg_salespersons.sql
│       │   ├── stg_monthly_targets.sql
│       │   ├── stg_date_table.sql
│       │   └── schema.yml
│       ├── marts/
│       │   ├── mart_sales_performance.sql
│       │   ├── mart_distributor_product_summary.sql
│       │   └── schema.yml
│       └── gold/
│           ├── top_products_2024.sql
│           ├── region_mom_growth_q3_2024.sql
│           ├── salesperson_achievement.sql
│           ├── distributor_return_rate.sql
│           ├── rolling_revenue_by_category.sql
│           └── schema.yml
│
└── data/
    └── FMN_DE_Sales_Assessment_Dataset.xlsx
```

---

## Schema Design

The pipeline uses a **star schema** in the `raw` schema with clearly separated fact and dimension tables.

| Table | Type | Description |
|---|---|---|
| `dim_products` | Dimension | 18 product SKUs across 5 categories |
| `dim_distributors` | Dimension | 15 distributor accounts across 6 regions |
| `dim_salespersons` | Dimension | 15 field reps with monthly targets |
| `dim_date` | Dimension | Calendar table spanning 2023–2024 |
| `fct_transactions` | Fact | 3,500 sales transaction records |
| `fct_monthly_targets` | Fact | Monthly target vs actual per salesperson |

---

## Data Quality Handling

| Issue | Approach |
|---|---|
| 66 NULL `distributor_id` in Transactions | Flagged with `has_missing_distributor` boolean. Rows retained for auditability and excluded from distributor-level aggregations |
| `achievement_pct` blank in Monthly_Targets | Computed during ETL as `actual_revenue / target_revenue * 100`. Division-by-zero guarded with `NULLIF` |
| `Returned` records in `transaction_status` | Retained in raw table with `_is_returned` flag. All revenue models filter `WHERE NOT is_returned` |
| 1,504 NULL `notes` values | Coalesced to empty string to avoid NULL propagation issues downstream |
| Column names with spaces | Normalised to `snake_case` during transform |
| Computed columns (`revenue_ngn`, `discount_amount_ngn`, `gross_profit_ngn`) | Re-derived from base fields to catch any source formula errors |

---

## Incremental Load Logic

- **Dimension tables** (`dim_*`) — Full replace on each run. Tables are small (15–18 rows) and simpler to maintain consistency with a full reload
- **Fact tables** (`fct_transactions`, `fct_monthly_targets`) — Incremental append. Existing primary keys are fetched before each load and only new rows are inserted, avoiding duplicates without requiring `MERGE`

---

## dbt Layers

### Staging (views)
Mirrors raw tables with clean column names, correct types, and no business logic. One model per source table.

| Model | Description |
|---|---|
| `stg_transactions` | All transactions with `is_returned` and `has_missing_distributor` flags |
| `stg_products` | Product dimension |
| `stg_distributors` | Distributor dimension |
| `stg_salespersons` | Salesperson dimension |
| `stg_monthly_targets` | Targets with computed `achievement_pct` and `month_start_date` |
| `stg_date_table` | Calendar dimension |

### Marts (tables)
Business-ready aggregations used for reporting.

| Model | Description |
|---|---|
| `mart_sales_performance` | Monthly performance per salesperson with MoM trend |
| `mart_distributor_product_summary` | Distributor revenue, return rates, and product breakdown |

### Gold (tables)
Each model directly answers one of the five business questions.

| Model | Business Question |
|---|---|
| `top_products_2024` | Top 5 products by revenue in 2024 |
| `region_mom_growth_q3_2024` | Region with highest MoM growth in Q3 2024 |
| `salesperson_achievement` | Average achievement % per salesperson |
| `distributor_return_rate` | Distributor with highest return rate |
| `rolling_revenue_by_category` | Rolling 3-month revenue by product category |

---

## Airflow DAG

**DAG ID:** `fmcg_sales_pipeline` | **Schedule:** Daily at 00:00 UTC

```
extract → transform → load → dbt_run → dbt_test
```

| Feature | Detail |
|---|---|
| Retries | 3 attempts with exponential backoff |
| Retry delay | 5 minutes |
| Alerting | Email notification on any task failure via `on_failure_callback` |
| Catchup | Disabled |
| Tags | `fmcg`, `sales`, `etl` |

---

## Running Locally with Docker

### Prerequisites
- Docker Desktop installed and running
- At least 4GB RAM allocated to Docker
- Git

### Steps

```bash
# 1. Clone the repo
git clone https://github.com/your-username/fmcg-sales-pipeline.git
cd fmcg-sales-pipeline

# 2. Create your env file
cp .env.example .env
# Edit .env with your credentials if needed

# 3. Place the dataset
mkdir -p data
cp /path/to/FMN_DE_Sales_Assessment_Dataset.xlsx data/

# 4. Build and start all services
docker compose up --build -d

# 5. Wait ~2 minutes for Airflow to initialise
# Then open Airflow UI: http://localhost:8080
# Login: airflow / airflow

# 6. Trigger the DAG
# Go to DAGs → fmcg_sales_pipeline → click the Play button
```

### Connecting to the FMCG Database Locally

Use any SQL client (DBeaver, TablePlus, psql):

```
Host:     localhost
Port:     5434
User:     postgres
Password: postgres
Database: fmcg_sales
```

Or as a connection string:
```
postgresql://postgres:postgres@localhost:5434/fmcg_sales
```

### Running dbt Manually

```bash
# Debug connection
docker compose exec airflow-scheduler bash -c \
  "cd /opt/airflow/dbt && dbt debug --profiles-dir . --project-dir ."

# Run all models
docker compose exec airflow-scheduler bash -c \
  "cd /opt/airflow/dbt && dbt run --profiles-dir . --project-dir ."

# Run tests
docker compose exec airflow-scheduler bash -c \
  "cd /opt/airflow/dbt && dbt test --profiles-dir . --project-dir ."
```

---

## Environment Variables

Copy `.env.example` to `.env` and fill in your values:

```dotenv
DB_USER=postgres_user
DB_PASSWORD=postgres_password
DB_HOST=postgres_host
DB_PORT=5432
DB_NAME=postgres_db
DB_URL=postgresql://postgres_user:postgres_password@postgres_host:5432/postgres_db
```

---

## What to Exclude from Git

The following are excluded via `.gitignore` and should never be committed:

```gitignore
.env
.venv/
__pycache__/
*.pyc
dbt/target/
dbt/logs/
dbt/dbt_packages/
logs/
data/
```

> **Note on the dataset:** `data/` is gitignored since the Excel file is provided separately by the assessors. Place it in the `data/` folder before running the pipeline.

---

## Design Decisions

**Why re-derive computed columns?**
Source Excel formulas for `revenue_ngn`, `discount_amount_ngn`, and `gross_profit_ngn` could contain errors. Re-computing from base fields ensures consistency regardless of source quality.

**Why retain returned transactions?**
Dropping returns would lose data needed for Q4 (return rate analysis). Rows are flagged with `_is_returned` and all revenue models filter them out explicitly — making the exclusion visible and auditable rather than silent.

**Why flag NULL distributor_id instead of dropping?**
66 rows (~2%) have no distributor. Dropping them silently would skew revenue totals. Flagging preserves the revenue while making the data quality issue visible to analysts and downstream models.

**Why full replace on dimensions?**
Dimension tables are small (15–18 rows). Upsert logic adds complexity with minimal benefit at this scale. Full replace is simpler and guarantees consistency on every run.

**Why a gold layer instead of plain SQL files?**
Gold layer models make business question answers reproducible, version-controlled, and automatically refreshed on every pipeline run. Assessors can query `gold.q1_top_products_2024` directly rather than running ad-hoc SQL.

**Why two Postgres instances?**
Airflow uses its own Postgres for metadata. Mixing pipeline data into the same instance creates operational risk. Separation keeps concerns clean and prevents pipeline issues from affecting Airflow's scheduler.