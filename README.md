# BookETL — Web Scraping & Analytics Pipeline

<div align="center">

![Python](https://img.shields.io/badge/Python-3.10-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-Cloud_Warehouse-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-Transforms-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![BeautifulSoup](https://img.shields.io/badge/BeautifulSoup4-Scraping-4B8BBE?style=for-the-badge)
![Pandas](https://img.shields.io/badge/Pandas-Cleaning-150458?style=for-the-badge&logo=pandas&logoColor=white)
![pytest](https://img.shields.io/badge/pytest-Tested-0A9EDC?style=for-the-badge&logo=pytest&logoColor=white)

**An end-to-end data engineering pipeline that scrapes 1000 products across 50 categories from the web, cleans and validates the data with Pandas, loads it into a Snowflake cloud warehouse, and transforms it into a business-ready analytics layer using dbt.**

</div>

---

## 📌 What This Project Does

Most businesses have product or pricing data scattered across websites with no structured way to analyse it. This pipeline solves that problem end to end:

1. **Scrapes** 1000 books across 50 categories from a live e-commerce website
2. **Cleans** the raw data — deduplication, type casting, validation, derived metrics
3. **Loads** clean records into Snowflake using bulk INSERT
4. **Transforms** raw data into an analytics summary table using dbt
5. **Tests** every layer — schema tests in dbt, unit tests in pytest

---

## 🏛️ Architecture

```
books.toscrape.com  (live website)
        │
        │  requests + BeautifulSoup
        │  scrapes 50 categories × 20 books per page
        ▼
scraper/books_scraper.py
        │
        │  pandas cleaning + validation
        │  dedup, type casting, price bands, ratings
        ▼
transform/cleaner.py
        │
        │  snowflake-connector bulk INSERT
        │  1000 rows → BOOKETL_DB.RAW.RAW_BOOKS
        ▼
Snowflake RAW Schema
        │
        │  dbt run
        │  stg_books (view) → mart_books_summary (table)
        ▼
Snowflake ANALYTICS Schema
  MART_BOOKS_SUMMARY
  ┌─────────────────┬─────────────┬───────────┬───────────┬────────────┐
  │ category        │ total_books │ avg_price │ avg_rating│ in_stock   │
  ├─────────────────┼─────────────┼───────────┼───────────┼────────────┤
  │ Fiction         │ 65          │ 36.07     │ 3.18      │ 65         │
  │ Mystery         │ 32          │ 31.72     │ 2.94      │ 32         │
  │ Young Adult     │ 54          │ 35.45     │ 3.30      │ 54         │
  │ ...             │ ...         │ ...       │ ...       │ ...        │
  └─────────────────┴─────────────┴───────────┴───────────┴────────────┘
```

---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Scraping** | Python, requests, BeautifulSoup4 | HTTP requests, HTML parsing, pagination |
| **Cleaning** | Pandas | Deduplication, validation, derived columns |
| **Warehouse** | Snowflake | Cloud data warehouse, RAW + ANALYTICS schemas |
| **Transform** | dbt Core + dbt-snowflake | SQL models, schema tests, documentation |
| **Testing** | pytest, pytest-mock | Unit tests with mocked HTTP calls |
| **Config** | pydantic-settings, python-dotenv | Type-safe environment variable management |

---

## 📁 Project Structure

```
booketl/
│
├── pipeline.py                  ← Entry point — runs scrape → clean → load
├── config.py                    ← All settings loaded from .env
├── requirements.txt
├── .env.example
│
├── scraper/
│   └── books_scraper.py         ← Scrapes all 50 categories with pagination
│
├── transform/
│   └── cleaner.py               ← Pandas pipeline: dedupe, validate, enrich
│
├── loader/
│   └── snowflake_loader.py      ← Bulk loads DataFrame into Snowflake RAW
│
├── scripts/
│   └── init_snowflake.py        ← One-time table creation
│
├── dbt_booketl/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   └── models/
│       ├── sources.yml
│       ├── staging/
│       │   ├── stg_books.sql    ← Cleans raw table (view)
│       │   └── schema.yml       ← Column-level dbt tests
│       └── marts/
│           ├── mart_books_summary.sql  ← Category analytics (table)
│           └── schema.yml
│
└── tests/
    ├── conftest.py
    ├── test_scraper.py          ← 3 unit tests, HTTP mocked
    └── test_cleaner.py          ← 6 unit tests, no network needed
```

---

## 🚀 Quick Start

### Prerequisites
- Python 3.10
- A free [Snowflake account](https://signup.snowflake.com) (no credit card needed)

### 1 — Clone and install

```bash
git clone https://github.com/yourusername/booketl.git
cd booketl

python -m venv .venv
.venv\Scripts\activate        # Windows
pip install -r requirements.txt
```

### 2 — Configure

```bash
copy .env.example .env
# Open .env and fill in your Snowflake credentials
```

### 3 — Set up Snowflake

Run this SQL in a Snowflake worksheet:

```sql
CREATE DATABASE IF NOT EXISTS BOOKETL_DB;
CREATE SCHEMA  IF NOT EXISTS BOOKETL_DB.RAW;
CREATE SCHEMA  IF NOT EXISTS BOOKETL_DB.ANALYTICS;
CREATE WAREHOUSE IF NOT EXISTS BOOKETL_WH
  WITH WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;
```

### 4 — Run the pipeline

```bash
python scripts\init_snowflake.py   # create table (once)
python pipeline.py                 # scrape → clean → load
```

### 5 — Run dbt

```bash
cd dbt_booketl
dbt deps --profiles-dir .
dbt run  --profiles-dir .
dbt test --profiles-dir .
```

### 6 — Query the results

```sql
SELECT category, total_books, avg_price, avg_rating, highly_rated_count
FROM BOOKETL_DB.ANALYTICS.MART_BOOKS_SUMMARY
ORDER BY total_books DESC;
```

---

## 📊 Sample Output

After a full pipeline run, `MART_BOOKS_SUMMARY` contains one row per category:

| Category | Total Books | Avg Price | Avg Rating | Highly Rated |
|---|---|---|---|---|
| Nonfiction | 110 | £34.26 | 2.88 | 38 |
| Fiction | 65 | £36.07 | 3.18 | 27 |
| Young Adult | 54 | £35.45 | 3.30 | 30 |
| Fantasy | 48 | £39.59 | 3.08 | 21 |
| Mystery | 32 | £31.72 | 2.94 | 12 |

---

## ✅ Tests

```bash
pytest tests/ -v
```

```
PASSED tests/test_scraper.py::TestBooksScraper::test_scrape_page_returns_books
PASSED tests/test_scraper.py::TestBooksScraper::test_http_error_returns_empty
PASSED tests/test_scraper.py::TestBooksScraper::test_empty_page_returns_empty
PASSED tests/test_cleaner.py::TestCleaner::test_returns_dataframe
PASSED tests/test_cleaner.py::TestCleaner::test_deduplicates
PASSED tests/test_cleaner.py::TestCleaner::test_price_band_exists
PASSED tests/test_cleaner.py::TestCleaner::test_is_highly_rated
PASSED tests/test_cleaner.py::TestCleaner::test_scraped_at_exists
PASSED tests/test_cleaner.py::TestCleaner::test_removes_zero_price
9 passed
```

---

## 🔄 Data Quality

**dbt schema tests** run after every transform:
- `not_null` on title, price, rating, category
- `accepted_values` on rating (1–5 only)
- `unique` on category in the mart

**Python unit tests** cover:
- Correct price parsing with encoding edge cases
- HTTP error handling (returns empty, does not crash)
- Deduplication logic
- Invalid price removal (zero, negative)
- Derived column correctness (price bands, rating flags)

---

## 📄 License

MIT — free to use, adapt, and deploy.

---

<div align="center">
Built to demonstrate end-to-end data engineering: scraping · cleaning · warehousing · transformation · testing
</div>
