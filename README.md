# Dev Job Market ETL

A small ETL pipeline that pulls developer job postings from two job-board APIs
(Adzuna, Jooble) for Vienna, Brno and Remote, cleans and normalizes the salary
data with PySpark, and writes the results to Parquet for analysis with pandas.

## Pipeline

```
Adzuna API ─┐
             ├─> data/raw/*.json ─> Spark transform ─> data/clean/*.parquet ─> pandas analysis
Jooble API ─┘
```

1. **Extract** — [`src/extract_adzuna.py`](src/extract_adzuna.py) and
   [`src/extract_jooble.py`](src/extract_jooble.py) each call their API and
   save raw results per city to `data/raw/<source>_<city>_jobs.json` (e.g.
   `adzuna_Vienna_jobs.json`, `jooble_Vienna_jobs.json`) — source-prefixed so
   the two APIs' files never overwrite each other. Adzuna's response is
   normalized into the same `{id, title, company, salary}` shape Jooble
   already returns, so downstream code doesn't need source-specific logic.
   Note: Adzuna doesn't cover Czech Republic, so Brno data comes from Jooble
   only; Vienna gets contributions from both.
2. **Transform** — [`src/transform.py`](src/transform.py) loads every raw
   file from both sources into a single Spark DataFrame (tagging each row
   with `city` and `api_source`), deduplicates per source+id, and parses the
   messy free-text `salary` field into a currency (`CZK`/`EUR`/`USD`) and a
   numeric value (handling things like `"40k"` vs `"40000"`). It then
   aggregates job count and average salary per city/currency.
3. **Load** — the cleaned rows and the summary are written to
   `data/clean/jobs.parquet` and `data/clean/jobs_summary.parquet`.
4. **Analyze** — [`src/read_parquet.py`](src/read_parquet.py) reads the
   Parquet files back with pandas for quick exploration.

## Sample output

`jobs_summary.parquet` (job count and average salary per city/currency):

| city   | currency | job_count | avg_salary |
|--------|----------|-----------|------------|
| Brno   | CZK      | 20        | 81,857     |
| Vienna | EUR      | 27        | 65,727     |
| Vienna | USD      | 20        | 164,465    |
| Remote | USD      | 3         | 68,491     |

(Rows with `currency = null` are listings that didn't include a parsed
salary. Vienna shows both EUR — from Adzuna — and USD — from Jooble —
confirming both sources are actually combined, not just concatenated files.)

## Tech stack

- **PySpark** — cleaning and aggregating semi-structured JSON at scale
- **httpx** — API requests to Adzuna and Jooble
- **pandas + PyArrow** — final Parquet I/O and analysis
- **python-dotenv** — local API credential management

## Setup

```bash
python -m venv venv
venv\Scripts\activate      # on Windows
pip install -r requirements.txt
cp .env.example .env       # then fill in your API keys
```

You'll need free API keys from [Adzuna](https://developer.adzuna.com/) and
[Jooble](https://jooble.org/api/about) to run the extract step. (For Jooble you need an API key per region.)

## Usage

```bash
python src/extract_adzuna.py
python src/extract_jooble.py
python src/transform.py
python src/read_parquet.py
```

`scripts/check_spark_setup.py` is a standalone smoke test to confirm your
local Java/PySpark install works before running the pipeline.

## Notes & engineering decisions

- **ANSI mode off**: Spark 4's default ANSI mode raises on invalid numeric
  casts, which real-world messy salary strings trigger constantly. It's
  disabled so bad values become `null` instead of crashing the job.
- **Parquet via pandas, not Spark's native writer**: Spark's native Parquet
  writer requires Hadoop's `winutils.exe` on Windows, which isn't installed
  in this environment. `df.toPandas().to_parquet(...)` sidesteps that while
  keeping Parquet as the storage format.
- **Two independent extractors, combined without overwriting**: Adzuna and
  Jooble have different auth and response shapes, so they're kept as
  separate scripts, but each writes to a source-prefixed filename
  (`adzuna_<city>_jobs.json` / `jooble_<city>_jobs.json`) and Adzuna's output
  is normalized to match Jooble's shape at extraction time. `transform.py`
  reads every `<source>_<city>_jobs.json` file it finds, tags each row with
  `api_source`, and dedupes per source+id (ids aren't comparable across
  APIs) — so both sources genuinely combine into one dataset instead of one
  silently overwriting the other on a case-insensitive filesystem.
- **Adzuna doesn't cover every city**: its supported countries don't include
  Czech Republic, so `extract_adzuna.py` only fetches Vienna. `transform.py`
  skips any `<source>_<city>_jobs.json` combination that doesn't exist
  rather than assuming every source covers every city.
