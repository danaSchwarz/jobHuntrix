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
   dump raw results per city to `data/raw/<city>_jobs.json`.
2. **Transform** — [`src/transform.py`](src/transform.py) loads all raw JSON
   into a single Spark DataFrame, deduplicates listings, and parses the messy
   free-text `salary` field into a currency (`CZK`/`EUR`/`USD`) and a numeric
   value (handling things like `"40k"` vs `"40000"`). It then aggregates job
   count and average salary per city/currency.
3. **Load** — the cleaned rows and the summary are written to
   `data/clean/jobs.parquet` and `data/clean/jobs_summary.parquet`.
4. **Analyze** — [`src/read_parquet.py`](src/read_parquet.py) reads the
   Parquet files back with pandas for quick exploration.

## Sample output

`jobs_summary.parquet` (job count and average salary per city/currency):

| city   | currency | job_count | avg_salary |
|--------|----------|-----------|------------|
| Brno   | CZK      | 21        | 75,102     |
| Vienna | USD      | 21        | 162,738    |
| Remote | USD      | 5         | 59,095     |

(Rows with `currency = null` are listings that didn't include a parsed salary.)

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
[Jooble](https://jooble.org/api/about) to run the extract step.

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
- **Two independent extractors**: Adzuna and Jooble have different auth and
  response shapes, so they're kept as separate scripts that both converge on
  the same `data/raw/<city>_jobs.json` contract — the transform step doesn't
  care which API a listing came from.
