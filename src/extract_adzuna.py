import os
import json
from dotenv import load_dotenv
import httpx

load_dotenv()
ADZUNA_APP_ID = os.getenv("ADZUNA_APP_ID")
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY")

# Capitalized to match extract_jooble.py's city naming, so files from both
# sources tag the same city consistently for transform.py.
# Adzuna doesn't cover Czech Republic at all (not in its supported country
# list), so Brno can only come from Jooble - Adzuna only contributes Vienna.
CITIES = {"Vienna": "at"}
SEARCH = "developer"
PAGES = 2

# Adzuna returns salary_min/salary_max as plain numbers with no currency
# symbol. We format them into the same "symbol + number" text Jooble's
# salary field already uses, so transform.py's existing parser handles
# both sources without any source-specific logic.
CURRENCY_SYMBOL = {"at": "€"}


def format_salary(job, symbol):
    salary_min = job.get("salary_min")
    salary_max = job.get("salary_max")
    if not salary_min or not salary_max:
        return ""
    return f"{symbol}{salary_min:.0f} - {symbol}{salary_max:.0f}"


os.makedirs("data/raw", exist_ok=True)

for city, country in CITIES.items():
    symbol = CURRENCY_SYMBOL[country]
    all_results = []
    for page in range(1, PAGES + 1):

        url = f"https://api.adzuna.com/v1/api/jobs/{country}/search/{page}"
        params = {
            "app_id": ADZUNA_APP_ID,
            "app_key": ADZUNA_APP_KEY,
            "results_per_page": 50,
            "what": SEARCH,
            "where": city.lower(),  # Adzuna's "where" param needs lowercase, unlike our city labels
            "content-type": "application/json",
        }

        response = httpx.get(url, params=params, timeout=30)
        response.raise_for_status()  # Raise an error for bad responses
        data = response.json().get("results", [])
        all_results.extend(data)

    # Normalize to the same {id, title, company, salary} shape Jooble's
    # raw output already has.
    normalized = [
        {
            "id": job.get("id"),
            "title": job.get("title"),
            "company": job.get("company", {}).get("display_name"),
            "salary": format_salary(job, symbol),
        }
        for job in all_results
    ]

    out_path = f"data/raw/adzuna_{city}_jobs.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(normalized, f, ensure_ascii=False, indent=2)

    print(f"{city}: saved {len(normalized)} job listings to {out_path}")
