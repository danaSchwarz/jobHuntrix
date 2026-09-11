import os
import json
from dotenv import load_dotenv
import httpx

load_dotenv()
ADZUNA_APP_ID = os.getenv("ADZUNA_APP_ID")
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY")

CITIES = {"vienna": "at", "brno": "cz"}
SEARCH = "developer"
PAGES = 2

os.makedirs("data/raw", exist_ok=True)

for city, country in CITIES.items():
    all_results = []
    for page in range(1, PAGES + 1):

        url = f"https://api.adzuna.com/v1/api/jobs/{country}/search/{page}"
        params = {
            "app_id": ADZUNA_APP_ID,
            "app_key": ADZUNA_APP_KEY,
            "results_per_page": 50,
            "what": SEARCH,
            "where": city,
            "content-type": "application/json",
        }

        response = httpx.get(url, params=params, timeout=30)
        response.raise_for_status()  # Raise an error for bad responses
        data = response.json().get("results", [])
        all_results.extend(data)

    with open(f"data/raw/{city}_jobs.json", "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)

    print(f"{city}: saved {len(all_results)} job listings to data/raw/{city}_jobs.json")
