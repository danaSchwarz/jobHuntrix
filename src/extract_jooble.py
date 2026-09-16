import os
import json
from dotenv import load_dotenv
import httpx

load_dotenv()
JOOBLE_API_KEY_AT = os.getenv("JOOBLE_API_KEY_AT")
JOOBLE_API_KEY_CZ = os.getenv("JOOBLE_API_KEY_CZ")

SOURCES = {
    "Brno":   ("https://cz.jooble.org/api/", JOOBLE_API_KEY_CZ),
    "Vienna": ("https://jooble.org/api/",    JOOBLE_API_KEY_AT),
    "Remote": ("https://jooble.org/api/",    JOOBLE_API_KEY_AT),
}

HEADERS = {"Content-Type": "application/json"}

os.makedirs("data/raw", exist_ok=True)

for city, (host, api_key) in SOURCES.items():
    url = f"{host}{api_key}"
    payload = {
        "keywords": "developer",
        "location": city,
        "page": 1,
    }
    response = httpx.post(url, json=payload, headers=HEADERS, timeout=30)
    response.raise_for_status()          # fail loudly on 4xx/5xx
    jobs = response.json().get("jobs", [])   # fresh list each city

    out_path = f"data/raw/jooble_{city}_jobs.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(jobs, f, ensure_ascii=False, indent=2)
    print(f"{city}: saved {len(jobs)} jobs to {out_path}")
