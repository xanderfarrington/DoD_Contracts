# src/collectors/usaspending_contracts.py
from __future__ import annotations

import json
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List

import requests

USASPENDING_ENDPOINT = "https://api.usaspending.gov/api/v2/search/spending_by_award/"

def fetch_usaspending_dod_contracts(days_back: int = 7, limit: int = 100) -> List[Dict[str, Any]]:
    """
    Fetch recent DoD awards from USAspending. No API key required. :contentReference[oaicite:4]{index=4}
    Uses the 'spending_by_award' advanced search style shown in the USAspending tutorial. :contentReference[oaicite:5]{index=5}
    """
    # Rolling window
    end_dt = date.today()
    start_dt = end_dt - timedelta(days=days_back)

    # NOTE: USAspending filters are powerful; this is a starter configuration.
    # You may need to adjust award_type_codes to contract-specific codes you care about.
    payload = {
        "filters": {
            "agencies": [
                {"type": "awarding", "tier": "toptier", "name": "Department of Defense"}
            ],
            "time_period": [
                {"start_date": start_dt.isoformat(), "end_date": end_dt.isoformat()}
            ],
        },
        "fields": [
            "Award ID",
            "Recipient Name",
            "Start Date",
            "End Date",
            "Award Amount",
            "Awarding Agency",
            "Awarding Sub Agency",
            "Award Type",
        ],
        "limit": limit,
        "page": 1,
        "sort": "Award Amount",
        "order": "desc",
    }

    out: List[Dict[str, Any]] = []
    while True:
        resp = requests.post(USASPENDING_ENDPOINT, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        results = data.get("results", [])
        out.extend(results)

        # Pagination varies by endpoint response; commonly includes metadata like page/hasNext.
        # If your response includes "hasNext" or "page_metadata", use that.
        page_meta = data.get("page_metadata") or {}
        has_next = page_meta.get("has_next_page")
        if not has_next:
            break

        payload["page"] += 1
        time.sleep(0.2)  # be polite

    return out

def normalize(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    norm = []
    for r in records:
        norm.append({
            "source": "usaspending",
            "award_id": r.get("Award ID"),
            "recipient": r.get("Recipient Name"),
            "start_date": r.get("Start Date"),
            "end_date": r.get("End Date"),
            "obligated_amount": r.get("Award Amount"),
            "awarding_agency": r.get("Awarding Agency"),
            "awarding_sub_agency": r.get("Awarding Sub Agency"),
            "award_type": r.get("Award Type"),
        })
    return norm

def main() -> None:
    records = fetch_usaspending_dod_contracts(days_back=7, limit=100)
    normalized = normalize(records)

    out_path = Path("data/contracts_latest.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(normalized, indent=2), encoding="utf-8")
    print(f"Wrote {len(normalized)} records to {out_path}")

if __name__ == "__main__":
    main()
