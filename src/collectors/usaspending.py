from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict, List

import requests

USASPENDING_ENDPOINT = "https://api.usaspending.gov/api/v2/search/spending_by_award/"


@dataclass(frozen=True)
class USAspendingConfig:
    days_back: int = 7
    page_limit: int = 500
    sleep_s: float = 0.2


def fetch_dod_awards(cfg: USAspendingConfig) -> List[Dict[str, Any]]:
    end_dt = date.today()
    start_dt = end_dt - timedelta(days=cfg.days_back)

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
        "limit": cfg.page_limit,
        "page": 1,
        "sort": "Award Amount",
        "order": "desc",
    }

    all_results: List[Dict[str, Any]] = []

    while True:
        resp = requests.post(USASPENDING_ENDPOINT, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        results = data.get("results", [])
        all_results.extend(results)

        page_meta = data.get("page_metadata") or {}
        if not page_meta.get("has_next_page"):
            break

        payload["page"] += 1
        time.sleep(cfg.sleep_s)

    return all_results
