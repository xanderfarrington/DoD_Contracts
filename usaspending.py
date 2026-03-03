# src/collectors/usaspending.py
from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

USASPENDING_ENDPOINT = "https://api.usaspending.gov/api/v2/search/spending_by_award/"


@dataclass(frozen=True)
class USAspendingConfig:
    days_back: int = 7
    page_limit: int = 500
    sleep_s: float = 0.2


def _session_with_retries() -> requests.Session:
    """
    A requests session with conservative retries for network hiccups / 5xx.
    """
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["POST"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch_dod_awards(cfg: USAspendingConfig) -> List[Dict[str, Any]]:
    """
    Fetch awards where awarding agency is Department of Defense for a rolling window.
    Returns raw USAspending results items.
    """
    end_dt = date.today()
    start_dt = end_dt - timedelta(days=cfg.days_back)

    payload: Dict[str, Any] = {
        "filters": {
            "agencies": [
                {"type": "awarding", "tier": "toptier", "name": "Department of Defense"}
            ],
            "time_period": [{"start_date": start_dt.isoformat(), "end_date": end_dt.isoformat()}],
        },
        # These are display fields; USAspending may change labels over time.
        # We'll normalize defensively downstream.
        "fields": [
            "Award ID",
            "Recipient Name",
            "Start Date",
            "End Date",
            "Award Amount",
            "Awarding Agency",
            "Awarding Sub Agency",
            "Award Type",
            "Place of Performance Zip5",
            "Awarding Agency Code",
            "Awarding Sub Agency Code",
            "NAICS",
            "PSC",
            "Recipient UEI",
            "Recipient DUNS",
            "Description",
        ],
        "limit": int(cfg.page_limit),
        "page": 1,
        "sort": "Award Amount",
        "order": "desc",
    }

    session = _session_with_retries()
    all_results: List[Dict[str, Any]] = []

    while True:
        resp = session.post(USASPENDING_ENDPOINT, json=payload, timeout=60)

        # If the server returns something unexpected, surface it clearly.
        if resp.status_code >= 400:
            raise RuntimeError(
                f"USAspending error {resp.status_code}: {resp.text[:500]}"
            )

        data = resp.json()
        results = data.get("results") or []
        all_results.extend(results)

        page_meta: Optional[Dict[str, Any]] = data.get("page_metadata") or {}
        has_next = bool(page_meta.get("has_next_page"))

        if not has_next:
            break

        payload["page"] += 1
        time.sleep(cfg.sleep_s)

    return all_results
