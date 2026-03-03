# src/normalize.py
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional


def _get(d: Dict[str, Any], *keys: str) -> Any:
    """
    Try a sequence of possible keys (USAspending field labels can vary).
    """
    for k in keys:
        if k in d and d[k] not in ("", None):
            return d[k]
    return None


def _to_iso_date(x: Any) -> Optional[str]:
    """
    Convert common USAspending date representations into ISO YYYY-MM-DD if possible.
    """
    if x in (None, ""):
        return None
    if isinstance(x, str):
        # Often already YYYY-MM-DD
        try:
            # Handle both date and datetime strings
            if "T" in x:
                return datetime.fromisoformat(x.replace("Z", "+00:00")).date().isoformat()
            return datetime.fromisoformat(x).date().isoformat()
        except Exception:
            # Leave as-is if it's some other format
            return x
    return str(x)


def normalize_usaspending(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Map raw USAspending items into a stable schema for storage/export.
    """
    out: List[Dict[str, Any]] = []

    for r in records:
        award_id = _get(r, "Award ID", "award_id", "AwardId")
        if not award_id:
            # Skip items with no stable ID
            continue

        out.append(
            {
                "source": "usaspending",
                "award_id": str(award_id),

                "recipient_name": _get(r, "Recipient Name", "recipient_name"),
                "recipient_uei": _get(r, "Recipient UEI", "recipient_uei"),
                "recipient_duns": _get(r, "Recipient DUNS", "recipient_duns"),

                "start_date": _to_iso_date(_get(r, "Start Date", "start_date")),
                "end_date": _to_iso_date(_get(r, "End Date", "end_date")),

                "award_amount": _get(r, "Award Amount", "award_amount"),
                "award_type": _get(r, "Award Type", "award_type"),

                "awarding_agency": _get(r, "Awarding Agency", "awarding_agency"),
                "awarding_sub_agency": _get(r, "Awarding Sub Agency", "awarding_sub_agency"),
                "awarding_agency_code": _get(r, "Awarding Agency Code", "awarding_agency_code"),
                "awarding_sub_agency_code": _get(r, "Awarding Sub Agency Code", "awarding_sub_agency_code"),

                "naics": _get(r, "NAICS", "naics"),
                "psc": _get(r, "PSC", "psc"),

                "place_of_performance_zip5": _get(r, "Place of Performance Zip5", "place_of_performance_zip5"),

                "description": _get(r, "Description", "description"),
            }
        )

    return out
