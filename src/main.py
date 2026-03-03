# src/main.py
from __future__ import annotations

import os
from pathlib import Path

from src.collectors.usaspending_contracts import fetch_usaspending_dod_contracts, normalize


def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if not v:
        return default
    try:
        return int(v)
    except ValueError:
        raise ValueError(f"Env var {name} must be an int; got {v!r}")


def main() -> None:
    days_back = env_int("DAYS_BACK", 7)
    limit = env_int("LIMIT", 100)

    out_json = Path(os.getenv("OUT_JSON", "data/contracts_latest.json"))

    records = fetch_usaspending_dod_contracts(days_back=days_back, limit=limit)
    normalized = normalize(records)

    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(__import__("json").dumps(normalized, indent=2), encoding="utf-8")
    print(f"Wrote {len(normalized)} records to {out_json}")


if __name__ == "__main__":
    main()
