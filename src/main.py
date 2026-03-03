from __future__ import annotations

import os
from pathlib import Path

from src.collectors.usaspending import USAspendingConfig, fetch_dod_awards
from src.normalize import normalize_usaspending
from src.storage import upsert_awards, read_latest, write_csv, write_json


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
    page_limit = env_int("PAGE_LIMIT", 500)

    out_sqlite = Path(os.getenv("OUT_SQLITE", "data/contracts.sqlite"))
    out_json = Path(os.getenv("OUT_JSON", "data/contracts_latest.json"))
    out_csv = Path(os.getenv("OUT_CSV", "data/contracts_latest.csv"))

    cfg = USAspendingConfig(days_back=days_back, page_limit=page_limit)

    raw = fetch_dod_awards(cfg)
    normalized = normalize_usaspending(raw)

    upsert_awards(out_sqlite, normalized)
    latest = read_latest(out_sqlite, limit=2000)

    write_json(out_json, latest)
    write_csv(out_csv, latest)

    print(f"Fetched raw: {len(raw)}")
    print(f"Normalized: {len(normalized)}")
    print(f"Wrote: {out_sqlite} / {out_json} / {out_csv}")


if __name__ == "__main__":
    main()
