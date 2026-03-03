# src/storage.py
from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SCHEMA = """
CREATE TABLE IF NOT EXISTS awards (
    source TEXT NOT NULL,
    award_id TEXT NOT NULL,

    recipient_name TEXT,
    recipient_uei TEXT,
    recipient_duns TEXT,

    start_date TEXT,
    end_date TEXT,

    award_amount REAL,
    award_type TEXT,

    awarding_agency TEXT,
    awarding_sub_agency TEXT,
    awarding_agency_code TEXT,
    awarding_sub_agency_code TEXT,

    naics TEXT,
    psc TEXT,

    place_of_performance_zip5 TEXT,
    description TEXT,

    ingested_at TEXT DEFAULT (datetime('now')),

    PRIMARY KEY (source, award_id)
);
"""


COLUMNS: Tuple[str, ...] = (
    "source",
    "award_id",
    "recipient_name",
    "recipient_uei",
    "recipient_duns",
    "start_date",
    "end_date",
    "award_amount",
    "award_type",
    "awarding_agency",
    "awarding_sub_agency",
    "awarding_agency_code",
    "awarding_sub_agency_code",
    "naics",
    "psc",
    "place_of_performance_zip5",
    "description",
)


def ensure_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.executescript(SCHEMA)


def upsert_awards(db_path: Path, awards: Iterable[Dict[str, Any]]) -> int:
    """
    Insert or update awards keyed by (source, award_id).
    """
    ensure_db(db_path)

    rows: List[Tuple[Any, ...]] = []
    for a in awards:
        rows.append(tuple(a.get(col) for col in COLUMNS))

    if not rows:
        return 0

    placeholders = ",".join(["?"] * len(COLUMNS))
    assignments = ",".join([f"{c}=excluded.{c}" for c in COLUMNS if c not in ("source", "award_id")])

    sql = f"""
    INSERT INTO awards ({",".join(COLUMNS)})
    VALUES ({placeholders})
    ON CONFLICT(source, award_id) DO UPDATE SET
      {assignments},
      ingested_at=datetime('now');
    """

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        cur = conn.cursor()
        cur.executemany(sql, rows)
        conn.commit()
        return cur.rowcount


def read_latest(db_path: Path, limit: int = 2000) -> List[Dict[str, Any]]:
    """
    Return a "latest view" ordered by ingested time, then award amount desc.
    """
    ensure_db(db_path)
    sql = f"""
    SELECT {",".join(COLUMNS)}
    FROM awards
    ORDER BY ingested_at DESC, award_amount DESC
    LIMIT ?;
    """
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql, (int(limit),)).fetchall()
        return [dict(r) for r in rows]


def write_json(path: Path, records: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(records, indent=2), encoding="utf-8")


def write_csv(path: Path, records: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(COLUMNS))
        w.writeheader()
        for r in records:
            w.writerow({k: r.get(k) for k in COLUMNS})
