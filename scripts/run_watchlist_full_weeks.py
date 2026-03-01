#!/usr/bin/env python3
"""Run weekly profile analysis for all active watchlist tickers."""

from __future__ import annotations

import argparse
import importlib.util
import json
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def load_profile_module(module_path: Path):
    spec = importlib.util.spec_from_file_location("profile_mod", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module from {module_path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def init_batch_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS watchlist_batch_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT NOT NULL,
            ticker TEXT NOT NULL,
            status TEXT NOT NULL,
            message TEXT,
            bars INTEGER,
            weeks_processed INTEGER,
            window_start TEXT,
            window_end TEXT,
            result_json TEXT NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT NOT NULL
        )
        """
    )
    conn.commit()


def active_tickers(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        """
        SELECT ticker
        FROM watchlist_stocks
        WHERE is_active = 1
        ORDER BY priority, category, ticker
        """
    ).fetchall()
    return [r[0] for r in rows]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run weekly analysis for all active watchlist tickers.")
    parser.add_argument("--cache-db", default="data/cache/yahoo_cache.sqlite")
    parser.add_argument("--full-weeks", type=int, default=3)
    parser.add_argument("--interval", default="30m")
    parser.add_argument("--tick-size", type=float, default=0.25)
    parser.add_argument("--ib-minutes", type=int, default=60)
    parser.add_argument("--sleep-seconds", type=float, default=1.0, help="sleep between chunk requests per symbol")
    parser.add_argument("--between-tickers", type=float, default=0.4, help="sleep between symbols")
    args = parser.parse_args()

    db_path = Path(args.cache_db)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    init_batch_table(conn)

    tickers = active_tickers(conn)
    if not tickers:
        print("No active tickers in watchlist_stocks.")
        return

    module_path = Path(__file__).with_name("fetch_es_yahoo_and_profile.py")
    mod = load_profile_module(module_path)

    batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    total = len(tickers)
    ok = 0
    no_data = 0
    failed = 0

    for i, ticker in enumerate(tickers, start=1):
        started = datetime.now(timezone.utc).isoformat()
        try:
            result: dict[str, Any] = mod.run(
                symbol=ticker,
                lookback_days=7,
                interval=args.interval,
                tick_size=args.tick_size,
                ib_minutes=args.ib_minutes,
                cache_db=db_path,
                sleep_seconds=max(0.0, args.sleep_seconds),
                full_weeks=max(1, args.full_weeks),
            )
            status = str(result.get("status", "unknown"))
            msg = result.get("message")
            bars = result.get("bars")
            weeks_processed = result.get("weeks_processed")
            window_start = result.get("window_start")
            window_end = result.get("window_end")
            if status == "ok":
                ok += 1
            elif status == "no_data":
                no_data += 1
            else:
                failed += 1
        except Exception as exc:  # pylint: disable=broad-except
            status = "error"
            msg = str(exc)
            bars = None
            weeks_processed = None
            window_start = None
            window_end = None
            result = {"status": "error", "message": str(exc), "symbol": ticker}
            failed += 1

        finished = datetime.now(timezone.utc).isoformat()
        conn.execute(
            """
            INSERT INTO watchlist_batch_runs (
                batch_id, ticker, status, message, bars, weeks_processed,
                window_start, window_end, result_json, started_at, finished_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                batch_id,
                ticker,
                status,
                msg,
                bars,
                weeks_processed,
                window_start,
                window_end,
                json.dumps(result, ensure_ascii=False),
                started,
                finished,
            ),
        )
        conn.commit()
        print(f"[{i}/{total}] {ticker}: {status}")
        if args.between_tickers > 0:
            time.sleep(args.between_tickers)

    print(
        json.dumps(
            {
                "batch_id": batch_id,
                "total": total,
                "ok": ok,
                "no_data": no_data,
                "failed": failed,
            },
            ensure_ascii=False,
        )
    )
    conn.close()


if __name__ == "__main__":
    main()
