#!/usr/bin/env python3
"""Fetch Yahoo Finance data via yfinance and store inferred profile metrics in SQLite.

No CSV/JSON files are produced.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import time
import warnings
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf


@dataclass
class IntradayBar:
    ts_utc: str
    open: float
    high: float
    low: float
    close: float
    volume: float


def init_cache(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS intraday_bars (
            symbol TEXT NOT NULL,
            interval TEXT NOT NULL,
            ts_utc TEXT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            fetched_at TEXT NOT NULL,
            PRIMARY KEY (symbol, interval, ts_utc)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fetched_chunks_v2 (
            symbol TEXT NOT NULL,
            interval TEXT NOT NULL,
            chunk_start TEXT NOT NULL,
            chunk_end TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            PRIMARY KEY (symbol, interval, chunk_start, chunk_end)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS profile_analysis (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            lookback_days INTEGER NOT NULL,
            interval TEXT NOT NULL,
            tick_size REAL NOT NULL,
            period_start TEXT NOT NULL,
            period_end TEXT NOT NULL,
            bars INTEGER NOT NULL,
            profile_high REAL NOT NULL,
            profile_low REAL NOT NULL,
            profile_range REAL NOT NULL,
            tpo_poc REAL NOT NULL,
            tpo_vah REAL NOT NULL,
            tpo_val REAL NOT NULL,
            total_tpo_count INTEGER NOT NULL,
            tpo_above INTEGER NOT NULL,
            tpo_below INTEGER NOT NULL,
            volume_poc REAL NOT NULL,
            volume_vah REAL NOT NULL,
            volume_val REAL NOT NULL,
            volume_above REAL NOT NULL,
            volume_below REAL NOT NULL,
            total_volume REAL NOT NULL,
            ib_high REAL NOT NULL,
            ib_low REAL NOT NULL,
            ib_width REAL NOT NULL,
            rotation_factor INTEGER NOT NULL,
            avg_subperiod_range REAL NOT NULL,
            notes_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS weekly_profile_analysis (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            interval TEXT NOT NULL,
            tick_size REAL NOT NULL,
            week_start TEXT NOT NULL,
            week_end TEXT NOT NULL,
            period_start TEXT NOT NULL,
            period_end TEXT NOT NULL,
            bars INTEGER NOT NULL,
            profile_high REAL NOT NULL,
            profile_low REAL NOT NULL,
            profile_range REAL NOT NULL,
            tpo_poc REAL NOT NULL,
            tpo_vah REAL NOT NULL,
            tpo_val REAL NOT NULL,
            total_tpo_count INTEGER NOT NULL,
            tpo_above INTEGER NOT NULL,
            tpo_below INTEGER NOT NULL,
            volume_poc REAL NOT NULL,
            volume_vah REAL NOT NULL,
            volume_val REAL NOT NULL,
            volume_above REAL NOT NULL,
            volume_below REAL NOT NULL,
            total_volume REAL NOT NULL,
            ib_high REAL NOT NULL,
            ib_low REAL NOT NULL,
            ib_width REAL NOT NULL,
            rotation_factor INTEGER NOT NULL,
            avg_subperiod_range REAL NOT NULL,
            notes_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(symbol, interval, week_start, week_end)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS weekly_profile_comparison (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            interval TEXT NOT NULL,
            week_start TEXT NOT NULL,
            week_end TEXT NOT NULL,
            prev_week_start TEXT,
            prev_week_end TEXT,
            poc_change REAL NOT NULL,
            vah_change REAL NOT NULL,
            val_change REAL NOT NULL,
            volume_poc_change REAL NOT NULL,
            ib_width_change REAL NOT NULL,
            rotation_factor_change INTEGER NOT NULL,
            total_volume_change REAL NOT NULL,
            bars_change INTEGER NOT NULL,
            direction_bias TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(symbol, interval, week_start, week_end)
        )
        """
    )
    conn.commit()
    return conn


def has_weekday(start_day: date, end_day: date) -> bool:
    cursor = start_day
    while cursor <= end_day:
        if cursor.weekday() < 5:
            return True
        cursor += timedelta(days=1)
    return False


def split_into_7day_chunks(start_day: date, end_day: date) -> list[tuple[date, date]]:
    chunks: list[tuple[date, date]] = []
    cursor = start_day
    while cursor <= end_day:
        chunk_end = min(cursor + timedelta(days=6), end_day)
        if has_weekday(cursor, chunk_end):
            chunks.append((cursor, chunk_end))
        cursor = chunk_end + timedelta(days=1)
    return chunks


def fetch_chunk_yfinance(symbol: str, interval: str, chunk_start: date, chunk_end: date) -> tuple[list[IntradayBar], str | None, bool]:
    start_s = chunk_start.isoformat()
    end_s = (chunk_end + timedelta(days=1)).isoformat()  # yfinance end is exclusive
    try:
        df = yf.download(
            tickers=symbol,
            start=start_s,
            end=end_s,
            interval=interval,
            progress=False,
            auto_adjust=False,
            threads=False,
        )
    except Exception as exc:
        return [], f"Failed chunk fetch from yfinance: {exc}", False

    if df is None or df.empty:
        return [], "No data returned from yfinance for this chunk.", True

    if hasattr(df.columns, "nlevels") and df.columns.nlevels > 1:
        df.columns = [col[0] for col in df.columns]

    out: list[IntradayBar] = []
    for idx, row in df.iterrows():
        dt = idx.to_pydatetime() if hasattr(idx, "to_pydatetime") else idx
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        if dt.weekday() >= 5:
            continue

        o = row.get("Open")
        h = row.get("High")
        l = row.get("Low")
        c = row.get("Close")
        v = row.get("Volume")
        if any(x is None for x in (o, h, l, c, v)):
            continue

        out.append(
            IntradayBar(
                ts_utc=dt.isoformat(),
                open=float(o),
                high=float(h),
                low=float(l),
                close=float(c),
                volume=float(v),
            )
        )

    if not out:
        return [], "No valid OHLCV rows in yfinance chunk.", True
    return out, None, True


def compute_complete_week_window(as_of: date, full_weeks: int) -> tuple[date, date]:
    if full_weeks <= 0:
        raise ValueError("full_weeks must be > 0")
    # Anchor to the latest completed Friday (or current Friday if today is Friday).
    last_friday = as_of - timedelta(days=(as_of.weekday() - 4) % 7)
    start_monday = last_friday - timedelta(days=7 * (full_weeks - 1) + 4)
    return start_monday, last_friday


def fetch_intraday_bars(
    symbol: str,
    interval: str,
    db_path: Path,
    sleep_seconds: float,
    start_date: date,
    end_date: date,
) -> tuple[list[IntradayBar], list[str]]:
    warnings.filterwarnings("ignore", category=UserWarning)
    conn = init_cache(db_path)
    start_dt = datetime(start_date.year, start_date.month, start_date.day, tzinfo=timezone.utc)
    end_dt = datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59, tzinfo=timezone.utc)
    chunks = split_into_7day_chunks(start_date, end_date)
    notices: list[str] = []
    fetched_at = datetime.now(timezone.utc).isoformat()

    for chunk_start, chunk_end in chunks:
        row = conn.execute(
            """
            SELECT 1 FROM fetched_chunks_v2
            WHERE symbol=? AND interval=? AND chunk_start=? AND chunk_end=?
            LIMIT 1
            """,
            (symbol, interval, chunk_start.isoformat(), chunk_end.isoformat()),
        ).fetchone()
        if row:
            continue

        bars, chunk_msg, completed = fetch_chunk_yfinance(symbol, interval, chunk_start, chunk_end)

        if chunk_msg:
            notices.append(f"[INFO] {symbol} {chunk_start}~{chunk_end}: {chunk_msg}")

        if bars:
            got_dates = {x.ts_utc[:10] for x in bars}
            cursor = chunk_start
            while cursor <= chunk_end:
                if cursor.weekday() < 5:
                    day = cursor.isoformat()
                    if day not in got_dates:
                        notices.append(f"[INFO] {symbol} {day}: no data returned from yfinance for this weekday.")
                cursor += timedelta(days=1)

            conn.executemany(
                """
                INSERT INTO intraday_bars (symbol, interval, ts_utc, open, high, low, close, volume, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, interval, ts_utc) DO UPDATE SET
                  open=excluded.open,
                  high=excluded.high,
                  low=excluded.low,
                  close=excluded.close,
                  volume=excluded.volume,
                  fetched_at=excluded.fetched_at
                """,
                [
                    (symbol, interval, b.ts_utc, b.open, b.high, b.low, b.close, b.volume, fetched_at)
                    for b in bars
                ],
            )

        if completed and bars:
            conn.execute(
                """
                INSERT INTO fetched_chunks_v2 (symbol, interval, chunk_start, chunk_end, fetched_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(symbol, interval, chunk_start, chunk_end) DO UPDATE SET
                  fetched_at=excluded.fetched_at
                """,
                (symbol, interval, chunk_start.isoformat(), chunk_end.isoformat(), fetched_at),
            )

        conn.commit()
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    rows = conn.execute(
        """
        SELECT ts_utc, open, high, low, close, volume
        FROM intraday_bars
        WHERE symbol=? AND interval=? AND ts_utc >= ? AND ts_utc <= ?
        ORDER BY ts_utc
        """,
        (symbol, interval, start_dt.isoformat(), end_dt.isoformat()),
    ).fetchall()
    conn.close()

    bars = [
        IntradayBar(
            ts_utc=r[0],
            open=float(r[1]),
            high=float(r[2]),
            low=float(r[3]),
            close=float(r[4]),
            volume=float(r[5]),
        )
        for r in rows
    ]
    return bars, notices


def price_to_tick(price: float, tick_size: float) -> int:
    return int(round(price / tick_size))


def tick_to_price(tick: int, tick_size: float) -> float:
    return round(tick * tick_size, 2)


def value_area_from_hist(values: np.ndarray, poc_idx: int, target_ratio: float = 0.70) -> tuple[int, int]:
    total = float(values.sum())
    if total <= 0:
        return poc_idx, poc_idx

    left = right = poc_idx
    covered = float(values[poc_idx])
    target = total * target_ratio

    n = len(values)
    while covered < target and (left > 0 or right < n - 1):
        left_val = float(values[left - 1]) if left > 0 else -1.0
        right_val = float(values[right + 1]) if right < n - 1 else -1.0

        if right_val >= left_val and right < n - 1:
            right += 1
            covered += float(values[right])
        elif left > 0:
            left -= 1
            covered += float(values[left])
        else:
            break
    return left, right


def bars_to_dataframe(bars: list[IntradayBar]) -> pd.DataFrame:
    df = pd.DataFrame(
        {
            "ts_utc": [b.ts_utc for b in bars],
            "open": [b.open for b in bars],
            "high": [b.high for b in bars],
            "low": [b.low for b in bars],
            "close": [b.close for b in bars],
            "volume": [b.volume for b in bars],
        }
    )
    if df.empty:
        return df
    df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)
    df = df.sort_values("ts_utc").reset_index(drop=True)
    return df


def infer_profile_metrics_df(df: pd.DataFrame, tick_size: float, ib_minutes: int, interval: str) -> dict[str, float | int | str]:
    if df.empty:
        raise ValueError("No valid intraday bars available for analysis.")

    low = float(df["low"].min())
    high = float(df["high"].max())
    total_volume = float(df["volume"].sum())

    low_ticks = np.floor(df["low"].to_numpy(dtype=float) / tick_size).astype(int)
    high_ticks = np.ceil(df["high"].to_numpy(dtype=float) / tick_size).astype(int)
    volumes = df["volume"].to_numpy(dtype=float)

    tick_floor = int(low_ticks.min())
    tick_ceil = int(high_ticks.max())
    tick_levels = np.arange(tick_floor, tick_ceil + 1, dtype=int)
    tpo_hist = np.zeros(len(tick_levels), dtype=float)
    vol_hist = np.zeros(len(tick_levels), dtype=float)

    for lo, hi, vol in zip(low_ticks, high_ticks, volumes):
        if hi < lo:
            continue
        s = lo - tick_floor
        e = hi - tick_floor + 1
        steps = e - s
        if steps <= 0:
            continue
        tpo_hist[s:e] += 1.0
        vol_hist[s:e] += vol / steps

    if not np.any(tpo_hist) or not np.any(vol_hist):
        raise ValueError("Unable to construct profile histograms from intraday bars.")

    mid_tick = int(round(((high + low) / 2.0) / tick_size))
    dist = np.abs(tick_levels - mid_tick)
    tpo_order = np.lexsort((dist, -tpo_hist))
    vol_order = np.lexsort((dist, -vol_hist))
    tpo_poc_idx = int(tpo_order[0])
    vol_poc_idx = int(vol_order[0])

    tpo_val_idx, tpo_vah_idx = value_area_from_hist(tpo_hist, tpo_poc_idx, 0.70)
    vol_val_idx, vol_vah_idx = value_area_from_hist(vol_hist, vol_poc_idx, 0.70)

    tpo_above = int(tpo_hist[tpo_poc_idx + 1 :].sum())
    tpo_below = int(tpo_hist[:tpo_poc_idx].sum())
    volume_above = float(vol_hist[vol_poc_idx + 1 :].sum())
    volume_below = float(vol_hist[:vol_poc_idx].sum())

    avg_subperiod_range = float((df["high"] - df["low"]).mean())
    close_diff = df["close"].diff().fillna(0.0).to_numpy(dtype=float)
    rotation_factor = int(np.sign(close_diff).sum())

    period_start_ts = df["ts_utc"].iloc[0]
    period_end_ts = df["ts_utc"].iloc[-1]
    ib_end = period_start_ts + pd.Timedelta(minutes=ib_minutes)
    ib_slice = df[df["ts_utc"] <= ib_end]
    if ib_slice.empty:
        ib_slice = df.iloc[:1]
    ib_high = float(ib_slice["high"].max())
    ib_low = float(ib_slice["low"].min())

    metrics = {
        "period_start": period_start_ts.isoformat(),
        "period_end": period_end_ts.isoformat(),
        "bars": int(len(df)),
        "interval": interval,
        "profile_high": round(high, 2),
        "profile_low": round(low, 2),
        "profile_range": round(high - low, 2),
        "tpo_poc": tick_to_price(int(tick_levels[tpo_poc_idx]), tick_size),
        "tpo_vah": tick_to_price(int(tick_levels[tpo_vah_idx]), tick_size),
        "tpo_val": tick_to_price(int(tick_levels[tpo_val_idx]), tick_size),
        "total_tpo_count": int(tpo_hist.sum()),
        "tpo_above": tpo_above,
        "tpo_below": tpo_below,
        "volume_poc": tick_to_price(int(tick_levels[vol_poc_idx]), tick_size),
        "volume_vah": tick_to_price(int(tick_levels[vol_vah_idx]), tick_size),
        "volume_val": tick_to_price(int(tick_levels[vol_val_idx]), tick_size),
        "volume_above": round(volume_above, 2),
        "volume_below": round(volume_below, 2),
        "total_volume": round(total_volume, 2),
        "ib_high": round(ib_high, 2),
        "ib_low": round(ib_low, 2),
        "ib_width": round(ib_high - ib_low, 2),
        "rotation_factor": rotation_factor,
        "avg_subperiod_range": round(avg_subperiod_range, 2),
    }
    return metrics


def infer_profile_metrics(bars: list[IntradayBar], tick_size: float, ib_minutes: int, interval: str) -> dict[str, float | int | str]:
    if not bars:
        raise ValueError("No intraday bars available for analysis.")
    return infer_profile_metrics_df(
        df=bars_to_dataframe(bars),
        tick_size=tick_size,
        ib_minutes=ib_minutes,
        interval=interval,
    )


def save_weekly_profile_analysis(
    db_path: Path,
    symbol: str,
    interval: str,
    tick_size: float,
    weekly_rows: list[dict],
    notices: list[str],
) -> None:
    conn = init_cache(db_path)
    created_at = datetime.now(timezone.utc).isoformat()
    for row in weekly_rows:
        metrics = row["metrics"]
        conn.execute(
            """
            INSERT INTO weekly_profile_analysis (
                symbol, interval, tick_size, week_start, week_end,
                period_start, period_end, bars,
                profile_high, profile_low, profile_range,
                tpo_poc, tpo_vah, tpo_val,
                total_tpo_count, tpo_above, tpo_below,
                volume_poc, volume_vah, volume_val,
                volume_above, volume_below, total_volume,
                ib_high, ib_low, ib_width,
                rotation_factor, avg_subperiod_range,
                notes_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, interval, week_start, week_end) DO UPDATE SET
                tick_size=excluded.tick_size,
                period_start=excluded.period_start,
                period_end=excluded.period_end,
                bars=excluded.bars,
                profile_high=excluded.profile_high,
                profile_low=excluded.profile_low,
                profile_range=excluded.profile_range,
                tpo_poc=excluded.tpo_poc,
                tpo_vah=excluded.tpo_vah,
                tpo_val=excluded.tpo_val,
                total_tpo_count=excluded.total_tpo_count,
                tpo_above=excluded.tpo_above,
                tpo_below=excluded.tpo_below,
                volume_poc=excluded.volume_poc,
                volume_vah=excluded.volume_vah,
                volume_val=excluded.volume_val,
                volume_above=excluded.volume_above,
                volume_below=excluded.volume_below,
                total_volume=excluded.total_volume,
                ib_high=excluded.ib_high,
                ib_low=excluded.ib_low,
                ib_width=excluded.ib_width,
                rotation_factor=excluded.rotation_factor,
                avg_subperiod_range=excluded.avg_subperiod_range,
                notes_json=excluded.notes_json,
                created_at=excluded.created_at
            """,
            (
                symbol,
                interval,
                tick_size,
                row["week_start"],
                row["week_end"],
                metrics["period_start"],
                metrics["period_end"],
                metrics["bars"],
                metrics["profile_high"],
                metrics["profile_low"],
                metrics["profile_range"],
                metrics["tpo_poc"],
                metrics["tpo_vah"],
                metrics["tpo_val"],
                metrics["total_tpo_count"],
                metrics["tpo_above"],
                metrics["tpo_below"],
                metrics["volume_poc"],
                metrics["volume_vah"],
                metrics["volume_val"],
                metrics["volume_above"],
                metrics["volume_below"],
                metrics["total_volume"],
                metrics["ib_high"],
                metrics["ib_low"],
                metrics["ib_width"],
                metrics["rotation_factor"],
                metrics["avg_subperiod_range"],
                json.dumps(notices, ensure_ascii=False),
                created_at,
            ),
        )
    conn.commit()
    conn.close()


def build_weekly_comparisons(weekly_rows: list[dict]) -> list[dict]:
    if not weekly_rows:
        return []
    df = pd.DataFrame(
        {
            "week_start": [r["week_start"] for r in weekly_rows],
            "week_end": [r["week_end"] for r in weekly_rows],
            "tpo_poc": [r["metrics"]["tpo_poc"] for r in weekly_rows],
            "tpo_vah": [r["metrics"]["tpo_vah"] for r in weekly_rows],
            "tpo_val": [r["metrics"]["tpo_val"] for r in weekly_rows],
            "volume_poc": [r["metrics"]["volume_poc"] for r in weekly_rows],
            "ib_width": [r["metrics"]["ib_width"] for r in weekly_rows],
            "rotation_factor": [r["metrics"]["rotation_factor"] for r in weekly_rows],
            "total_volume": [r["metrics"]["total_volume"] for r in weekly_rows],
            "bars": [r["metrics"]["bars"] for r in weekly_rows],
        }
    ).sort_values("week_start").reset_index(drop=True)

    df["prev_week_start"] = df["week_start"].shift(1)
    df["prev_week_end"] = df["week_end"].shift(1)
    df["poc_change"] = df["tpo_poc"].diff().fillna(0.0)
    df["vah_change"] = df["tpo_vah"].diff().fillna(0.0)
    df["val_change"] = df["tpo_val"].diff().fillna(0.0)
    df["volume_poc_change"] = df["volume_poc"].diff().fillna(0.0)
    df["ib_width_change"] = df["ib_width"].diff().fillna(0.0)
    df["rotation_factor_change"] = df["rotation_factor"].diff().fillna(0).astype(int)
    df["total_volume_change"] = df["total_volume"].diff().fillna(0.0)
    df["bars_change"] = df["bars"].diff().fillna(0).astype(int)

    # Simple directional label based on POC and VA shifts.
    df["direction_bias"] = np.select(
        [
            (df["poc_change"] > 0) & (df["vah_change"] > 0) & (df["val_change"] > 0),
            (df["poc_change"] < 0) & (df["vah_change"] < 0) & (df["val_change"] < 0),
        ],
        ["up_shift", "down_shift"],
        default="mixed",
    )

    out: list[dict] = []
    for row in df.to_dict(orient="records"):
        out.append(
            {
                "week_start": row["week_start"],
                "week_end": row["week_end"],
                "prev_week_start": row["prev_week_start"],
                "prev_week_end": row["prev_week_end"],
                "poc_change": round(float(row["poc_change"]), 2),
                "vah_change": round(float(row["vah_change"]), 2),
                "val_change": round(float(row["val_change"]), 2),
                "volume_poc_change": round(float(row["volume_poc_change"]), 2),
                "ib_width_change": round(float(row["ib_width_change"]), 2),
                "rotation_factor_change": int(row["rotation_factor_change"]),
                "total_volume_change": round(float(row["total_volume_change"]), 2),
                "bars_change": int(row["bars_change"]),
                "direction_bias": str(row["direction_bias"]),
            }
        )
    return out


def save_weekly_comparisons(db_path: Path, symbol: str, interval: str, comparisons: list[dict]) -> None:
    if not comparisons:
        return
    conn = init_cache(db_path)
    created_at = datetime.now(timezone.utc).isoformat()
    for c in comparisons:
        conn.execute(
            """
            INSERT INTO weekly_profile_comparison (
                symbol, interval, week_start, week_end, prev_week_start, prev_week_end,
                poc_change, vah_change, val_change, volume_poc_change,
                ib_width_change, rotation_factor_change, total_volume_change, bars_change,
                direction_bias, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, interval, week_start, week_end) DO UPDATE SET
                prev_week_start=excluded.prev_week_start,
                prev_week_end=excluded.prev_week_end,
                poc_change=excluded.poc_change,
                vah_change=excluded.vah_change,
                val_change=excluded.val_change,
                volume_poc_change=excluded.volume_poc_change,
                ib_width_change=excluded.ib_width_change,
                rotation_factor_change=excluded.rotation_factor_change,
                total_volume_change=excluded.total_volume_change,
                bars_change=excluded.bars_change,
                direction_bias=excluded.direction_bias,
                created_at=excluded.created_at
            """,
            (
                symbol,
                interval,
                c["week_start"],
                c["week_end"],
                c["prev_week_start"],
                c["prev_week_end"],
                c["poc_change"],
                c["vah_change"],
                c["val_change"],
                c["volume_poc_change"],
                c["ib_width_change"],
                c["rotation_factor_change"],
                c["total_volume_change"],
                c["bars_change"],
                c["direction_bias"],
                created_at,
            ),
        )
    conn.commit()
    conn.close()


def run(
    symbol: str,
    lookback_days: int,
    interval: str,
    tick_size: float,
    ib_minutes: int,
    cache_db: Path,
    sleep_seconds: float,
    full_weeks: int,
) -> dict:
    today = datetime.now(timezone.utc).date()
    if full_weeks > 0:
        start_date, end_date = compute_complete_week_window(today, full_weeks)
        effective_lookback_days = (end_date - start_date).days + 1
    else:
        end_date = today
        start_date = end_date - timedelta(days=lookback_days)
        effective_lookback_days = lookback_days

    bars, notices = fetch_intraday_bars(
        symbol=symbol,
        interval=interval,
        db_path=cache_db,
        sleep_seconds=sleep_seconds,
        start_date=start_date,
        end_date=end_date,
    )

    for msg in notices:
        print(msg)

    if not bars:
        return {
            "status": "no_data",
            "symbol": symbol,
            "lookback_days": effective_lookback_days,
            "interval": interval,
            "window_start": start_date.isoformat(),
            "window_end": end_date.isoformat(),
            "message": "No intraday bars available.",
        }

    df = bars_to_dataframe(bars)
    df["trade_date"] = df["ts_utc"].dt.date
    df["week_start"] = (
        (df["ts_utc"].dt.normalize() - pd.to_timedelta(df["ts_utc"].dt.weekday, unit="D"))
        .dt.date
    )
    df["week_end"] = df["week_start"] + timedelta(days=4)

    weekly_rows: list[dict] = []
    for week_start, gdf in df.groupby("week_start", sort=True):
        week_end = (week_start + timedelta(days=4)).isoformat()
        gdf = gdf.sort_values("ts_utc").reset_index(drop=True)
        metrics = infer_profile_metrics_df(
            df=gdf[["ts_utc", "open", "high", "low", "close", "volume"]],
            tick_size=tick_size,
            ib_minutes=ib_minutes,
            interval=interval,
        )
        weekly_rows.append(
            {
                "week_start": week_start.isoformat(),
                "week_end": week_end,
                "metrics": metrics,
            }
        )

    save_weekly_profile_analysis(
        db_path=cache_db,
        symbol=symbol,
        interval=interval,
        tick_size=tick_size,
        weekly_rows=weekly_rows,
        notices=notices,
    )
    comparisons = build_weekly_comparisons(weekly_rows)
    save_weekly_comparisons(
        db_path=cache_db,
        symbol=symbol,
        interval=interval,
        comparisons=comparisons,
    )

    return {
        "status": "ok",
        "symbol": symbol,
        "lookback_days": effective_lookback_days,
        "interval": interval,
        "window_start": start_date.isoformat(),
        "window_end": end_date.isoformat(),
        "bars": len(bars),
        "weeks_processed": len(weekly_rows),
        "weekly_metrics": weekly_rows,
        "weekly_comparison_summary": comparisons,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch yfinance intraday bars and store inferred profile metrics into SQLite.")
    parser.add_argument("--symbol", default="ES=F", help="Yahoo symbol, default ES=F")
    parser.add_argument("--lookback-days", type=int, default=7, help="Lookback window in calendar days")
    parser.add_argument("--interval", default="30m", help="yfinance interval, default 30m")
    parser.add_argument("--tick-size", type=float, default=0.25, help="Price tick size, default 0.25")
    parser.add_argument("--ib-minutes", type=int, default=60, help="Initial Balance window in minutes, default 60")
    parser.add_argument("--sleep-seconds", type=float, default=2.0, help="Sleep between chunk requests, default 2.0")
    parser.add_argument(
        "--full-weeks",
        type=int,
        default=0,
        help="If >0, fetch the latest completed N full weeks (Mon-Fri) instead of lookback-days",
    )
    parser.add_argument("--cache-db", default="data/cache/yahoo_cache.sqlite", help="SQLite cache DB path")
    args = parser.parse_args()

    result = run(
        symbol=args.symbol,
        lookback_days=args.lookback_days,
        interval=args.interval,
        tick_size=args.tick_size,
        ib_minutes=args.ib_minutes,
        cache_db=Path(args.cache_db),
        sleep_seconds=max(0.0, args.sleep_seconds),
        full_weeks=max(0, args.full_weeks),
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
