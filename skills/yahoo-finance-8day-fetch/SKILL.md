---
name: yahoo-finance-8day-fetch
description: Fetch Yahoo Finance historical data with anti-rate-limit chunking. Use when pulling Yahoo symbols (especially ES=F) for lookback windows longer than 7 days, and enforce 7-day request chunks while skipping weekend-only chunks.
---

# Yahoo Finance 8-Day Fetch

Use Yahoo chart API in 7-day chunks instead of one large request.

## Rules

1. Split requested lookback period into consecutive 7-day calendar chunks.
2. Skip a chunk when it contains only Saturday/Sunday.
3. Query per chunk with `interval=1d`.
4. Default to no automatic retry (`--max-retries 1`). Only increase retries when explicitly requested.
5. Add a short pause between chunks to avoid burst throttling.
6. Persist fetched bars into local SQLite cache so already-fetched dates are not re-downloaded.
7. Persist fetched chunk markers in SQLite (`symbol + chunk_start + chunk_end`) and skip network for completed chunks.
8. Merge chunk results by `date` and deduplicate.
9. Filter out weekend bars in final output.
10. If a weekday has no data, log it and continue; do not fail the run.

## SQLite Cache

Use default cache DB path: `data/cache/yahoo_cache.sqlite`.

Tables:

- `daily_bars(symbol, date, open, high, low, close, volume, fetched_at)`
- `fetched_chunks(symbol, chunk_start, chunk_end, fetched_at)`

## ES Default

Use `ES=F` as the default Yahoo symbol for S&P 500 E-mini futures.

## Execution

For this repository, run:

```bash
python3 scripts/fetch_es_yahoo_and_profile.py --symbol ES=F --lookback-days <N> --cache-db data/cache/yahoo_cache.sqlite
```

This script already applies 7-day chunking, weekend skipping, SQLite caching, and non-fatal missing-day reporting.
