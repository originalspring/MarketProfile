# MarketProfile

## Bootstrap On A New Machine

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install yfinance pandas numpy
python scripts/init_watchlist.py
```

## Run Single Symbol (Latest 3 Full Weeks)

```bash
python scripts/fetch_es_yahoo_and_profile.py --symbol ES=F --full-weeks 3 --interval 30m --cache-db data/cache/yahoo_cache.sqlite
```

## Run Full Watchlist (Latest 3 Full Weeks)

```bash
python scripts/run_watchlist_full_weeks.py --cache-db data/cache/yahoo_cache.sqlite --full-weeks 3 --interval 30m --sleep-seconds 1 --between-tickers 0.4
```

## Core SQLite Tables (Current Pipeline)

- `watchlist_stocks`
- `intraday_bars`
- `fetched_chunks_v2`
- `weekly_profile_analysis`
- `weekly_profile_comparison`
- `watchlist_batch_runs`

## Useful Queries

Latest batch status:

```bash
sqlite3 -header -column data/cache/yahoo_cache.sqlite "SELECT batch_id, COUNT(*) AS rows, SUM(CASE WHEN status='ok' THEN 1 ELSE 0 END) AS ok_rows, SUM(CASE WHEN status='no_data' THEN 1 ELSE 0 END) AS no_data_rows, SUM(CASE WHEN status='error' THEN 1 ELSE 0 END) AS error_rows FROM watchlist_batch_runs GROUP BY batch_id ORDER BY batch_id DESC LIMIT 1;"
```

Latest week-by-week profile for a ticker:

```bash
sqlite3 -header -column data/cache/yahoo_cache.sqlite "SELECT symbol, week_start, week_end, tpo_poc, tpo_vah, tpo_val, total_volume FROM weekly_profile_analysis WHERE symbol='ES=F' AND interval='30m' ORDER BY week_start;"
```

Latest weekly comparison for a ticker:

```bash
sqlite3 -header -column data/cache/yahoo_cache.sqlite "SELECT symbol, week_start, week_end, direction_bias, poc_change, vah_change, val_change, total_volume_change FROM weekly_profile_comparison WHERE symbol='ES=F' AND interval='30m' ORDER BY week_start;"
```
