# MarketProfile

## Bootstrap On A New Machine

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install yfinance pandas numpy
python scripts/init_watchlist.py
```

Then re-fetch/recompute historical windows as needed, for example:

```bash
python scripts/fetch_es_yahoo_and_profile.py --symbol ES=F --full-weeks 3 --interval 30m --cache-db data/cache/yahoo_cache.sqlite
```
