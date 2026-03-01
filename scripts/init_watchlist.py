#!/usr/bin/env python3
"""Initialize stock watchlist in local SQLite database."""

from __future__ import annotations

import sqlite3
from pathlib import Path


WATCHLIST_ROWS = [
    # Magnificent 7
    ("AAPL", "Apple Inc.", "magnificent7", 1, 1),
    ("MSFT", "Microsoft Corp.", "magnificent7", 1, 1),
    ("NVDA", "NVIDIA Corp.", "magnificent7", 1, 1),
    ("AMZN", "Amazon.com Inc.", "magnificent7", 1, 1),
    ("GOOGL", "Alphabet Inc. Class A", "magnificent7", 1, 1),
    ("META", "Meta Platforms Inc.", "magnificent7", 1, 1),
    ("TSLA", "Tesla Inc.", "magnificent7", 1, 1),
    # S&P mega caps / large leaders
    ("BRK-B", "Berkshire Hathaway Inc. Class B", "sp500_large", 2, 1),
    ("AVGO", "Broadcom Inc.", "sp500_large", 2, 1),
    ("LLY", "Eli Lilly and Co.", "sp500_large", 2, 1),
    ("JPM", "JPMorgan Chase & Co.", "sp500_large", 2, 1),
    ("XOM", "Exxon Mobil Corp.", "sp500_large", 2, 1),
    ("UNH", "UnitedHealth Group Inc.", "sp500_large", 2, 1),
    ("V", "Visa Inc. Class A", "sp500_large", 2, 1),
    ("MA", "Mastercard Inc. Class A", "sp500_large", 2, 1),
    ("COST", "Costco Wholesale Corp.", "sp500_large", 2, 1),
    ("WMT", "Walmart Inc.", "sp500_large", 2, 1),
    # Quantum-related names
    ("IONQ", "IonQ Inc.", "quantum", 3, 1),
    ("RGTI", "Rigetti Computing Inc.", "quantum", 3, 1),
    ("QBTS", "D-Wave Quantum Inc.", "quantum", 3, 1),
    ("QUBT", "Quantum Computing Inc.", "quantum", 3, 1),
    ("ARQQ", "Arqit Quantum Inc.", "quantum", 3, 1),
    ("QTUM", "Defiance Quantum ETF", "quantum", 3, 1),
    ("TTAN", "ServiceTitan Inc.", "custom", 2, 1),
    # Common ETFs
    ("SPY", "SPDR S&P 500 ETF Trust", "etf_core", 2, 1),
    ("VOO", "Vanguard S&P 500 ETF", "etf_core", 2, 1),
    ("IVV", "iShares Core S&P 500 ETF", "etf_core", 2, 1),
    ("VTI", "Vanguard Total Stock Market ETF", "etf_core", 2, 1),
    ("QQQ", "Invesco QQQ Trust", "etf_core", 2, 1),
    ("TQQQ", "ProShares UltraPro QQQ", "etf_leveraged", 3, 1),
    ("SQQQ", "ProShares UltraPro Short QQQ", "etf_leveraged", 3, 1),
    ("DIA", "SPDR Dow Jones Industrial Average ETF", "etf_core", 3, 1),
    ("IWM", "iShares Russell 2000 ETF", "etf_core", 3, 1),
    ("VXUS", "Vanguard Total International Stock ETF", "etf_international", 2, 1),
    ("VEA", "Vanguard FTSE Developed Markets ETF", "etf_international", 3, 1),
    ("VWO", "Vanguard FTSE Emerging Markets ETF", "etf_international", 3, 1),
    ("BND", "Vanguard Total Bond Market ETF", "etf_bond", 3, 1),
    ("TLT", "iShares 20+ Year Treasury Bond ETF", "etf_bond", 3, 1),
    ("IEF", "iShares 7-10 Year Treasury Bond ETF", "etf_bond", 3, 1),
    ("XLK", "Technology Select Sector SPDR Fund", "etf_sector", 3, 1),
    ("XLF", "Financial Select Sector SPDR Fund", "etf_sector", 3, 1),
    ("XLV", "Health Care Select Sector SPDR Fund", "etf_sector", 3, 1),
    ("XLE", "Energy Select Sector SPDR Fund", "etf_sector", 3, 1),
    ("XLI", "Industrial Select Sector SPDR Fund", "etf_sector", 3, 1),
    ("XLY", "Consumer Discretionary Select Sector SPDR Fund", "etf_sector", 3, 1),
    ("XLP", "Consumer Staples Select Sector SPDR Fund", "etf_sector", 3, 1),
    ("XLU", "Utilities Select Sector SPDR Fund", "etf_sector", 3, 1),
    ("XLC", "Communication Services Select Sector SPDR Fund", "etf_sector", 3, 1),
    ("XLRE", "Real Estate Select Sector SPDR Fund", "etf_sector", 3, 1),
    ("VIXY", "ProShares VIX Short-Term Futures ETF", "etf_volatility", 3, 1),
    # Crypto (Yahoo symbols)
    ("BTC-USD", "Bitcoin USD", "crypto", 2, 1),
    ("ETH-USD", "Ethereum USD", "crypto", 2, 1),
    ("SOL-USD", "Solana USD", "crypto", 2, 1),
    ("DOGE-USD", "Dogecoin USD", "crypto", 2, 1),
    ("XRP-USD", "XRP USD", "crypto", 3, 1),
    ("ADA-USD", "Cardano USD", "crypto", 3, 1),
    ("BNB-USD", "BNB USD", "crypto", 3, 1),
    ("AVAX-USD", "Avalanche USD", "crypto", 3, 1),
    ("LINK-USD", "Chainlink USD", "crypto", 3, 1),
    ("LTC-USD", "Litecoin USD", "crypto", 3, 1),
]


def main() -> None:
    db_path = Path("data/cache/yahoo_cache.sqlite")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS watchlist_stocks (
            ticker TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            priority INTEGER NOT NULL DEFAULT 3,
            is_active INTEGER NOT NULL DEFAULT 1,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )

    conn.executemany(
        """
        INSERT INTO watchlist_stocks (ticker, name, category, priority, is_active, updated_at)
        VALUES (?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(ticker) DO UPDATE SET
            name = excluded.name,
            category = excluded.category,
            priority = excluded.priority,
            is_active = excluded.is_active,
            updated_at = datetime('now')
        """,
        WATCHLIST_ROWS,
    )

    conn.commit()
    conn.close()
    print(f"Upserted {len(WATCHLIST_ROWS)} watchlist rows into {db_path}")


if __name__ == "__main__":
    main()
