---
name: quant-numpy-pandas
description: Use NumPy and Pandas as the default stack for market/quant data processing. Trigger when implementing or refactoring data analysis, profile calculations, feature engineering, or statistical summaries.
---

# Quant NumPy Pandas Preference

Use `pandas` + `numpy` as the primary implementation path for data analysis.

## Rules

1. Prefer DataFrame/Series operations over hand-written Python loops when computing indicators or aggregates.
2. Prefer `numpy` arrays for histogram, binning, vectorized transforms, and numeric reductions.
3. Keep custom loops only for unavoidable path-dependent logic.
4. Avoid introducing new analysis libraries unless explicitly requested.
5. Persist outputs to SQLite when the workflow already uses local DB storage.

## Current Project Scope

- Works with Yahoo/yfinance + SQLite profile pipeline.
- Applies to TPO/Volume Profile inferred calculations and follow-up metrics.
