"""Validators for normalized records.

This subdirectory contains validator implementations:
- ohlcv.py: OHLCV-specific validators (price relationships, volume, timestamps)
- open_interest.py: OI-specific validators (amounts, ratios, settlement currencies)
- trades.py: Trade-specific validators (prices, quantities, side validity)
- composite.py: Multi-validator pipelines (apply multiple validators in sequence)
"""
