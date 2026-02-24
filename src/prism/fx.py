"""Foreign exchange rate conversion using the free Frankfurter API."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger("prism.fx")

# Memory cache to avoid hitting the API multiple times for the same date/currency
_RATES_CACHE: dict[tuple[str, str], float] = {}


def convert_currency(
    amount: float,
    from_currency: str,
    to_currency: str = "EUR",
    date: str = "latest",
) -> float:
    """Convert an amount from one currency to another using historical rates.

    Args:
        amount: The monetary value to convert (can be negative).
        from_currency: 3-letter currency code (e.g. 'USD', 'GBP').
        to_currency: 3-letter currency code (e.g. 'EUR').
        date: YYYY-MM-DD string. Use 'latest' for current rate.

    Returns:
        The converted amount, rounded to 2 decimal places.
        If the API fails, logs a warning and returns the original amount.
    """
    from_currency = from_currency.upper()
    to_currency = to_currency.upper()

    if from_currency == to_currency:
        return amount

    # Check cache first (date, from_currency)
    cache_key = (date, from_currency)
    if cache_key in _RATES_CACHE:
        rate = _RATES_CACHE[cache_key]
        return round(amount * rate, 2)

    try:
        import httpx

        # Frankfurter API: https://api.frankfurter.dev/v1/2026-01-15?base=USD&symbols=EUR
        url = f"https://api.frankfurter.dev/v1/{date}"
        params = {"base": from_currency, "symbols": to_currency}
        
        with httpx.Client(timeout=10.0) as client:
            resp = client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

            rates = data.get("rates", {})
            if to_currency not in rates:
                raise ValueError(f"Rate for {to_currency} not found in response")

            rate = float(rates[to_currency])
            
            # Cache the rate
            _RATES_CACHE[cache_key] = rate
            
            logger.info("FX %s: 1 %s = %.4f %s", date, from_currency, rate, to_currency)
            return round(amount * rate, 2)

    except Exception as e:
        logger.warning(
            "FX conversion failed for %s to %s on %s: %s. Returning original amount.",
            from_currency, to_currency, date, e
        )
        return amount

