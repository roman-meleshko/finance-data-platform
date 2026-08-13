"""Instrument-intrinsic income facts, derived from the ISIN itself.

A bond's coupon and a company's dividend are properties of the instrument, not
of whoever happens to hold it, so they are derived from a hash of the ISIN
rather than from the run's seed. The same bond pays the same coupon on the same
date in every simulated world; only the holders change.

Payment frequency follows domicile convention rather than being annual for
everything: US names pay dividends quarterly, UK/Japan/Hong Kong
semi-annually, continental Europe annually; USD and GBP debt pays semi-annual
coupons, the rest annual. A book where every instrument pays once a year
between April and July is the kind of uniformity a practitioner notices in
minutes. The ANNUAL amount is drawn once and split across the payments, so
frequency changes the calendar, not the income.

Both the price model and the trade engine need these facts -- prices because a
share drops by its dividend on the ex date, trades because the holder receives
it -- so they live here rather than in either.
"""

from __future__ import annotations

import hashlib

from . import config

SEMI_COUPON_CCYS = ('USD', 'GBP')
QUARTERLY_DIV_COUNTRIES = ('US',)
SEMI_DIV_COUNTRIES = ('GB', 'JP', 'HK')


def isin_hash(isin: str) -> int:
    return int.from_bytes(hashlib.sha256(isin.encode()).digest()[:8], 'big')


def _spread(month: int, n: int) -> list[int]:
    """n payment months, evenly spaced from an anchor month."""
    return sorted((month - 1 + k * (12 // n)) % 12 + 1 for k in range(n))


def coupon_payments(inst: dict) -> list[tuple[float, int, int]]:
    """[(rate portion as % of par, month, day)] for real debt, else [].

    The portions sum to the instrument's annual rate.
    """
    if inst['asset_class'] != 'bond':
        return []
    h = isin_hash(inst['isin'])
    lo, hi = config.COUPON_RATE_RANGE
    steps = round((hi - lo) / config.COUPON_RATE_STEP) + 1
    annual = lo + (h % steps) * config.COUPON_RATE_STEP
    n = 2 if inst['currency'] in SEMI_COUPON_CCYS else 1
    month = 1 + (h >> 8) % 12
    day = 1 + (h >> 16) % 28
    return [(annual / n, m, day) for m in _spread(month, n)]


def annual_coupon_rate(inst: dict) -> float | None:
    """The instrument's annual rate as % of par, for accrual math."""
    payments = coupon_payments(inst)
    return round(sum(p[0] for p in payments), 6) if payments else None


def dividend_payments(inst: dict) -> list[tuple[float, int, int]]:
    """[(absolute amount per share, ex month, ex day)] for equities, else [].

    Declared as amounts, the way a board declares them. Yield is a derived
    statistic; making it the input meant a share that doubled paid twice as
    much, which reverses the causality.
    """
    if inst['asset_class'] != 'equity':
        return []
    h = isin_hash(inst['isin'])
    lo, hi = config.DIVIDEND_PER_SHARE_RANGE
    steps = round((hi - lo) / config.DIVIDEND_PER_SHARE_STEP) + 1
    annual = lo + ((h >> 8) % steps) * config.DIVIDEND_PER_SHARE_STEP
    country = inst['isin'][:2]
    if country in QUARTERLY_DIV_COUNTRIES:
        n = 4
    elif country in SEMI_DIV_COUNTRIES:
        n = 2
    else:
        n = 1
    m_lo, m_hi = config.DIVIDEND_MONTHS
    month = m_lo + (h >> 16) % (m_hi - m_lo + 1)
    day = 1 + (h >> 24) % 28
    per_payment = round(annual / n, 2)
    if per_payment <= 0:
        return []
    return [(per_payment, m, day) for m in _spread(month, n)]
