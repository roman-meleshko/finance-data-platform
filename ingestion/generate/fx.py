"""Currency conversion against the real ECB reference rates.

A private-banking account holds a cash sub-account per currency. Buying a
Tokyo-listed share out of a euro account is two bookings, not one: an FX
conversion that funds the yen sub-account, then the purchase. Modelling the
purchase alone lets the ledger spend currencies it was never funded in, which
is how a cash balance that looks fine in aggregate turns out to be short yen in
every account.

Rates come from the same ECB reference file the warehouse ingests, so a mart
can re-derive every conversion and check the bank's margin. Rates are published
on ECB business days only and are carried forward across weekends and holidays,
which is the same point-in-time join the FX mart makes.

ECB quotes units of currency per one euro; EUR itself is absent from the file
and is the implicit base.
"""

from __future__ import annotations

import glob
from bisect import bisect_right

import pyarrow.parquet as pq

from . import config

BASE = 'EUR'

# Currencies with no ECB reference rate, mapped to the currency they are
# formally pegged to. Documented rather than silently dropped: the alternative
# is excluding the instrument, which hides a real coverage gap.
PEGS = {
    'BMD': ('USD', 1.0),       # Bermudian dollar, pegged 1:1 to USD since 1970
    # The lev's currency board fixes it at 1.95583 to the euro; the ECB series
    # ends 2025-12-31 (euro-adoption path), after which carry-forward serves
    # exactly this value -- the peg entry makes that a documented fact rather
    # than an accident of the last published row.
    'BGN': ('EUR', 1.95583),
}


class FxRates:
    """Point-in-time ECB rates with carry-forward, plus the bank's margin."""

    def __init__(self, by_ccy: dict[str, tuple[list[str], list[float]]]):
        self._by_ccy = by_ccy

    @classmethod
    def load(cls, cfg: config.GenConfig) -> FxRates:
        files = sorted(glob.glob(str(cfg.parquet_dir / 'ecb_fxref' / '*.parquet')))
        if not files:
            raise FileNotFoundError(
                f'no ecb_fxref parquet under {cfg.parquet_dir} -- FX conversion '
                'needs the reference rates the warehouse also loads'
            )
        rows: dict[str, dict[str, float]] = {}
        for path in files:
            table = pq.read_table(path, columns=['date', 'currency', 'fx_rate'])
            for d, ccy, rate in zip(
                table.column('date').to_pylist(),
                table.column('currency').to_pylist(),
                table.column('fx_rate').to_pylist(),
            ):
                if not d or not ccy or not rate:
                    continue
                try:
                    rows.setdefault(ccy, {})[d[:10]] = float(rate)
                except ValueError:
                    continue
        by_ccy = {}
        for ccy, series in rows.items():
            dates = sorted(series)
            by_ccy[ccy] = (dates, [series[d] for d in dates])
        return cls(by_ccy)

    def per_eur(self, currency: str, day: str) -> float:
        """Units of `currency` per one euro on `day`, carried forward."""
        if currency == BASE:
            return 1.0
        if currency not in self._by_ccy:
            peg, ratio = PEGS.get(currency, (None, None))
            if peg is None:
                raise KeyError(f'no ECB rate and no peg for {currency}')
            return self.per_eur(peg, day) * ratio
        dates, values = self._by_ccy[currency]
        i = bisect_right(dates, day) - 1
        if i < 0:
            return values[0]   # before the file starts: earliest published rate
        return values[i]

    def convert(self, amount: float, frm: str, to: str, day: str) -> float:
        """Mid-market conversion, no margin -- the honest reference amount."""
        if frm == to:
            return amount
        return amount / self.per_eur(frm, day) * self.per_eur(to, day)

    def buy_cost(self, amount_to: float, frm: str, to: str, day: str) -> float:
        """What the client pays in `frm` to obtain `amount_to` of `to`.

        The bank sells the client the foreign currency slightly expensively:
        the margin is the FX revenue line that every private bank earns and
        that a trade-fee-only model misses entirely.
        """
        mid = self.convert(amount_to, to, frm, day)
        return mid * (1.0 + config.FX_MARGIN_BP / 1e4)
