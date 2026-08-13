"""Simulated daily prices: one shared market factor plus idiosyncratic noise.

Each instrument's daily log-return is beta * market + own noise, so instruments
move together the way a real book does. Bonds are quoted as a percentage of par
around 100 with low volatility; everything else is per unit. Returns are
Gaussian by construction: no fat tails, no volatility clustering. That is fine
for market value and exposure (sums), and is exactly why VaR is out of scope
until a fatter model justifies it.

The market path is CONDITIONED: draws are resampled until the factor's
cumulative return lands inside a declared band (MARKET_CUMRET_BAND, scaled to
the window length). An unconditioned draw once landed at -1.47 sigma, which
made every performance figure in the book negative and inverted the
return-by-risk-profile ordering -- true of that sample path, useless for a
dataset whose job is letting KPIs be demonstrated. Only the shared factor is
conditioned; betas, dispersion and idiosyncratic noise are untouched, and the
conditioning is disclosed here rather than discovered later.

Prices also carry a small imperfection layer, because a feed with 100%
coverage gives a data-quality panel nothing to report: a thin slice of rows
repeats the previous close as price_source='carried_forward', and a thinner
slice of instrument-days emits no row at all. Both are derived from the ISIN
hash -- instrument-intrinsic, stable across seeds, zero rng draws consumed.
"""

from __future__ import annotations

from bisect import bisect_left
from datetime import date

import numpy as np

from . import config, income
from .universe import OPEN_ENDED, Universe

DAYS = config.TRADING_DAYS_PER_YEAR
PAR = 100.0


def _pull_to_par(paths: np.ndarray, universe: Universe, sessions: list[str]):
    """Percent-of-par instruments converge on 100 as redemption approaches.

    A bond's redemption value is certain, so its price cannot wander freely
    near maturity -- the closer the date, the tighter the price is pinned. The
    maturity dates are the real ones from FIRDS; instruments with no maturity
    (perpetuals, and everything not quoted per cent of par) are untouched.
    """
    day0 = date.fromisoformat(sessions[0])
    span = np.array(
        [(date.fromisoformat(d) - day0).days for d in sessions], dtype=float
    )
    for j, row in enumerate(universe.rows):
        if row['price_convention'] != 'percent_of_par':
            continue
        if row['maturity_dt'] >= OPEN_ENDED:
            continue
        try:
            days_to_maturity = (date.fromisoformat(row['maturity_dt']) - day0).days
        except ValueError:
            continue
        if days_to_maturity <= 0:
            continue
        # weight falls linearly from 1 to 0 as the redemption date arrives
        weight = np.clip(1.0 - span / days_to_maturity, 0.0, 1.0)
        paths[:, j] = PAR + (paths[:, j] - PAR) * weight
    return paths


def _drop_on_ex_date(paths: np.ndarray, universe: Universe, sessions: list[str]):
    """A share drops by its dividend on the ex date.

    Without this, price return and income are the same money counted twice, and
    any total-return figure double-counts. The drop is a level shift carried
    forward, which is what separates a price series from a total-return series.
    """
    years = range(int(sessions[0][:4]), int(sessions[-1][:4]) + 1)
    for j, row in enumerate(universe.rows):
        for per_share, month, day in income.dividend_payments(row):
            for year in years:
                ex = f'{year}-{month:02d}-{day:02d}'
                if not (sessions[0] <= ex <= sessions[-1]):
                    continue
                t = bisect_left(sessions, ex)
                if t >= len(sessions):
                    continue
                # never drive a price to nothing: a dividend larger than the
                # share price is not a dividend, it is a return of capital
                drop = min(per_share, max(paths[t:, j].min() - 0.01, 0.0))
                if drop > 0:
                    paths[t:, j] -= drop
    return paths


def simulate_prices(
    cfg: config.GenConfig,
    universe: Universe,
    sessions: list[str],
    fx,
    rng_market: np.random.Generator,
    rng_idio: np.random.Generator,
):
    n_days = len(sessions)
    n_inst = len(universe.rows)

    mu_d = config.MARKET_DRIFT_ANNUAL / DAYS
    sig_m = config.MARKET_VOL_ANNUAL / np.sqrt(DAYS)
    # Conditioned draw (see module docstring). The band is stated per year and
    # scaled to the window, so a three-month fixture window is not asked to
    # deliver a two-year bull run. Draws consume the market stream in order,
    # which keeps the accepted path a pure function of the seed.
    years = n_days / DAYS
    lo, hi = (b * years for b in config.MARKET_CUMRET_BAND)
    market = None
    closest, closest_dist = None, float('inf')
    for _ in range(config.MARKET_MAX_REDRAWS):
        draw = rng_market.normal(mu_d, sig_m, size=n_days)
        cum = float(np.expm1(draw.sum()))
        if lo <= cum <= hi:
            market = draw
            break
        if abs(cum - (lo + hi) / 2) < closest_dist:
            closest, closest_dist = draw, abs(cum - (lo + hi) / 2)
    if market is None:
        # the band was never met: take the closest draw rather than fail the
        # run -- the miss is visible in the measured cumulative return
        market = closest

    betas = np.empty(n_inst)
    sig_i = np.empty(n_inst)
    start = np.empty(n_inst)
    for j, row in enumerate(universe.rows):
        b_lo, b_hi, v_lo, v_hi, s_lo, s_hi = config.PRICE_PARAMS[row['asset_class']]
        beta = rng_idio.uniform(b_lo, b_hi)
        total_annual = rng_idio.uniform(v_lo, v_hi)
        # cap beta so idiosyncratic variance stays real: an uncapped
        # high-beta/low-vol draw collapses idio onto the 1e-10 floor and turns
        # the configured total vol into a lie (a pure index tracker)
        beta = min(beta, 0.95 * total_annual / config.MARKET_VOL_ANNUAL)
        var_idio = max(
            (total_annual**2 - (beta * config.MARKET_VOL_ANNUAL) ** 2) / DAYS, 1e-10
        )
        betas[j] = beta
        sig_i[j] = np.sqrt(var_idio)
        level = rng_idio.uniform(s_lo, s_hi)
        # Quote the instrument in ITS OWN currency. The configured bands are
        # euro-sized, so a Tokyo listing has to be scaled by the exchange rate
        # or it prints at 190 yen -- about one euro twenty -- and the book ends
        # up holding millions of units of everything priced in a weak currency.
        # Percent-of-par instruments are exempt: a bond quotes near 100 in
        # every currency, because par is a percentage, not an amount.
        if row['price_convention'] != 'percent_of_par':
            try:
                level *= fx.per_eur(row['currency'], sessions[0])
            except KeyError:
                pass
        start[j] = level

    idio = rng_idio.normal(0.0, 1.0, size=(n_days, n_inst)) * sig_i
    log_ret = market[:, None] * betas[None, :] + idio
    paths = start[None, :] * np.exp(np.cumsum(log_ret, axis=0))

    if config.PULL_TO_PAR:
        paths = _pull_to_par(paths, universe, sessions)
    paths = _drop_on_ex_date(paths, universe, sessions)

    rows = {
        'business_date': [],
        'isin': [],
        'price': [],
        'currency': [],
        'price_source': [],
        'price_convention': [],
    }
    # Imperfection layer (see module docstring). Knuth's multiplicative hash
    # spreads (isin, session) pairs; the two thresholds partition [0, 1000).
    # The first emitted session per instrument is exempt -- there is no
    # previous close to carry, and a missing first row would look like a
    # later listing date rather than a feed gap.
    stale_lt = config.PRICE_STALE_PER_MILLE
    missing_lt = stale_lt + config.PRICE_MISSING_PER_MILLE
    hashes = [income.isin_hash(inst['isin']) for inst in universe.rows]
    last_price: dict[int, float] = {}
    for t, day in enumerate(sessions):
        for j, inst in enumerate(universe.rows):
            if not (inst['first_trade_dt'] <= day <= inst['last_tradeable_dt']):
                continue
            lot = (hashes[j] + t * 2654435761) % 1000
            first_emission = j not in last_price
            if not first_emission and stale_lt <= lot < missing_lt:
                continue  # a genuinely missing instrument-day
            if not first_emission and lot < stale_lt:
                price, source = last_price[j], 'carried_forward'
            else:
                price, source = round(float(paths[t, j]), 4), 'simulated'
            last_price[j] = price
            rows['business_date'].append(day)
            rows['isin'].append(inst['isin'])
            rows['price'].append(price)
            rows['currency'].append(inst['currency'])
            rows['price_source'].append(source)
            rows['price_convention'].append(inst['price_convention'])
    return rows, paths
