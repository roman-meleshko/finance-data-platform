"""Trade flow: per-account behaviour driven by mandate, lifecycle and season.

Every account runs its own engine:
- an account ARRIVES on-platform on a dated event. Books that predate the data
  window arrive as a free-of-payment TRANSFER_IN of securities plus a residual
  cash deposit, which is what a platform go-live actually looks like; accounts
  opened inside the window arrive as a cash DEPOSIT and then genuinely buy
  their book, which is what new money actually looks like. Nothing is rebuilt
  by pretending a 2022 client went shopping in 2024.
- after arrival, activity depends on the mandate: discretionary accounts follow
  house-model books and rebalance at quarter-ends, advisory accounts trade less
  often and chunkier, execution-only accounts trade in bursts and concentrate.
- bonds pay an annual coupon on the coupon date, and a buyer pays the seller
  the interest ACCRUED since the last coupon, because the buyer will collect
  the whole of the next one. Equities pay an absolute dividend per share with
  real ex/pay mechanics: entitlement snaps at the ex date, cash lands
  DIVIDEND_PAY_LAG_SESSIONS later, and the share price drops by the dividend
  on the ex date so price return and income are not the same money twice.
- clients top up and draw down: DEPOSIT and WITHDRAWAL events are external
  flows, and their sum is Net New Money -- the growth metric every private bank
  reports and the one thing a book with no inflows can never show.
- a share of accounts go dormant; some close entirely; August is quiet and
  quarter-ends are busy.

CASH IS HELD PER CURRENCY, which is how a real account holds it. Buying a
Tokyo-listed share out of a euro account books an FX conversion first, at the
ECB reference rate of the day plus the bank's margin; without that leg a ledger
silently spends currencies it was never funded in. The guarantee is therefore
sharper than before: for EVERY (account, currency) pair, running cash is >= 0
at every step on a trade-date basis. Buys are affordability-checked in the
currency they settle in, sell fees are capped at proceeds, income is
non-negative, and fee sweeps are capped at available cash.

Positions can never go negative: buys are step-multiples of the instrument's
real FIRDS denomination, sells are clamped to the holding.

Holdings and cash are never emitted as balances -- they are derived downstream
from the movement stream. The one exception is gen_position_snapshot, which is
deliberately produced from the engine's own state as an INDEPENDENT record, so
the warehouse has something real to reconcile against instead of an identity
that can only agree with itself.
"""

from __future__ import annotations

from bisect import bisect_left
from dataclasses import dataclass, field
from datetime import date

import numpy as np

from . import config, income
from .fx import FxRates
from .universe import OPEN_ENDED, Universe

PAR_REDEMPTION = 100.0   # a bond redeems at 100% of nominal


@dataclass
class _AccountState:
    account: dict
    params: dict
    target: int
    slice_value: float
    cash: dict[str, float]     # per-currency sub-accounts, never negative
    start_idx: int             # session index the account arrives on-platform
    migrated: bool             # book predates the window: arrives as a transfer
    dormant: bool
    profile: str               # the client's suitability profile, drives allocation
    core: np.ndarray           # instrument indices this account keeps returning to
    closes_at: int | None = None   # attrition: session the relationship ends
    burst_days: set[int] = field(default_factory=set)
    holdings: dict[int, int] = field(default_factory=dict)

    @property
    def base(self) -> str:
        return self.account['base_currency']


def house_model_name(profile: str) -> str:
    """The model book a discretionary account on this profile follows."""
    return f'MODEL_{profile.upper()}'


def pool_key(account: dict, profile: str) -> str:
    """Which buying pool an account draws from.

    A discretionary account follows its profile's house model. Everything else
    draws from a pool tilted by both its mandate and its client's profile --
    an advisory client with a conservative profile is recommended conservative
    things, which is the whole point of recording the profile. The profile is
    looked up from the client rather than copied onto the account, because
    suitability is assessed once per client.
    """
    if account['house_model']:
        return account['house_model']
    return f"{account['mandate_type']}|{profile}"


def _target_positions(rng: np.random.Generator, mult: float) -> int:
    n = rng.lognormal(
        mean=np.log(config.POSITIONS_PER_ACCOUNT_MEDIAN * mult),
        sigma=config.POSITIONS_PER_ACCOUNT_SIGMA,
    )
    return int(min(60, max(5, round(n))))


def _popularity(rng: np.random.Generator, n: int) -> np.ndarray:
    """Zipf-shaped interest over instruments: a minority takes most of the flow."""
    ranks = rng.permutation(n) + 1
    w = 1.0 / np.power(ranks, config.POPULARITY_ALPHA)
    return w / w.sum()


def _session_flags(sessions: list[str]) -> tuple[np.ndarray, np.ndarray, set[int]]:
    """August flag, quarter-end flag (last N sessions of Mar/Jun/Sep/Dec), and
    the fee-sweep days (the very last session of each quarter month)."""
    august = np.array([d[5:7] == '08' for d in sessions], dtype=bool)
    qe = np.zeros(len(sessions), dtype=bool)
    by_quarter: dict[str, list[int]] = {}
    for i, d in enumerate(sessions):
        month = d[5:7]
        if month in ('03', '06', '09', '12'):
            by_quarter.setdefault(d[:4] + 'Q' + month, []).append(i)
    for idxs in by_quarter.values():
        for i in idxs[-config.QUARTER_END_SESSIONS:]:
            qe[i] = True
    sweep_days = {idxs[-1] for idxs in by_quarter.values()}
    return august, qe, sweep_days


def _month_end_sessions(sessions: list[str]) -> set[int]:
    last: dict[str, int] = {}
    for i, d in enumerate(sessions):
        last[d[:7]] = i
    return set(last.values())


def _income_schedule(
    inst_rows: list[dict], sessions: list[str]
) -> dict[int, list[tuple[int, str, float]]]:
    """Session index -> [(instrument index, event_type, amount_basis)].

    amount_basis is the PER-PAYMENT rate as percent of par for coupons and the
    per-payment absolute amount per share for dividends -- both
    instrument-intrinsic, both from income.py, which also decides how many
    payments a year the instrument's domicile convention implies.
    """
    schedule: dict[int, list[tuple[int, str, float]]] = {}
    years = range(int(sessions[0][:4]), int(sessions[-1][:4]) + 1)
    for j, inst in enumerate(inst_rows):
        pay_legs = (
            [('COUPON', p) for p in income.coupon_payments(inst)]
            + [('DIVIDEND', p) for p in income.dividend_payments(inst)]
        )
        for etype, (basis, month, day) in pay_legs:
            for year in years:
                due = f'{year}-{month:02d}-{day:02d}'
                if not (sessions[0] <= due <= sessions[-1]):
                    continue
                t = bisect_left(sessions, due)
                if t < len(sessions):
                    schedule.setdefault(t, []).append((j, etype, basis))
    for entries in schedule.values():
        entries.sort()
    return schedule


def _redemption_schedule(
    inst_rows: list[dict], sessions: list[str]
) -> dict[int, list[int]]:
    """Session index -> instrument indices redeeming that day.

    Redemption lands on the last session on or before the FIRDS maturity date,
    so the cash arrives by the maturity rather than after it. Instruments
    maturing outside the window have no entry.
    """
    schedule: dict[int, list[int]] = {}
    for j, inst in enumerate(inst_rows):
        maturity = inst['maturity_dt']
        if maturity >= OPEN_ENDED or not sessions[0] <= maturity <= sessions[-1]:
            continue
        t = bisect_left(sessions, maturity)
        if t == len(sessions) or sessions[t] > maturity:
            t -= 1
        if t >= 0:
            schedule.setdefault(t, []).append(j)
    for entries in schedule.values():
        entries.sort()
    return schedule


def _accrued_interest(inst: dict, quantity: int, day: str) -> float:
    """Interest accrued since the most recent coupon date, ACT/365.

    Bonds quote clean and settle dirty: whoever holds the bond on the coupon
    date collects the whole coupon, so a mid-period buyer compensates the
    seller for the part they earned -- and a seller RECEIVES it, which is the
    symmetry a one-sided implementation once broke. Accrual is annual rate
    times days since the latest payment date over 365, which is exact at every
    payment frequency.
    """
    rate = income.annual_coupon_rate(inst)
    if rate is None:
        return 0.0
    d = date.fromisoformat(day)
    latest = None
    for _, month, day_of_month in income.coupon_payments(inst):
        for year in (d.year - 1, d.year):
            anniversary = date(year, month, min(day_of_month, 28))
            if anniversary <= d and (latest is None or anniversary > latest):
                latest = anniversary
    if latest is None:
        return 0.0
    fraction = (d - latest).days / 365.0
    return round(quantity * rate / 100.0 * fraction, 2)


def _buy_qty(inst, price, budget) -> int:
    """Largest whole dealing size the budget covers, respecting the
    instrument's real minimum denomination from FIRDS."""
    step = max(int(inst.get('nominal_step') or 1), 1)
    if inst['price_convention'] == 'percent_of_par':
        nominal = budget / max(price / 100.0, 1e-9)
        return int(max(0, int(nominal / step) * step))
    units = int(budget / max(price, 1e-9))
    return int(max(0, units // step * step))


def _gross(inst: dict, quantity: int, price: float) -> float:
    if inst['price_convention'] == 'percent_of_par':
        return round(quantity * price / 100.0, 2)
    return round(quantity * price, 2)


def _build_states(
    cfg: config.GenConfig,
    universe: Universe,
    accounts: list[dict],
    sessions: list[str],
    pools: dict,
    profiles: dict[str, str],
    rng: np.random.Generator,
) -> list[_AccountState]:
    session_index = {d: i for i, d in enumerate(sessions)}
    n_sessions = len(sessions)
    states = []
    for acc in accounts:  # accounts arrive sorted by account_id: deterministic
        params = config.MANDATE_PARAMS[acc['mandate_type']]
        target = _target_positions(rng, params['target_mult'])
        migrated = acc['migrated']  # declared on the account, not re-derived
        if migrated:
            # pre-existing book: arrives on-platform in a migration cohort
            start_idx = int(rng.integers(0, config.MIGRATION_SESSIONS + 1))
        else:
            start_idx = session_index.get(acc['opened_date'], 0)
        pool, weights = pools[pool_key(acc, profiles[acc['client_id']])]
        lo, hi = params['core_n']
        n_core = min(int(rng.integers(lo, hi + 1)), pool.size)
        core = rng.choice(pool, size=n_core, replace=False, p=weights)
        dormant = rng.random() < config.DORMANT_SHARE
        closes_at = None
        if rng.random() < config.ATTRITION_SHARE and start_idx + 60 < n_sessions:
            closes_at = int(rng.integers(start_idx + 60, n_sessions))
        state = _AccountState(
            account=acc,
            params=params,
            target=target,
            slice_value=acc['arrival_book_value'] / max(target, 1),
            cash={acc['base_currency']: 0.0},
            start_idx=start_idx,
            migrated=migrated,
            dormant=dormant,
            profile=profiles[acc['client_id']],
            core=np.sort(core),
            closes_at=closes_at,
        )
        if acc['mandate_type'] == 'execution_only' and not dormant:
            lo_b, hi_b = config.BURST_DAYS_PER_YEAR
            years = max(1, round(n_sessions / config.TRADING_DAYS_PER_YEAR))
            n_bursts = int(rng.integers(lo_b, hi_b + 1)) * years
            fund_end = min(start_idx + config.FUNDING_SESSIONS, n_sessions)
            if fund_end < n_sessions:
                days = rng.choice(
                    np.arange(fund_end, n_sessions),
                    size=min(n_bursts, n_sessions - fund_end),
                    replace=False,
                )
                state.burst_days = {int(d) for d in days}
        states.append(state)
    return states


def _expected_trades(state: _AccountState, t: int, august: bool, qe: bool,
                     rng: np.random.Generator) -> float:
    if t < state.start_idx:
        return 0.0
    fund_end = state.start_idx + config.FUNDING_SESSIONS
    building = (
        not state.migrated
        and t < fund_end
        and len(state.holdings) < state.target
    )
    if building:
        # Deployment of new money. Migrated books are excluded -- they arrived
        # WITH their book and a short one simply stays short; the burst rate
        # firing for them was an unlabelled three-week anomaly in every
        # seasonality measure. August and quarter-end apply to funding too (a
        # deployment program lives in the same calendar); dormancy does not,
        # because deploying the opening book is why the account exists at all.
        lam = state.target * config.FUNDING_FILL / config.FUNDING_SESSIONS
        if august:
            lam *= config.AUGUST_FACTOR
        if qe:
            lam *= state.params['qe_boost']
        return lam
    lam = state.params['base_rate']
    if state.dormant:
        lam *= config.DORMANT_FACTOR
    if august:
        lam *= config.AUGUST_FACTOR
    if qe:
        lam *= state.params['qe_boost']
    if t in state.burst_days:
        lam += rng.integers(config.BURST_TRADES[0], config.BURST_TRADES[1] + 1)
    return lam


def _pick_buy(state: _AccountState, pools: dict, alive: np.ndarray,
              rng: np.random.Generator, p_repeat_mult: float = 1.0) -> int | None:
    params = state.params
    repeat_pool = np.array(
        sorted(set(state.holdings) | set(state.core.tolist())), dtype=int
    )
    if repeat_pool.size and rng.random() < params['p_repeat'] * p_repeat_mult:
        repeat_pool = repeat_pool[alive[repeat_pool]]
        if repeat_pool.size:
            w = np.ones(repeat_pool.size)
            core_set = set(state.core.tolist())
            for k, j in enumerate(repeat_pool):
                if int(j) in core_set:
                    w[k] += params['core_w']
            return int(rng.choice(repeat_pool, p=w / w.sum()))
    pool, weights = pools[pool_key(state.account, state.profile)]
    mask = alive[pool]
    pool = pool[mask]
    if pool.size == 0:
        return None
    w = weights[mask]
    return int(rng.choice(pool, p=w / w.sum()))


def generate_trades(
    cfg: config.GenConfig,
    universe: Universe,
    accounts: list[dict],
    clients: list[dict],
    sessions: list[str],
    settle_map: dict[str, str],
    paths: np.ndarray,
    fx: FxRates,
    rng: np.random.Generator,
):
    inst_rows = universe.rows
    n_inst = len(inst_rows)
    popularity = _popularity(rng, n_inst)
    august_flags, qe_flags, sweep_days = _session_flags(sessions)
    month_ends = _month_end_sessions(sessions)
    income_due = _income_schedule(inst_rows, sessions)
    redemption_due = _redemption_schedule(inst_rows, sessions)

    # Buying pools are tilted TWICE: by mandate, which sets how concentrated the
    # flow is, and by the client's risk profile, which sets what asset classes
    # the account is supposed to hold. Without the second tilt the profile is a
    # label on a client row that changes nothing about the book.
    classes = np.array([r['asset_class'] for r in inst_rows])
    # Availability, not headcount, is what a class actually offers. Structured
    # notes are short-dated and issued continuously, so most of those sampled
    # are born mid-window and are quotable for about a third of it. A class
    # buyable a third of the time needs three times the selection weight to
    # reach the same share of a book. Weighting by headcount alone left the
    # stated allocations unreachable -- a target in config that the data does
    # not meet is exactly the defect this project keeps finding.
    n_sessions = len(sessions)
    availability = np.array([
        max(sum(1 for d in sessions
                if r['first_trade_dt'] <= d <= r['last_tradeable_dt']), 1)
        / n_sessions
        for r in inst_rows
    ])
    natural = {
        c: float(availability[classes == c].sum() / availability.sum())
        for c in set(classes)
    }

    # The allocation targets are stated by VALUE, but the tilt steers PICKS.
    # One pick is not one unit of value: a par-quoted note with a 100k minimum
    # denomination books a position several times the standard slice, so a
    # count-level tilt overshoots chunky classes and starves granular ones --
    # measured at 3x off target for the aggressive profile. The correction is
    # each class's typical fill value relative to the standard slice.
    ref_slice = config.OPENING_CASH_MEDIAN / config.POSITIONS_PER_ACCOUNT_MEDIAN
    fill_value = {}
    for cls in set(classes):
        steps = [
            max(int(r.get('nominal_step') or 1), 1)
            for r in inst_rows
            if r['asset_class'] == cls
            and r['price_convention'] == 'percent_of_par'
        ]
        typical_step = float(np.median(steps)) if steps else 0.0
        fill_value[cls] = max(ref_slice, typical_step)

    def profile_tilt(profile: str) -> np.ndarray:
        """Per-instrument multiplier that pulls a draw toward the profile's
        target VALUE allocation: over-weight a class the profile wants more of
        than the universe offers, under-weight one it wants less of, and
        discount classes whose minimum denominations make each pick outsized."""
        targets = config.RISK_PROFILE_ALLOCATION[profile]
        mult = np.ones(n_inst)
        for cls, share in natural.items():
            if share <= 0:
                continue
            value_factor = fill_value[cls] / ref_slice
            mult[classes == cls] = targets.get(cls, 0.01) / (share * value_factor)
        return mult

    pools: dict[str, tuple[np.ndarray, np.ndarray]] = {}
    for mandate, params in sorted(config.MANDATE_PARAMS.items()):
        if mandate == 'discretionary':
            # discretionary accounts draw from their profile's house model
            # (pool_key routes them there); four pools built here were
            # constructed on every run and never read
            continue
        for profile, _ in config.RISK_PROFILES:
            w = np.power(popularity, params['pop_exp']) * profile_tilt(profile)
            pools[f'{mandate}|{profile}'] = (np.arange(n_inst), w / w.sum())

    # One house model per risk profile, drawn from that profile's own tilt --
    # so a balanced model book really is a balanced book, not a random 30 names.
    # Members must be quotable for at least half the window: an investment
    # committee lists established names, and a model dominated by mid-window
    # issues meant only ~10 of 30 members existed on a migration day, so every
    # discretionary book was born at nine positions against a target of 22.
    durable = np.where(availability >= 0.5)[0]
    model_size = min(durable.size, config.HOUSE_MODEL_SIZE)
    disc_exp = config.MANDATE_PARAMS['discretionary']['pop_exp']
    for profile, _ in config.RISK_PROFILES:
        tilted = (popularity * profile_tilt(profile))[durable]
        tilted = tilted / tilted.sum()
        members = np.sort(
            rng.choice(durable, size=model_size, replace=False, p=tilted)
        )
        w = np.power(popularity[members], disc_exp) * profile_tilt(profile)[members]
        pools[house_model_name(profile)] = (members, w / w.sum())

    profiles = {c['client_id']: c['risk_profile'] for c in clients}
    states = _build_states(cfg, universe, accounts, sessions, pools, profiles, rng)

    cols = {
        'trade_id': [], 'trade_date': [], 'settlement_date': [], 'account_id': [],
        'isin': [], 'mic': [], 'side': [], 'quantity': [], 'price': [],
        'gross_consideration': [], 'accrued_interest': [], 'fees': [],
        'currency': [], 'trade_type': [],
    }
    events = {
        'event_id': [], 'event_date': [], 'entitlement_date': [], 'account_id': [],
        'isin': [], 'event_type': [], 'amount': [], 'currency': [],
    }
    transfers = {
        'transfer_id': [], 'transfer_date': [], 'account_id': [], 'isin': [],
        'mic': [], 'direction': [], 'quantity': [], 'market_price': [],
        'market_value': [], 'currency': [],
    }
    fx_trades = {
        'fx_id': [], 'trade_date': [], 'account_id': [], 'sell_currency': [],
        'sell_amount': [], 'buy_currency': [], 'buy_amount': [],
        'reference_rate': [], 'margin_bp': [], 'related_trade_id': [],
    }
    snapshots = {
        'snapshot_date': [], 'account_id': [], 'isin': [], 'quantity': [],
        'currency': [], 'is_month_end': [],
    }
    seq = eseq = tseq = fseq = 0
    # Observability for everything the engine declines to do. Silent shortfall
    # is how a quarter of fee revenue vanished without one failing check.
    stats = {
        'buys_dropped_unaffordable': 0,
        'buys_dropped_min_ticket': 0,
        'buys_dropped_house_cap': 0,
        'sells_skipped_zero_lots': 0,
        'fee_receivable_events': 0,
        'redemptions': 0,
        'closure_liquidations': 0,
    }

    # House-level concentration, tracked as cumulative EUR net investment per
    # ISIN and per issuer. Marked-to-cost rather than to market: cheap, order-
    # deterministic, and honest enough for a cap whose job is stopping one
    # name from becoming a sixth of the whole book.
    house_isin: dict[str, float] = {}
    house_issuer: dict[str, float] = {}
    house_total = 0.0

    def _eur(amount: float, currency: str, day: str) -> float:
        try:
            return fx.convert(amount, currency, 'EUR', day)
        except KeyError:
            return amount

    def house_add(inst, signed_eur: float):
        nonlocal house_total
        house_isin[inst['isin']] = house_isin.get(inst['isin'], 0.0) + signed_eur
        issuer = inst['issuer_lei']
        if issuer:
            house_issuer[issuer] = house_issuer.get(issuer, 0.0) + signed_eur
        house_total += signed_eur

    def house_blocks(inst, add_eur: float) -> bool:
        """Would this buy push the ISIN or its issuer past the house cap?"""
        if house_total + add_eur < config.HOUSE_CAP_MIN_BOOK:
            return False
        cap = config.HOUSE_EXPOSURE_CAP * (house_total + add_eur)
        if house_isin.get(inst['isin'], 0.0) + add_eur > cap:
            return True
        issuer = inst['issuer_lei']
        return bool(
            issuer and house_issuer.get(issuer, 0.0) + add_eur > cap
        )

    def _fee(gross: float, currency: str, day: str) -> float:
        """Tiered brokerage: bp declines with EUR-equivalent ticket size."""
        gross_eur = _eur(gross, currency, day)
        for upper, bp in config.FEE_TIERS:
            if gross_eur <= upper:
                return round(max(config.FEE_MIN, gross * bp / 1e4), 2)
        raise AssertionError('unreachable: FEE_TIERS ends with inf')

    def emit_event(day, entitled, account_id, isin, etype, amount, currency):
        nonlocal eseq
        eseq += 1
        events['event_id'].append(f'CEV{eseq:08d}')
        events['event_date'].append(day)
        events['entitlement_date'].append(entitled)
        events['account_id'].append(account_id)
        events['isin'].append(isin)
        events['event_type'].append(etype)
        events['amount'].append(amount)
        events['currency'].append(currency)

    def emit_transfer(day, state, j, quantity, price, direction='IN'):
        nonlocal tseq
        inst = inst_rows[j]
        value = _gross(inst, quantity, price)
        tseq += 1
        transfers['transfer_id'].append(f'TRF{tseq:08d}')
        transfers['transfer_date'].append(day)
        transfers['account_id'].append(state.account['account_id'])
        transfers['isin'].append(inst['isin'])
        transfers['mic'].append(inst['mic'])
        transfers['direction'].append(direction)
        transfers['quantity'].append(quantity)
        transfers['market_price'].append(price)
        transfers['market_value'].append(value)
        transfers['currency'].append(inst['currency'])
        house_add(inst, _eur(value if direction == 'IN' else -value,
                             inst['currency'], day))

    def credit(state, currency, amount):
        state.cash[currency] = round(state.cash.get(currency, 0.0) + amount, 2)

    def debit(state, currency, amount):
        state.cash[currency] = round(state.cash.get(currency, 0.0) - amount, 2)

    def ensure_currency(state, day, currency, need, for_trade) -> bool:
        """Fund a currency sub-account by converting from the base currency.

        The conversion carries the id of the purchase it funds, because that
        is the relationship: you convert in order to settle a specific trade,
        and a mart should be able to show the pair together.

        Returns False when the base account cannot pay for the conversion --
        the cash constraint still binds, it just binds in the right place.
        """
        nonlocal fseq
        have = state.cash.get(currency, 0.0)
        if have >= need:
            return True
        if currency == state.base:
            return False
        short = (need - have) * config.FX_BUFFER
        try:
            cost = fx.buy_cost(short, state.base, currency, day)
            reference = fx.per_eur(currency, day) / fx.per_eur(state.base, day)
        except KeyError:
            return False
        if cost > state.cash.get(state.base, 0.0):
            return False
        fseq += 1
        fx_trades['fx_id'].append(f'FXT{fseq:08d}')
        fx_trades['trade_date'].append(day)
        fx_trades['account_id'].append(state.account['account_id'])
        fx_trades['sell_currency'].append(state.base)
        fx_trades['sell_amount'].append(round(cost, 2))
        fx_trades['buy_currency'].append(currency)
        fx_trades['buy_amount'].append(round(short, 2))
        fx_trades['reference_rate'].append(round(reference, 8))
        fx_trades['margin_bp'].append(config.FX_MARGIN_BP)
        fx_trades['related_trade_id'].append(for_trade)
        debit(state, state.base, round(cost, 2))
        credit(state, currency, round(short, 2))
        return True

    day_index = {d: t for t, d in enumerate(sessions)}
    pending_pay: dict[int, list[tuple[str, str, float, str, str]]] = {}
    dropped_dividends = 0

    def portfolio_value(state, t) -> float:
        """Holdings marked to market, converted to the account's base currency."""
        total = 0.0
        day = sessions[t]
        for j in sorted(state.holdings):
            if state.holdings[j] <= 0:
                continue
            inst = inst_rows[j]
            value = _gross(inst, state.holdings[j], float(paths[t, j]))
            try:
                total += fx.convert(value, inst['currency'], state.base, day)
            except KeyError:
                total += value
        return total

    for t, day in enumerate(sessions):
        alive = universe.alive_mask(day)

        for state in states:
            acc = state.account
            if state.closes_at is not None and t > state.closes_at:
                continue

            # ---- arrival: the account appears on the platform, dated ----
            if t == state.start_idx:
                initial = acc['arrival_book_value']
                if state.migrated:
                    # a book that predates the window arrives as securities,
                    # free of payment, with a residual cash balance alongside
                    residual = round(initial * config.MIGRATION_CASH_SHARE, 2)
                    emit_event(day, day, acc['account_id'], None, 'DEPOSIT',
                               residual, state.base)
                    credit(state, state.base, residual)
                    book_value = initial - residual
                    slice_value = book_value / max(state.target, 1)
                    pool, weights = pools[pool_key(acc, state.profile)]
                    live = pool[alive[pool]]
                    if live.size:
                        w = weights[alive[pool]]
                        picks = sorted(int(x) for x in np.unique(
                            rng.choice(live, size=min(state.target, live.size),
                                       replace=False, p=w / w.sum())
                        ))
                        # Two passes with a carry, so a pick whose minimum
                        # denomination exceeds its slice hands the money to
                        # the other picks instead of silently shrinking the
                        # book -- one starved arrival is how discretionary
                        # accounts ended up at a third of their target size.
                        # The slice converts into the instrument's currency
                        # exactly as the buy path converts, not at 1:1.
                        carry = 0.0
                        for j in picks:
                            inst = inst_rows[j]
                            price = round(float(paths[t, j]), 4)
                            budget = slice_value + carry
                            try:
                                budget_ccy = fx.convert(
                                    budget, state.base, inst['currency'], day
                                )
                            except KeyError:
                                budget_ccy = budget
                            qty = _buy_qty(inst, price, budget_ccy)
                            if qty <= 0:
                                carry = budget
                                continue
                            value_ccy = _gross(inst, qty, price)
                            spent = budget * (
                                value_ccy / budget_ccy if budget_ccy else 1.0
                            )
                            carry = max(budget - spent, 0.0)
                            emit_transfer(day, state, j, qty, price)
                            state.holdings[j] = state.holdings.get(j, 0) + qty
                        # spend what the chunky picks left on NEW names, in
                        # weight order -- topping up names already bought
                        # would concentrate the very book this pass exists to
                        # diversify
                        unpicked = [
                            int(x) for x in live[np.argsort(-w)]
                            if int(x) not in state.holdings
                        ]
                        for j in unpicked:
                            if carry < slice_value * 0.25:
                                break
                            inst = inst_rows[j]
                            price = round(float(paths[t, j]), 4)
                            try:
                                carry_ccy = fx.convert(
                                    carry, state.base, inst['currency'], day
                                )
                            except KeyError:
                                carry_ccy = carry
                            qty = _buy_qty(inst, price, carry_ccy)
                            if qty <= 0:
                                continue
                            value_ccy = _gross(inst, qty, price)
                            carry = max(
                                carry - carry * (
                                    value_ccy / carry_ccy if carry_ccy else 1.0
                                ),
                                0.0,
                            )
                            emit_transfer(day, state, j, qty, price)
                            state.holdings[j] = state.holdings.get(j, 0) + qty
                else:
                    emit_event(day, day, acc['account_id'], None, 'DEPOSIT',
                               initial, state.base)
                    credit(state, state.base, initial)

            if t < state.start_idx:
                continue

            # ---- dividend payments falling due today ----
            for account_id, isin, amount, currency, ex_day in pending_pay.get(t, ()):
                if account_id != acc['account_id']:
                    continue
                emit_event(day, ex_day, account_id, isin, 'DIVIDEND',
                           amount, currency)
                credit(state, currency, amount)

            # ---- coupons and dividend entitlements ----
            for j, etype, basis in income_due.get(t, ()):
                qty = state.holdings.get(j, 0)
                if qty <= 0:
                    continue
                inst = inst_rows[j]
                if etype == 'COUPON':
                    amount = round(qty * basis / 100.0, 2)
                    if amount <= 0:
                        continue
                    emit_event(day, day, acc['account_id'], inst['isin'],
                               'COUPON', amount, inst['currency'])
                    credit(state, inst['currency'], amount)
                else:
                    amount = round(qty * basis, 2)   # absolute per share
                    if amount <= 0:
                        continue
                    t_pay = t + config.DIVIDEND_PAY_LAG_SESSIONS
                    if t_pay >= len(sessions):
                        dropped_dividends += 1
                        continue
                    pending_pay.setdefault(t_pay, []).append(
                        (acc['account_id'], inst['isin'], amount,
                         inst['currency'], day)
                    )

            # ---- redemptions: bonds mature, principal comes back ----
            # Emitted as a REDEMPTION-typed trade rather than a cash event,
            # because the movement stream must fully explain positions: a cash
            # amount carries no quantity, so an event-only redemption would
            # leave the derived book still holding a matured bond the snapshot
            # no longer shows. A trade row carries both sides of the fact --
            # the quantity leaving and the principal arriving -- at par, no
            # commission (nobody brokers a redemption), accrued to the day.
            for j in redemption_due.get(t, ()):
                qty = state.holdings.get(j, 0)
                if qty <= 0:
                    continue
                inst = inst_rows[j]
                if inst['price_convention'] == 'percent_of_par':
                    price = PAR_REDEMPTION
                else:
                    price = round(float(paths[t, j]), 4)  # final fixing
                gross = _gross(inst, qty, price)
                accrued = (
                    _accrued_interest(inst, qty, day)
                    if inst['asset_class'] == 'bond' else 0.0
                )
                seq += 1
                cols['trade_id'].append(f'TRD{seq:08d}')
                cols['trade_date'].append(day)
                cols['settlement_date'].append(settle_map[day])
                cols['account_id'].append(acc['account_id'])
                cols['isin'].append(inst['isin'])
                cols['mic'].append(inst['mic'])
                cols['side'].append('SELL')
                cols['quantity'].append(qty)
                cols['price'].append(price)
                cols['gross_consideration'].append(gross)
                cols['accrued_interest'].append(accrued)
                cols['fees'].append(0.0)
                cols['currency'].append(inst['currency'])
                cols['trade_type'].append('REDEMPTION')
                credit(state, inst['currency'], gross + accrued)
                house_add(inst, -_eur(gross, inst['currency'], day))
                del state.holdings[j]
                stats['redemptions'] += 1

            # ---- quarter-end fee sweeps, charged in the base currency ----
            if t in sweep_days:
                value = portfolio_value(state, t)
                # Fees are BOOKED IN FULL. The old sweep silently truncated
                # the fee to whatever cash sat in the base sub-account, which
                # cost a quarter of management revenue and produced one-cent
                # fee rows no term sheet could explain. When the account
                # cannot pay, the shortfall becomes an explicit receivable
                # (the bank lends the client the fee), so revenue reconciles
                # and cash still never goes below zero.
                charges = [('CUSTODY_FEE', config.CUSTODY_FEE_BP_ANNUAL, value)]
                if acc['mandate_type'] == 'discretionary':
                    # the all-in fee is charged on the whole relationship,
                    # cash included; custody stays on holdings by design
                    charges.append((
                        'MGMT_FEE', config.MGMT_FEE_BP_ANNUAL,
                        value + max(state.cash.get(state.base, 0.0), 0.0),
                    ))
                for etype, bp, basis in charges:
                    fee = round(basis * bp / 1e4 / 4.0, 2)
                    if fee <= 0:
                        continue
                    payable = min(
                        fee,
                        int(max(state.cash.get(state.base, 0.0), 0.0) * 100)
                        / 100.0,
                    )
                    shortfall = round(fee - payable, 2)
                    # the advance is booked BEFORE the charge it finances --
                    # ledger order is causal order, and a replay that dips
                    # negative mid-day is a replay of the wrong story
                    if shortfall > 0:
                        emit_event(day, day, acc['account_id'], None,
                                   'FEE_RECEIVABLE', shortfall, state.base)
                        stats['fee_receivable_events'] += 1
                        credit(state, state.base, shortfall)
                    emit_event(day, day, acc['account_id'], None, etype,
                               -fee, state.base)
                    debit(state, state.base, fee)
                # the trade slice tracks the book instead of staying frozen at
                # arrival size: a fixed slice on a growing book is why cash
                # only ever piled up (deposits scaled with value, buys did not)
                relationship = value + state.cash.get(state.base, 0.0)
                state.slice_value = max(
                    state.slice_value, relationship / max(state.target, 1)
                )

            # ---- external client flows: this is what Net New Money measures ----
            if not state.dormant and t > state.start_idx:
                per_session = 1.0 / config.TRADING_DAYS_PER_YEAR
                if rng.random() < config.DEPOSIT_RATE_PER_YEAR * per_session:
                    lo, hi = config.DEPOSIT_FRACTION
                    base_value = portfolio_value(state, t) + state.cash.get(
                        state.base, 0.0)
                    amount = round(base_value * rng.uniform(lo, hi), 2)
                    if amount > 0:
                        emit_event(day, day, acc['account_id'], None, 'DEPOSIT',
                                   amount, state.base)
                        credit(state, state.base, amount)
                if rng.random() < config.WITHDRAWAL_RATE_PER_YEAR * per_session:
                    lo, hi = config.WITHDRAWAL_FRACTION
                    free = state.cash.get(state.base, 0.0)
                    amount = round(min(free, free * rng.uniform(lo, hi) * 5), 2)
                    if amount > 0:
                        emit_event(day, day, acc['account_id'], None, 'WITHDRAWAL',
                                   -amount, state.base)
                        debit(state, state.base, amount)

        # ---- trading ----
        for state in states:
            if state.closes_at is not None and t > state.closes_at:
                continue
            lam = _expected_trades(state, t, august_flags[t], qe_flags[t], rng)
            if lam <= 0:
                continue
            n_trades = rng.poisson(lam)
            if n_trades and not state.dormant:
                # Idle cash is meant to be deployed. Above the declared share,
                # the buy probability floors at the deploy rate -- without
                # this, deposits scaled with the book while buys did not, and
                # the cash pile rose monotonically for two years.
                pv = portfolio_value(state, t)
                base_cash = state.cash.get(state.base, 0.0)
                deploy = (
                    pv + base_cash > 0
                    and base_cash / (pv + base_cash) > config.CASH_DEPLOY_SHARE
                )
            else:
                deploy = False
            for _ in range(n_trades):
                held = state.holdings
                # FUNDING now means what it says: an account deploying new
                # money into a book it does not yet have. Migrated books never
                # get the tag, because they never bought their book.
                building = (
                    not state.migrated
                    and len(held) < state.target
                    and t < state.start_idx + config.FUNDING_SESSIONS
                )
                p_buy = (
                    config.FUNDING_BUY_BIAS if building
                    else 0.70 if len(held) < state.target
                    else 0.45
                )
                if deploy:
                    p_buy = max(p_buy, config.CASH_DEPLOY_P_BUY)
                side = 'BUY' if (not held or rng.random() < p_buy) else 'SELL'

                if side == 'BUY':
                    # A buy the account cannot afford is a buy that does not
                    # happen -- it is never rewritten into a sale of something
                    # unrelated. The old flip made a third of all SELL rows
                    # secretly failed purchases and stamped 270 of them
                    # FUNDING, a self-contradiction one GROUP BY exposes.
                    # While BUILDING, an unaffordable pick redraws a few times
                    # before the slot is forfeited: a house model holds
                    # par-quoted names whose minimum ticket can exceed one
                    # slice, and burning the whole slot on each of those left
                    # discretionary books at nine positions against a target
                    # of twenty-two.
                    repeat_mult = config.FUNDING_P_REPEAT_DAMP if building else 1.0
                    sigma = (
                        config.FUNDING_SIZE_SIGMA if building
                        else state.params['size_sigma']
                    )
                    attempts = (
                        config.BUILD_PICK_ATTEMPTS
                        if len(held) < state.target else 1
                    )
                    placed = False
                    for _attempt in range(attempts):
                        j = _pick_buy(state, pools, alive, rng, repeat_mult)
                        if j is None:
                            break
                        inst = inst_rows[j]
                        ccy = inst['currency']
                        base = float(paths[day_index[day], j])
                        noise = rng.normal(
                            0.0, config.TRADE_PRICE_NOISE_BP / 1e4
                        )
                        price = round(base * (1.0 + noise), 4)
                        want = state.slice_value * rng.lognormal(0.0, sigma)
                        try:
                            want_ccy = fx.convert(want, state.base, ccy, day)
                        except KeyError:
                            want_ccy = want
                        headroom = max(
                            config.FEE_MIN, want_ccy * config.FEE_BP / 1e4
                        )
                        budget = min(
                            want_ccy,
                            max(state.cash.get(ccy, 0.0) - headroom, 0.0)
                            if state.cash.get(ccy, 0.0) > headroom
                            else want_ccy,
                        )
                        qty = _buy_qty(inst, price, budget)
                        if qty <= 0:
                            continue
                        placed = True
                        break
                    if not placed:
                        stats['buys_dropped_unaffordable'] += 1
                        continue
                    gross = _gross(inst, qty, price)
                    if _eur(gross, ccy, day) < config.MIN_TRADE_VALUE:
                        stats['buys_dropped_min_ticket'] += 1
                        continue
                    if house_blocks(inst, _eur(gross, ccy, day)):
                        stats['buys_dropped_house_cap'] += 1
                        continue

                if side == 'SELL':
                    held_alive = [j for j in sorted(held) if alive[j] and held[j] > 0]
                    if not held_alive:
                        continue
                    j = held_alive[int(rng.integers(len(held_alive)))]
                    inst = inst_rows[j]
                    ccy = inst['currency']
                    base = float(paths[day_index[day], j])
                    noise = rng.normal(0.0, config.TRADE_PRICE_NOISE_BP / 1e4)
                    price = round(base * (1.0 + noise), 4)
                    if rng.random() < config.SELL_FULL_P:
                        frac = 1.0
                    else:
                        lo_f, hi_f = config.SELL_FRAC_CLAMP
                        frac = min(hi_f, max(lo_f, rng.beta(*config.SELL_BETA)))
                    step = max(int(inst.get('nominal_step') or 1), 1)
                    # floor, never round: rounding a small fraction UP to one
                    # lot -- or promoting zero lots to a full sale -- turns
                    # "trim a little" into "liquidate", which is not what the
                    # draw said. Zero lots means the position stays open.
                    qty = int(held[j] * frac) // step * step
                    qty = min(max(qty, 0), held[j])
                    if frac == 1.0:
                        qty = held[j]
                    if qty == 0:
                        stats['sells_skipped_zero_lots'] += 1
                        continue
                    gross = _gross(inst, qty, price)

                # Bonds settle dirty on BOTH sides: the buyer pays the accrued
                # interest and the seller receives it. Accrual runs to the
                # settlement date, because that is when the money moves.
                accrued = (
                    _accrued_interest(inst, qty, settle_map[day])
                    if inst['asset_class'] == 'bond' else 0.0
                )
                if state.account['mandate_type'] in config.BROKERAGE_MANDATES:
                    fees = _fee(gross, ccy, day)
                else:
                    fees = 0.0   # discretionary pays an all-in fee, not commission
                if side == 'SELL':
                    fees = min(fees, gross + accrued)

                if side == 'BUY':
                    need = gross + accrued + fees
                    next_trade_id = f'TRD{seq + 1:08d}'
                    if not ensure_currency(state, day, ccy, need, next_trade_id):
                        stats['buys_dropped_unaffordable'] += 1
                        continue
                    # ensure_currency returning True is a funding guarantee;
                    # if it ever lies, fail loudly rather than dropping the
                    # trade AFTER its FX leg booked (an orphan conversion)
                    assert state.cash.get(ccy, 0.0) + 1e-6 >= need, (
                        f'funded conversion left {ccy} short on {day}'
                    )

                trade_type = 'FUNDING' if side == 'BUY' and building else 'MARKET'
                seq += 1
                cols['trade_id'].append(f'TRD{seq:08d}')
                cols['trade_date'].append(day)
                cols['settlement_date'].append(settle_map[day])
                cols['account_id'].append(state.account['account_id'])
                cols['isin'].append(inst['isin'])
                cols['mic'].append(inst['mic'])
                cols['side'].append(side)
                cols['quantity'].append(qty)
                cols['price'].append(price)
                cols['gross_consideration'].append(gross)
                cols['accrued_interest'].append(accrued)
                cols['fees'].append(fees)
                cols['currency'].append(ccy)
                cols['trade_type'].append(trade_type)

                if side == 'BUY':
                    debit(state, ccy, gross + accrued + fees)
                    held[j] = held.get(j, 0) + qty
                    house_add(inst, _eur(gross, ccy, day))
                else:
                    credit(state, ccy, gross + accrued - fees)
                    held[j] -= qty
                    if held[j] == 0:
                        del held[j]
                    house_add(inst, -_eur(gross, ccy, day))

        # ---- closures: the relationship ends, the book leaves ----
        # A closed account transfers its securities to the new custodian --
        # direction OUT, at market, the mirror of how migrated books arrived.
        # Positions abandoned in a closed account were rows the reconciliation
        # could never see; now the derived book goes to zero the way a real
        # closure does, and gen_transfer.direction finally has two values.
        for state in states:
            if state.closes_at != t:
                continue
            for j in sorted(state.holdings):
                if state.holdings[j] <= 0:
                    continue
                price = round(float(paths[t, j]), 4)
                emit_transfer(day, state, j, state.holdings[j], price,
                              direction='OUT')
                stats['closure_liquidations'] += 1
            state.holdings.clear()

        # ---- independent month-end snapshot, straight from engine state ----
        if t in month_ends:
            # The window's final session snapshots too, but a partial month is
            # labelled as such: an is_month_end filter that silently includes
            # a mid-month row corrupts every period-over-period figure, and
            # one that silently drops the newest AUM is no better. Days 25+
            # count as genuine month ends (the last session of a month lands
            # there); anything earlier is the window edge.
            genuine = t < len(sessions) - 1 or int(day[8:10]) >= 25
            for state in states:
                if t < state.start_idx:
                    continue
                if state.closes_at is not None and t > state.closes_at:
                    continue
                for j in sorted(state.holdings):
                    if state.holdings[j] <= 0:
                        continue
                    snapshots['snapshot_date'].append(day)
                    snapshots['account_id'].append(state.account['account_id'])
                    snapshots['isin'].append(inst_rows[j]['isin'])
                    snapshots['quantity'].append(state.holdings[j])
                    snapshots['currency'].append(inst_rows[j]['currency'])
                    snapshots['is_month_end'].append(genuine)

    holdings = {s.account['account_id']: dict(s.holdings) for s in states}
    return {
        'trades': cols,
        'events': events,
        'transfers': transfers,
        'fx_trades': fx_trades,
        'snapshots': snapshots,
        'holdings': holdings,
        'dropped_dividends': dropped_dividends,
        'stats': stats,
    }
