"""Deliberate, named defects. Each one exists to make a specific test fail.

duplicate_trade_id           -> grain uniqueness on gen_trade (trade_id unique)
orphan_instrument            -> referential integrity to the instrument dimension
broken_invariant             -> an oversell that drives a derived position negative
missing_price                -> a held instrument with no price on one business day
duplicate_event_id           -> grain uniqueness on gen_cash_event (event_id unique)
orphan_cash_event            -> relationships: gen_cash_event.account_id -> gen_account
assignment_before_onboarding -> event-log ordering: gen_rm_assignment.assigned_date
                                must be >= the client's client_since (expression test)
snapshot_break               -> THE reconciliation: derived positions (from the
                                movement stream) vs gen_position_snapshot. This is
                                the only defect that a recurrence test cannot dodge,
                                because the snapshot is produced independently of
                                the movements -- exactly the custodian-statement
                                versus book-of-record break a real operations team
                                investigates every morning.
"""

from __future__ import annotations

from datetime import date, timedelta

from . import config
from .universe import Universe

ORPHAN_ISIN = 'XX0000000000'
ORPHAN_ACCOUNT = 'ACC99999'


def apply_defects(
    names: tuple[str, ...],
    trades: dict[str, list],
    prices: dict[str, list],
    events: dict[str, list],
    snapshots: dict[str, list],
    assignments: list[dict],
    holdings: dict[str, dict[int, int]],
    universe: Universe,
    sessions: list[str],
    settle_map: dict[str, str],
) -> list[str]:
    notes = []
    n = len(trades['trade_id'])
    if n == 0:
        return ['no trades generated; defects skipped']

    if 'duplicate_trade_id' in names:
        i = n // 3
        for values in trades.values():
            values.append(values[i])
        notes.append(f"duplicate_trade_id: duplicated {trades['trade_id'][i]}")

    if 'orphan_instrument' in names:
        i = (2 * n) // 3
        trades['isin'][i] = ORPHAN_ISIN
        trades['mic'][i] = 'XXXX'
        notes.append(
            f"orphan_instrument: {trades['trade_id'][i]} "
            f'now references {ORPHAN_ISIN}'
        )

    if 'broken_invariant' in names:
        # oversell an existing holding so the derived position goes negative
        pick = None
        for acc_id in sorted(holdings):
            held = holdings[acc_id]
            if held:
                j = min(held)
                pick = (acc_id, j, held[j])
                break
        if pick:
            acc_id, j, qty = pick
            inst = universe.rows[j]
            day = sessions[-1]
            # price the oversell from the instrument's OWN most recent price;
            # the previous trades["price"][-1] took whatever trade happened to
            # be booked last, almost always a different instrument entirely.
            # Backwards scan hits the latest row because prices emit in
            # ascending day order.
            price = next(
                (
                    prices['price'][k]
                    for k in range(len(prices['isin']) - 1, -1, -1)
                    if prices['isin'][k] == inst['isin']
                ),
                trades['price'][-1],  # fallback; unreachable on measured data
            )
            oversell = qty * 2
            if inst['price_convention'] == 'percent_of_par':
                gross = round(oversell * price / 100.0, 2)
            else:
                gross = round(oversell * price, 2)
            trades['trade_id'].append(f"TRD{len(trades['trade_id']) + 1:08d}")
            trades['trade_date'].append(day)
            trades['settlement_date'].append(settle_map[day])
            trades['account_id'].append(acc_id)
            trades['isin'].append(inst['isin'])
            trades['mic'].append(inst['mic'])
            trades['side'].append('SELL')
            trades['quantity'].append(oversell)
            trades['price'].append(price)
            trades['gross_consideration'].append(gross)
            trades['accrued_interest'].append(0.0)
            trades['fees'].append(
                round(max(config.FEE_MIN, gross * config.FEE_BP / 1e4), 2)
            )
            trades['currency'].append(inst['currency'])
            trades['trade_type'].append('MARKET')
            notes.append(
                f"broken_invariant: oversell of {inst['isin']} on {day} in {acc_id} "
                f"(held {qty}, sold {oversell})"
            )
        else:
            notes.append('broken_invariant: no held position found; skipped')

    if 'missing_price' in names:
        target_isin = trades['isin'][0]
        day = sessions[len(sessions) // 2]
        keep = [
            k for k in range(len(prices['isin']))
            if not (
                prices['isin'][k] == target_isin
                and prices['business_date'][k] == day
            )
        ]
        removed = len(prices['isin']) - len(keep)
        # a zero-row removal would ship a "defective" dataset that passes the
        # very test this defect exists to fail — refuse instead of exiting 0
        assert removed > 0, (
            f'missing_price removed nothing: {target_isin} has no price on '
            f'{day} — the corpus changed under the defect'
        )
        for key in prices:
            prices[key] = [prices[key][k] for k in keep]
        notes.append(
            f'missing_price: removed {removed} price row(s) '
            f'for {target_isin} on {day}'
        )

    n_ev = len(events['event_id'])

    if 'duplicate_event_id' in names:
        assert n_ev > 0, 'duplicate_event_id: no cash events to duplicate'
        i = n_ev // 3
        for values in events.values():
            values.append(values[i])
        notes.append(f"duplicate_event_id: duplicated {events['event_id'][i]}")

    if 'orphan_cash_event' in names:
        assert n_ev > 0, 'orphan_cash_event: no cash events to orphan'
        i = (2 * n_ev) // 3
        events['account_id'][i] = ORPHAN_ACCOUNT
        notes.append(
            f"orphan_cash_event: {events['event_id'][i]} "
            f'now references {ORPHAN_ACCOUNT}'
        )

    if 'assignment_before_onboarding' in names:
        # the event log's implicit ordering rule: nothing is assigned before
        # the client exists. The list is built as one onboarding row per
        # client followed by the reassignments -- backdate the first
        # reassignment far enough to precede any client_since.
        n_clients = len({a['client_id'] for a in assignments})
        reassigned = assignments[n_clients:]
        if not reassigned:
            # micro scales produce rosters of one and therefore no
            # reassignments; skipping cleanly beats aborting the whole run
            # over a defect that has nothing to attach to
            notes.append('assignment_before_onboarding: SKIPPED, no '
                         'reassignment rows at this scale')
            names = tuple(n for n in names if n != 'assignment_before_onboarding')
    if 'assignment_before_onboarding' in names:
        n_clients = len({a['client_id'] for a in assignments})
        reassigned = assignments[n_clients:]
        target = reassigned[0]
        old = target['assigned_date']
        target['assigned_date'] = (
            date.fromisoformat(sessions[0]) - timedelta(days=3650)
        ).isoformat()
        notes.append(
            f"assignment_before_onboarding: {target['client_id']} assignment "
            f"backdated {old} -> {target['assigned_date']}"
        )

    if 'bad_enum' in names:
        # an enum value from outside the domain -- the row an accepted_values
        # test exists to catch, and until now no defect could produce one
        n_tr = len(trades['trade_id'])
        assert n_tr > 0, 'bad_enum: no trades to corrupt'
        i = n_tr // 4
        old_side = trades['side'][i]
        trades['side'][i] = 'B'
        notes.append(
            f"bad_enum: {trades['trade_id'][i]} side {old_side} -> 'B'"
        )

    if 'null_required' in names:
        # a null where the contract says values only -- the row a not_null
        # test exists to catch; meaningful now that absence is real NULL
        # rather than empty string everywhere
        n_tr = len(trades['trade_id'])
        assert n_tr > 0, 'null_required: no trades to corrupt'
        i = (3 * n_tr) // 4
        trades['currency'][i] = None
        notes.append(
            f"null_required: {trades['trade_id'][i]} currency set to NULL"
        )

    if 'snapshot_break' in names:
        n_snap = len(snapshots['snapshot_date'])
        assert n_snap > 0, 'snapshot_break: no position snapshots to break'
        i = n_snap // 2
        before = snapshots['quantity'][i]
        # a plausible break: the custodian reports a quantity the movement
        # stream does not support. Off by a whole lot, not a rounding hair.
        snapshots['quantity'][i] = int(before * 2 + 1)
        notes.append(
            f"snapshot_break: {snapshots['account_id'][i]} / "
            f"{snapshots['isin'][i]} on {snapshots['snapshot_date'][i]} "
            f"reported {snapshots['quantity'][i]} against {before} derived"
        )

    return notes
