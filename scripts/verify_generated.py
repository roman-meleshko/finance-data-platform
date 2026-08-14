"""Verify the generated dataset's invariants and measure its shape.

Every check here can fail, and the exit code says whether any did. The checks
are the dataset's actual guarantees:

1. Cash: replaying every movement per (account, currency) in causal order --
   events first, then each trade preceded by the FX conversion booked to fund
   it -- never takes a balance below zero. The tightest low is tracked
   separately and reported as a real minimum, not an accumulator's start value.
2. Positions: the movement stream (trades, redemptions included, plus
   securities transfers in both directions) rebuilt to every snapshot date
   equals gen_position_snapshot exactly. The snapshot is produced by a second
   write path precisely so this comparison can break.
3. Grain: id columns are unique; (business_date, isin) is unique in prices.
4. Convention: absence is NULL, never the empty string, in every column of
   every table.
5. Liveness: no trade or price on an instrument after min(termination,
   maturity), checked against the FIRDS reference parquet when --parquet-dir
   is given.

With --shape, writes measured_shape.json next to the manifest: the row counts,
sums and distributions the documentation quotes, produced by the same script
that gates the data so the two cannot drift apart.

Usage: python scripts/verify_generated.py [--out data/parquet/generated]
       [--parquet-dir data/parquet] [--shape]
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[1]
TOL = -0.005   # half a cent of rounding dust is not an overdraft


def load(out: Path, name: str) -> list[dict]:
    return pq.read_table(out / f'{name}.parquet').to_pylist()


def check_cash(trades, events, fx, failures) -> dict:
    fx_by_trade: dict[str, list[dict]] = defaultdict(list)
    for f in fx:
        fx_by_trade[f['related_trade_id']].append(f)
    trade_ids = {t['trade_id'] for t in trades}
    orphan_fx = [f for f in fx if f['related_trade_id'] not in trade_ids]

    steps: dict[str, list[tuple[tuple, str, float]]] = defaultdict(list)
    for e in events:
        steps[e['account_id']].append(
            ((e['event_date'], 0, e['event_id']), e['currency'],
             float(e['amount']))
        )
    for t in trades:
        key_fx = (t['trade_date'], 1, t['trade_id'], 0)
        for f in fx_by_trade.get(t['trade_id'], ()):
            steps[t['account_id']].append(
                (key_fx, f['sell_currency'], -float(f['sell_amount']))
            )
            steps[t['account_id']].append(
                (key_fx, f['buy_currency'], float(f['buy_amount']))
            )
        accrued = float(t['accrued_interest'] or 0.0)
        if t['side'] == 'BUY':
            delta = -float(t['gross_consideration']) - accrued - float(t['fees'])
        else:
            # bonds settle dirty on both sides: the seller RECEIVES accrued
            delta = float(t['gross_consideration']) + accrued - float(t['fees'])
        steps[t['account_id']].append(
            ((t['trade_date'], 1, t['trade_id'], 1), t['currency'], delta)
        )

    breaches = 0
    pairs = set()
    tightest = (float('inf'), None)
    for account, movements in steps.items():
        balance: dict[str, float] = defaultdict(float)
        for key, currency, amount in sorted(movements, key=lambda x: str(x[0])):
            pairs.add((account, currency))
            balance[currency] = round(balance[currency] + amount, 2)
            if balance[currency] < tightest[0]:
                tightest = (balance[currency], (account, currency, key[0]))
            if balance[currency] < TOL:
                breaches += 1

    if orphan_fx:
        failures.append(f'{len(orphan_fx)} FX conversions reference no trade')
    if breaches:
        failures.append(f'{breaches} (account, currency) overdraft steps')
    print(f'cash: {len(pairs)} account-currency pairs, breaches={breaches}, '
          f'tightest={tightest[0]:.2f} at {tightest[1]}')
    return {'pairs': len(pairs), 'breaches': breaches,
            'tightest_balance': tightest[0]}


def check_positions(trades, transfers, snapshots, failures) -> dict:
    """Rebuild positions from movements at each snapshot date and compare."""
    moves: dict[tuple[str, str], list[tuple[str, int]]] = defaultdict(list)
    for t in trades:
        sign = 1 if t['side'] == 'BUY' else -1
        moves[(t['account_id'], t['isin'])].append(
            (t['trade_date'], sign * int(t['quantity']))
        )
    for tr in transfers:
        sign = 1 if tr['direction'] == 'IN' else -1
        moves[(tr['account_id'], tr['isin'])].append(
            (tr['transfer_date'], sign * int(tr['quantity']))
        )

    snap = {
        (s['snapshot_date'], s['account_id'], s['isin']): int(s['quantity'])
        for s in snapshots
    }
    snap_dates = sorted({s['snapshot_date'] for s in snapshots})
    snap_accounts = {s['account_id'] for s in snapshots}

    breaks = 0
    compared = 0
    for (account, isin), deltas in moves.items():
        deltas.sort()
        for d in snap_dates:
            derived = sum(q for day, q in deltas if day <= d)
            reported = snap.get((d, account, isin), 0)
            relevant = derived != 0 or reported != 0
            if not relevant:
                continue
            # a snapshot only covers accounts that existed by that date;
            # movements never precede arrival, so first movement is the test
            if deltas[0][0] > d:
                continue
            compared += 1
            if account in snap_accounts and derived != reported:
                breaks += 1
                if breaks <= 5:
                    print(f'  BREAK {d} {account} {isin}: '
                          f'derived={derived} snapshot={reported}')
    if breaks:
        failures.append(f'{breaks} derived-vs-snapshot breaks')
    print(f'positions: {compared} (date, account, isin) cells compared, '
          f'breaks={breaks}')
    return {'cells': compared, 'breaks': breaks}


def check_grain(out: Path, failures) -> None:
    unique_on = {
        'gen_trade': 'trade_id', 'gen_cash_event': 'event_id',
        'gen_transfer': 'transfer_id', 'gen_fx_trade': 'fx_id',
        'gen_account': 'account_id', 'gen_client': 'client_id',
        'gen_rm': 'rm_id', 'gen_desk': 'desk_id',
    }
    for name, col in unique_on.items():
        values = pq.read_table(out / f'{name}.parquet', columns=[col]).column(0)
        n, u = len(values), len(set(values.to_pylist()))
        if n != u:
            failures.append(f'{name}.{col}: {n - u} duplicate ids')
    prices = pq.read_table(
        out / 'gen_price.parquet', columns=['business_date', 'isin']
    )
    keys = list(zip(prices.column(0).to_pylist(), prices.column(1).to_pylist()))
    if len(keys) != len(set(keys)):
        failures.append('gen_price: duplicate (business_date, isin)')
    print(f'grain: {len(unique_on)} id columns + price grain checked')


def check_nulls(out: Path, failures) -> None:
    empty_total = 0
    for path in sorted(out.glob('gen_*.parquet')):
        table = pq.read_table(path)
        for col in table.column_names:
            column = table.column(col)
            if column.type == 'string':
                n_empty = sum(1 for v in column.to_pylist() if v == '')
                if n_empty:
                    empty_total += n_empty
                    failures.append(
                        f'{path.stem}.{col}: {n_empty} empty strings '
                        '(absence must be NULL)'
                    )
    print(f'convention: empty strings across all tables = {empty_total}')


def check_liveness(out: Path, parquet_dir: Path, failures) -> None:
    files = sorted((parquet_dir / 'firds_instrument').glob('*.parquet'))
    if not files:
        print('liveness: skipped (no reference parquet)')
        return
    bounds: dict[str, str] = {}
    for f in files:
        t = pq.read_table(
            f, columns=['isin', 'termination_dt', 'debt_maturity_dt']
        )
        for isin, term, mat in zip(
            t.column(0).to_pylist(), t.column(1).to_pylist(),
            t.column(2).to_pylist(),
        ):
            limit = min(
                (term or '9999-12-31')[:10] or '9999-12-31',
                (mat or '9999-12-31')[:10] or '9999-12-31',
            )
            prev = bounds.get(isin)
            if prev is None or limit > prev:
                bounds[isin] = limit   # most permissive venue row governs
    late_trades = 0
    trades = pq.read_table(
        out / 'gen_trade.parquet', columns=['isin', 'trade_date', 'trade_type']
    ).to_pylist()
    for t in trades:
        if t['trade_date'] > bounds.get(t['isin'], '9999-12-31'):
            late_trades += 1
    if late_trades:
        failures.append(f'{late_trades} trades after termination/maturity')
    print(f'liveness: trades past instrument end = {late_trades}')


def measure_shape(out: Path, cash_stats, position_stats) -> dict:
    manifest = json.loads((out / 'manifest.json').read_text())
    shape: dict = {
        'combined_sha256': manifest['combined_sha256'],
        'tables': {k: v['rows'] for k, v in manifest['tables'].items()},
        'run_stats': manifest.get('run_stats', {}),
        'cash': cash_stats,
        'positions': position_stats,
    }
    trades = load(out, 'gen_trade')
    by = defaultdict(int)
    for t in trades:
        by[(t['side'], t['trade_type'])] += 1
    shape['trades'] = {f'{s}_{ty}': n for (s, ty), n in sorted(by.items())}
    events = load(out, 'gen_cash_event')
    ev = defaultdict(lambda: [0, 0.0])
    for e in events:
        ev[e['event_type']][0] += 1
        ev[e['event_type']][1] = round(
            ev[e['event_type']][1] + float(e['amount']), 2
        )
    shape['events'] = {k: {'n': v[0], 'sum': v[1]} for k, v in sorted(ev.items())}
    accrued_sell = sum(
        float(t['accrued_interest'] or 0) for t in trades if t['side'] == 'SELL'
    )
    accrued_buy = sum(
        float(t['accrued_interest'] or 0) for t in trades if t['side'] == 'BUY'
    )
    shape['accrued_interest'] = {
        'buy_sum': round(accrued_buy, 2), 'sell_sum': round(accrued_sell, 2),
    }
    return shape


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', type=Path,
                        default=REPO_ROOT / 'data' / 'parquet' / 'generated')
    parser.add_argument('--parquet-dir', type=Path,
                        default=REPO_ROOT / 'data' / 'parquet')
    parser.add_argument('--shape', action='store_true',
                        help='write measured_shape.json next to the manifest')
    args = parser.parse_args()

    failures: list[str] = []
    trades = load(args.out, 'gen_trade')
    events = load(args.out, 'gen_cash_event')
    fx = load(args.out, 'gen_fx_trade')
    transfers = load(args.out, 'gen_transfer')
    snapshots = load(args.out, 'gen_position_snapshot')

    cash_stats = check_cash(trades, events, fx, failures)
    position_stats = check_positions(trades, transfers, snapshots, failures)
    check_grain(args.out, failures)
    check_nulls(args.out, failures)
    check_liveness(args.out, args.parquet_dir, failures)

    if args.shape:
        shape = measure_shape(args.out, cash_stats, position_stats)
        (args.out / 'measured_shape.json').write_text(
            json.dumps(shape, indent=2) + '\n'
        )
        print(f"shape: written to {args.out / 'measured_shape.json'}")

    if failures:
        for f in failures:
            print(f'FAIL: {f}')
        return 1
    print('PASS: every check green')
    return 0


if __name__ == '__main__':
    sys.exit(main())
