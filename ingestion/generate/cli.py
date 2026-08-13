"""Entry point: python -m ingestion.generate.cli --seed 42

Produces thirteen Parquet tables -- master data, the movement stream, prices,
an independent position snapshot and a crypto mapping --
plus a manifest with per-table content hashes. Same seed, same window, same
inputs -> same combined hash.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import exchange_calendars as xcals
import pandas as pd

from . import config, defects, fx, master, prices, trades, universe
from .rng import make_streams


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument(
        '--scale',
        type=float,
        default=1.0,
        help='Fewer clients, not poorer ones. Scales client, RM, universe and '
        'crypto-sleeve counts; per-account size is NOT scaled, so a 0.05 run '
        'is a smaller bank rather than a different one (default: 1.0).',
    )
    parser.add_argument('--start', default='2024-07-18')
    parser.add_argument('--end', default='2026-07-17')
    parser.add_argument('--calendar', default='XFRA')
    parser.add_argument('--out', type=Path, default=config.DEFAULT_OUT)
    parser.add_argument('--parquet-dir', type=Path, default=config.DEFAULT_PARQUET)
    parser.add_argument(
        '--defects',
        default='',
        help=f"comma-separated subset of: {', '.join(config.DEFECT_NAMES)}",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    defect_names = tuple(d for d in args.defects.split(',') if d)
    unknown = set(defect_names) - set(config.DEFECT_NAMES)
    if unknown:
        print(f'unknown defects: {sorted(unknown)}', file=sys.stderr)
        return 2
    # resolve() both sides: `--out data/generated` is the natural spelling and
    # compares unequal to the absolute DEFAULT_OUT, so the guard let the one
    # thing it exists to prevent walk straight through it -- a defect-injected
    # run silently replacing the canonical dataset, detectable only as a
    # mysteriously failing determinism pin.
    if defect_names and args.out.resolve() == config.DEFAULT_OUT.resolve():
        # same discipline as the shredder guard: a flagged run must never
        # overwrite the canonical clean dataset by default
        print(
            'defect runs must name --out explicitly '
            f'({config.DEFAULT_OUT} is the canonical clean set)',
            file=sys.stderr,
        )
        return 2
    # normalize dates once: unpadded forms ('2026-7-17') pass pandas but
    # string-compare wrong in every downstream lexicographic window check
    try:
        args.start = pd.Timestamp(args.start).date().isoformat()
        args.end = pd.Timestamp(args.end).date().isoformat()
    except ValueError:
        print(
            f'unparseable --start/--end: {args.start!r} {args.end!r}',
            file=sys.stderr,
        )
        return 2

    cfg = config.GenConfig(
        seed=args.seed,
        scale=args.scale,
        start=args.start,
        end=args.end,
        calendar=args.calendar,
        parquet_dir=args.parquet_dir,
        out_dir=args.out,
        defects=defect_names,
    )

    t0 = time.time()
    cal = xcals.get_calendar(cfg.calendar)
    sessions = [d.date().isoformat() for d in cal.sessions_in_range(cfg.start, cfg.end)]
    if not sessions:
        print('no trading sessions in window', file=sys.stderr)
        return 2
    # settlement needs sessions beyond the window end; clamp the horizon to the
    # calendar's coverage and refuse to book silent T+0 settlements at the tail
    horizon_ts = min(pd.Timestamp(cfg.end) + pd.Timedelta(days=10), cal.last_session)
    horizon = horizon_ts.date().isoformat()
    extended = [d.date().isoformat() for d in cal.sessions_in_range(cfg.start, horizon)]
    if len(extended) < len(sessions) + config.SETTLE_LAG_SESSIONS:
        print(
            f'calendar {cfg.calendar} ends too close to --end: cannot settle '
            f'T+{config.SETTLE_LAG_SESSIONS} beyond {sessions[-1]} '
            f'(calendar coverage stops {cal.last_session.date().isoformat()})',
            file=sys.stderr,
        )
        return 2
    settle_map = {
        day: extended[min(i + config.SETTLE_LAG_SESSIONS, len(extended) - 1)]
        for i, day in enumerate(sessions)
    }

    streams = make_streams(cfg.seed)
    uni = universe.sample_universe(cfg, streams['universe'])
    desks, rms, clients, accounts, assignments = master.build_master(
        cfg, streams['master'], streams['assign'], sessions,
        entity_leis=universe.load_entity_leis(cfg),
    )
    rates = fx.FxRates.load(cfg)
    price_rows, paths = prices.simulate_prices(
        cfg, uni, sessions, rates, streams['market'], streams['idio']
    )
    book = trades.generate_trades(
        cfg, uni, accounts, clients, sessions, settle_map, paths, rates,
        streams['trades']
    )
    trade_rows = book['trades']
    event_rows = book['events']
    transfer_rows = book['transfers']
    fx_rows = book['fx_trades']
    snapshot_rows = book['snapshots']
    holdings = book['holdings']
    dropped_dividends = book['dropped_dividends']

    notes = []
    if defect_names:
        notes = defects.apply_defects(
            defect_names, trade_rows, price_rows, event_rows, snapshot_rows,
            assignments, holdings, uni, sessions, settle_map,
        )

    session_set = set(sessions)
    spine = pd.date_range(cfg.start, cfg.end, freq='D')
    calendar_rows = {
        'calendar_date': [d.date().isoformat() for d in spine],
        'is_trading_day': [d.date().isoformat() in session_set for d in spine],
    }

    crypto_rows = [
        {'isin': r['isin'], 'underlying_symbol': r['crypto_underlying'],
         'instrument_name': r['name'], 'asset_class': r['asset_class'],
         'match_basis': 'firds_full_name'}
        for r in uni.rows if r['crypto_underlying']
    ]

    tables = {
        'gen_desk': {k: [d[k] for d in desks] for k in desks[0]},
        'gen_rm': {k: [r[k] for r in rms] for k in rms[0]},
        'gen_rm_assignment': {k: [a[k] for a in assignments] for k in assignments[0]},
        'gen_client': {k: [c[k] for c in clients] for k in clients[0]},
        'gen_account': {k: [a[k] for a in accounts] for k in accounts[0]},
        'gen_trade': trade_rows,
        'gen_transfer': transfer_rows,
        'gen_fx_trade': fx_rows,
        'gen_price': price_rows,
        'gen_cash_event': event_rows,
        'gen_position_snapshot': snapshot_rows,
        'gen_crypto_mapping': {
            k: [r[k] for r in crypto_rows] for k in crypto_rows[0]
        } if crypto_rows else None,
        'gen_calendar': calendar_rows,
    }
    tables = {k: v for k, v in tables.items() if v is not None}
    manifest = write_tables(cfg.out_dir, tables, run_stats=book['stats'])

    print(f"sessions={len(sessions)} universe={len(uni.rows)} "
          f"accounts={len(accounts)} rm={len(rms)} "
          f"assignments={len(assignments)} "
          f"trades={len(trade_rows['trade_id'])} "
          f"events={len(event_rows['event_id'])} "
          f"dropped_dividends={dropped_dividends} "
          f"crypto={len(crypto_rows)} "
          f"prices={len(price_rows['isin'])} elapsed={time.time() - t0:.1f}s")
    print('run_stats ' + ' '.join(f'{k}={v}' for k, v in sorted(book['stats'].items())))
    for note in notes:
        print(f'defect: {note}')
    print(f"combined_sha256={manifest['combined_sha256']}")
    return 0


def write_tables(out_dir: Path, tables: dict, run_stats: dict | None = None) -> dict:
    from .writer import write_tables as _write

    return _write(out_dir, tables, run_stats=run_stats)


if __name__ == '__main__':
    sys.exit(main())
