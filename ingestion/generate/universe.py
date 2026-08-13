"""Sample the tradable instrument universe from the shredded FIRDS parquet.

One row per ISIN (lexicographically smallest MIC wins as the trading venue, and
the venue count is kept so multi-listing survives as a fact). Sampling is
weighted to hit the configured asset-class mix and deliberately prefers
derivative instruments that carry basket underlyings, so the underlying bridge
downstream has real rows to stand on.

Loading is columnar: Arrow kernels reduce the 14.7M-row corpus to the ~820k
unique winners, and Python dicts materialise once at the end, winners only.
Semantics are identical to the original row loop and were proven so
(field-identical output, hash-identical full pipeline) before the swap.
"""

from __future__ import annotations

import glob
from dataclasses import dataclass

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from . import config

OPEN_ENDED = '9999-12-31'

NEEDED = [
    'isin', 'trading_venue_mic', 'cfi_code', 'cfi_category', 'notional_ccy',
    'issuer_lei', 'first_trade_dt', 'termination_dt', 'full_name',
    'debt_nominal_per_unit', 'debt_maturity_dt',
]


@dataclass
class Universe:
    rows: list[dict]

    def alive_mask(self, date: str) -> np.ndarray:
        # last_tradeable_dt = min(termination, maturity): a matured bond is as
        # untradeable as a delisted share, and this mask is the single gate
        return np.array(
            [
                r['first_trade_dt'] <= date <= r['last_tradeable_dt']
                for r in self.rows
            ],
            dtype=bool,
        )


def _blank(column: pa.ChunkedArray) -> pa.ChunkedArray:
    """True where null or empty string -- the columnar `not x` / `x or ...`."""
    return pc.fill_null(pc.or_(pc.is_null(column), pc.equal(column, '')), True)


def _default(column: pa.ChunkedArray, default: str) -> pa.ChunkedArray:
    """Columnar `value or default`."""
    return pc.if_else(_blank(column), default, column)


def _nominal_per_unit(raw: pa.ChunkedArray) -> pa.ChunkedArray:
    """FIRDS debt_nominal_per_unit as a float; 0.0 where absent or unparseable.

    This single regulatory field decides two things the generator used to guess:
    whether an instrument is quoted as a percentage of par, and what its minimum
    dealing denomination is.
    """
    return pc.fill_null(
        pc.cast(_default(raw, '0'), pa.float64(), safe=False), 0.0
    )


def _asset_class(cfi: pa.ChunkedArray, nominal: pa.ChunkedArray) -> pa.ChunkedArray:
    """C->fund, ES*->equity, other E->certificate, and D split by denomination.

    D-class covers both real debt and Zertifikate. FIRDS separates them for us:
    debt issued in denominations of 1,000 or 100,000 is a bond; a 'note' issued
    in units of 1 is a structured product wearing a debt CFI code.
    """
    head1 = pc.utf8_slice_codeunits(cfi, 0, 1)
    head2 = pc.utf8_slice_codeunits(cfi, 0, 2)
    null = pa.nulls(len(cfi), pa.string())
    debt = pc.if_else(
        pc.greater_equal(nominal, config.NOMINAL_PER_UNIT_PAR_FLOOR),
        'bond',
        'structured',
    )
    return pc.if_else(
        pc.equal(head1, 'C'),
        'fund',
        pc.if_else(
            pc.equal(head1, 'D'),
            debt,
            pc.if_else(
                pc.equal(head1, 'E'),
                pc.if_else(pc.equal(head2, 'ES'), 'equity', 'certificate'),
                null,
            ),
        ),
    )


def _price_convention(asset_class: pa.ChunkedArray) -> pa.ChunkedArray:
    """config.PRICE_CONVENTION applied columnar, every class read from config
    so the mapping has exactly one home."""
    out = pa.nulls(len(asset_class), pa.string())
    for cls, convention in sorted(config.PRICE_CONVENTION.items()):
        out = pc.if_else(pc.equal(asset_class, cls), convention, out)
    return out


def crypto_underlying(name: str) -> str:
    """The crypto asset an instrument tracks, from its real FIRDS name, or ''.

    A name match is a heuristic, so it is done once here and the result is
    emitted as a mapping table -- a frozen, inspectable artefact beats a regular
    expression buried in a downstream model. Leveraged and inverse wrappers are
    excluded: a daily-reset 2x product is not distributable to EU retail and
    does not belong in a discretionary mandate.
    """
    upper = name.upper()
    if any(token in upper for token in config.CRYPTO_EXCLUDE_TOKENS):
        return ''
    for keyword, symbol in config.CRYPTO_KEYWORDS:
        if keyword in upper:
            return symbol
    return ''


def _nominal_step(nominal: pa.ChunkedArray, convention: pa.ChunkedArray):
    """Minimum dealing size: the real denomination for percent-of-par debt,
    1 unit for everything else."""
    return pc.if_else(
        pc.equal(convention, 'percent_of_par'),
        pc.if_else(
            pc.greater_equal(nominal, config.NOMINAL_PER_UNIT_PAR_FLOOR),
            nominal,
            pa.scalar(float(config.DEFAULT_NOMINAL_STEP)),
        ),
        pa.scalar(1.0),
    )


def load_entity_leis(cfg: config.GenConfig) -> dict[str, list[str]]:
    """country -> LEIs of live legal entities, for the entity clients.

    A corporate client of a private bank HAS an LEI -- it needs one to be the
    counterparty on any reportable trade -- and GLEIF is already ingested, so
    inventing an identifier here would be inventing a fact the platform can
    check. Filtered to entities that are live and registered, sorted, so the
    draw is over a stable list.

    Returns an empty mapping when the corpus is absent: the LEI is an
    enrichment, and the generator must still run on a fixture without it.
    """
    path = cfg.parquet_dir / 'gleif' / 'gleif_entity.parquet'
    if not path.is_file():
        return {}
    table = pq.read_table(
        path, columns=['lei', 'legal_country', 'entity_status', 'registration_status']
    )
    live = pc.and_(
        pc.equal(table['entity_status'], 'ACTIVE'),
        pc.equal(table['registration_status'], 'ISSUED'),
    )
    table = table.filter(live).sort_by([('lei', 'ascending')])
    by_country: dict[str, list[str]] = {}
    for lei, country in zip(
        table['lei'].to_pylist(), table['legal_country'].to_pylist()
    ):
        if country:
            by_country.setdefault(country, []).append(lei)
    return by_country


def load_instruments(cfg: config.GenConfig) -> list[dict]:
    files = sorted(glob.glob(str(cfg.parquet_dir / 'firds_instrument' / '*.parquet')))
    if not files:
        raise FileNotFoundError(f'no firds_instrument parquet under {cfg.parquet_dir}')

    parts = []
    offset = 0
    for path in files:
        table = pq.read_table(path, columns=NEEDED)
        # global row index: reproduces the row loop's first-occurrence-wins
        # tie-break on equal (isin, mic)
        occ = pa.array(np.arange(offset, offset + table.num_rows), pa.int64())
        offset += table.num_rows

        nominal = _nominal_per_unit(table['debt_nominal_per_unit'])
        cls = _asset_class(pc.fill_null(table['cfi_code'], ''), nominal)
        keep = pc.and_(pc.invert(_blank(table['isin'])), cls.is_valid())

        table = table.append_column('asset_class', cls)
        table = table.append_column('nominal_per_unit', nominal)
        table = table.append_column('occ', occ)
        parts.append(table.filter(keep))

    pool = pa.concat_tables(parts)
    if pool.num_rows == 0:
        # header-only parquet (interrupted shred) or every row filtered out:
        # fail with a diagnosis, not a slice error in the dedup below
        raise ValueError(
            f'no usable instrument rows under {cfg.parquet_dir} '
            '(files exist but are empty, or no row has a valid isin + CFI)'
        )
    mic_index = pool.schema.get_field_index('trading_venue_mic')
    pool = pool.set_column(
        mic_index, 'trading_venue_mic', pc.fill_null(pool['trading_venue_mic'], '')
    )

    # distinct venues per isin ('' counts, as in the row loop's set of mics)
    venue_counts = pool.group_by(['isin']).aggregate(
        [('trading_venue_mic', 'count_distinct')]
    ).sort_by([('isin', 'ascending')])

    # winner per isin: sort (isin, mic, occ) and keep each group's first row
    ordered = pool.sort_by(
        [
            ('isin', 'ascending'),
            ('trading_venue_mic', 'ascending'),
            ('occ', 'ascending'),
        ]
    )
    isins = ordered['isin'].combine_chunks()
    n = len(isins)
    changed = pc.not_equal(isins.slice(1, n - 1), isins.slice(0, n - 1))
    first_of_group = np.concatenate(
        ([True], changed.to_numpy(zero_copy_only=False))
    )
    winners = ordered.filter(pa.array(first_of_group))

    # winners and venue_counts are both isin-sorted with one row per isin,
    # so they align positionally; assert rather than pay for a join
    if winners.num_rows != venue_counts.num_rows or not pc.all(
        pc.equal(winners['isin'], venue_counts['isin'])
    ).as_py():
        raise AssertionError('winner/venue-count alignment broken')

    out = pa.table(
        {
            'isin': winners['isin'],
            'mic': winners['trading_venue_mic'],
            'cfi_code': pc.fill_null(winners['cfi_code'], ''),
            'asset_class': winners['asset_class'],
            'currency': _default(winners['notional_ccy'], 'EUR'),
            'issuer_lei': pc.fill_null(winners['issuer_lei'], ''),
            'name': pc.utf8_slice_codeunits(
                pc.fill_null(winners['full_name'], ''), 0, 120
            ),
            'first_trade_dt': pc.utf8_slice_codeunits(
                _default(winners['first_trade_dt'], '1900-01-01'), 0, 10
            ),
            'termination_dt': pc.utf8_slice_codeunits(
                _default(winners['termination_dt'], OPEN_ENDED), 0, 10
            ),
            'n_venues': venue_counts['trading_venue_mic_count_distinct'],
            'price_convention': _price_convention(winners['asset_class']),
            'nominal_per_unit': winners['nominal_per_unit'],
            'nominal_step': _nominal_step(
                winners['nominal_per_unit'],
                _price_convention(winners['asset_class']),
            ),
            # real redemption date from FIRDS; OPEN_ENDED means perpetual or
            # not applicable, and drives pull-to-par in prices.py
            'maturity_dt': pc.utf8_slice_codeunits(
                _default(winners['debt_maturity_dt'], OPEN_ENDED), 0, 10
            ),
        }
    )
    # A bond does not trade past its redemption: after maturity there is
    # nothing left to buy. Liveness previously tested only the venue dates,
    # so nine trades executed on matured bonds and the positions sat on the
    # book for months. One derived column, used by every liveness test, is
    # cheaper than remembering the min() at each of them.
    out = out.append_column(
        'last_tradeable_dt',
        pc.min_element_wise(out['termination_dt'], out['maturity_dt']),
    )
    return out.to_pylist()


def load_underlying_parents(cfg: config.GenConfig) -> set[str]:
    files = sorted(glob.glob(str(cfg.parquet_dir / 'firds_underlying' / '*.parquet')))
    parents: set[str] = set()
    for path in files:
        table = pq.read_table(path, columns=['parent_isin'])
        parents.update(table.column('parent_isin').unique().to_pylist())
    return parents


def sample_universe(cfg: config.GenConfig, rng: np.random.Generator) -> Universe:
    instruments = load_instruments(cfg)
    parents = load_underlying_parents(cfg)

    # keep instruments alive inside the window AND surviving through its end:
    # a mid-window termination would strand any held position (prices stop at
    # termination_dt and the alive mask blocks selling), so the sample requires
    # survival by construction rather than by luck of the corpus.
    # Deliberately NOT extended to maturity_dt: a bond maturing in-window is
    # wanted, because redemption at maturity closes the position with cash --
    # that is the lifecycle, not a stranding.
    pool = [
        r for r in instruments
        if r['first_trade_dt'] <= cfg.end and r['termination_dt'] >= cfg.end
    ]
    if not pool:
        raise ValueError(
            'no usable instruments in parquet_dir '
            '(empty or unreadable firds_instrument, or none survive the window)'
        )
    pool.sort(key=lambda r: r['isin'])  # stable order before any random choice

    by_class: dict[str, list[dict]] = {}
    for row in pool:
        by_class.setdefault(row['asset_class'], []).append(row)

    chosen: list[dict] = []
    for cls, weight in config.UNIVERSE_CLASS_WEIGHTS:
        rows = by_class.get(cls, [])
        want = round(cfg.universe_target * weight)
        if not rows or want == 0:
            continue
        if len(rows) <= want:
            chosen.extend(rows)
            continue
        # prefer basket-carrying instruments so the underlying bridge is exercised
        weights = np.array(
            [3.0 if r['isin'] in parents else 1.0 for r in rows], dtype=float
        )
        weights /= weights.sum()
        idx = rng.choice(len(rows), size=want, replace=False, p=weights)
        chosen.extend(rows[i] for i in sorted(idx))

    issuers = {r['issuer_lei'] for r in chosen if r['issuer_lei']}
    if len(issuers) < config.MIN_ISSUERS and len(chosen) < len(pool):
        # widen with unseen issuers, deterministically
        seen = {r['isin'] for r in chosen}
        for row in pool:
            # blank issuer_lei must not count toward the issuer floor
            if (not row['issuer_lei'] or row['isin'] in seen
                    or row['issuer_lei'] in issuers):
                continue
            chosen.append(row)
            issuers.add(row['issuer_lei'])
            if len(issuers) >= config.MIN_ISSUERS:
                break

    # Reserved crypto sleeve. The streaming layer has to have something in the
    # book that a live crypto price can revalue; leaving that to the draw meant
    # it nearly disappeared on a re-sample. Added deterministically, in isin
    # order, after the class quotas -- so the sleeve is a floor, not a quota.
    seen = {r['isin'] for r in chosen}
    have_crypto = sum(1 for r in chosen if crypto_underlying(r['name']))
    sleeve_target = cfg.crypto_sleeve_target  # scales with --scale, see GenConfig
    if have_crypto < sleeve_target:
        for row in pool:
            if have_crypto >= sleeve_target:
                break
            if row['isin'] in seen or not crypto_underlying(row['name']):
                continue
            chosen.append(row)
            seen.add(row['isin'])
            have_crypto += 1

    chosen.sort(key=lambda r: r['isin'])
    for i, row in enumerate(chosen):
        row['instrument_seq'] = i
        row['has_underlyings'] = row['isin'] in parents
        row['crypto_underlying'] = crypto_underlying(row['name'])
    return Universe(rows=chosen)
