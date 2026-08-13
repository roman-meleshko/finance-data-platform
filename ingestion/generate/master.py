"""Master data: desks, relationship managers, clients and accounts.

Accounts carry an opened_date: most predate the data window (long-standing
clients), the rest open during it -- so the book has a visible lifecycle
instead of every account springing to life on day one.

RMs form per-desk rosters (round-robin), every client is owned by exactly one
RM, and the client's accounts book on that RM's desk. Ownership history is an
EVENT LOG (gen_rm_assignment: client, rm, assigned_date) -- the warehouse
derives validity ranges from it; the generator never emits ValidFrom/To.
account.rm_id is the source system's denormalized snapshot taken at account
opening: after a mid-window reassignment it goes stale by design, and
resolving trades to the RM-of-record via the log is the downstream
point-in-time exercise.
"""

from __future__ import annotations

from datetime import date, timedelta

import numpy as np
from mimesis import Finance, Person
from mimesis.locales import Locale

from . import config
from .rng import mimesis_seed


def pick_weighted(rng: np.random.Generator, pairs) -> str:
    labels = [k for k, _ in pairs]
    weights = np.array([w for _, w in pairs], dtype=float)
    weights /= weights.sum()
    return labels[rng.choice(len(labels), p=weights)]


def _opened_date(
    rng: np.random.Generator, cfg: config.GenConfig, sessions: list[str]
) -> str:
    if rng.random() < config.PRE_WINDOW_OPEN_SHARE:
        lo, hi = config.PRE_WINDOW_OPEN_DAYS
        days_before = int(rng.integers(lo, hi + 1))
        return (date.fromisoformat(cfg.start) - timedelta(days=days_before)).isoformat()
    return sessions[int(rng.integers(len(sessions)))]


def _build_rms(cfg: config.GenConfig, desks: list[dict]) -> list[dict]:
    """Round-robin rosters: rm i sits on desk i % n_desks; names follow the
    desk region's locale; titles follow roster seniority. No rng draws."""
    person = {
        'fr': Person(locale=Locale.FR, seed=mimesis_seed(cfg.seed, 4)),
        'de': Person(locale=Locale.DE, seed=mimesis_seed(cfg.seed, 5)),
    }
    rms = []
    for i in range(cfg.n_rms):
        desk = desks[i % len(desks)]
        locale = config.RM_LOCALES[desk['region']]
        roster_pos = i // len(desks)
        title = config.RM_TITLES[min(roster_pos, len(config.RM_TITLES) - 1)]
        rms.append(
            {
                'rm_id': f'RM{i + 1:03d}',
                'rm_name': person[locale].full_name(),
                'desk_id': desk['desk_id'],
                'title': title,
            }
        )
    return rms


def _reassignments(
    cfg: config.GenConfig,
    rng_assign: np.random.Generator,
    clients: list[dict],
    initial_rm: dict[str, str],
    rms: list[dict],
    sessions: list[str],
) -> list[dict]:
    """A slice of clients changes RM mid-window -- always within the desk
    (cross-desk moves are out of scope v1). Drawn from the dedicated 'assign'
    stream so master data is untouched by how many draws happen here."""
    rm_desk = {r['rm_id']: r['desk_id'] for r in rms}
    by_desk: dict[str, list[str]] = {}
    for r in rms:
        by_desk.setdefault(r['desk_id'], []).append(r['rm_id'])

    n_re = max(1, round(config.REASSIGNMENT_SHARE * cfg.n_clients))
    idx = rng_assign.choice(cfg.n_clients, size=min(n_re, len(clients)), replace=False)
    out = []
    for ci in sorted(int(i) for i in idx):
        client = clients[ci]
        current = initial_rm[client['client_id']]
        alternatives = [r for r in by_desk[rm_desk[current]] if r != current]
        if not alternatives:
            continue  # micro-scale rosters of one: nothing to reassign to
        candidates = [s for s in sessions if s > client['client_since']]
        if not candidates:
            continue
        out.append(
            {
                'client_id': client['client_id'],
                'rm_id': alternatives[int(rng_assign.integers(len(alternatives)))],
                'assigned_date': candidates[int(rng_assign.integers(len(candidates)))],
            }
        )
    return out


def build_master(
    cfg: config.GenConfig,
    rng: np.random.Generator,
    rng_assign: np.random.Generator,
    sessions: list[str],
    entity_leis: dict[str, list[str]] | None = None,
):
    person_fr = Person(locale=Locale.FR, seed=mimesis_seed(cfg.seed, 1))
    person_de = Person(locale=Locale.DE, seed=mimesis_seed(cfg.seed, 2))
    finance_de = Finance(locale=Locale.DE, seed=mimesis_seed(cfg.seed, 3))

    desks = []
    for i in range(cfg.n_desks):
        desks.append(
            {
                'desk_id': f'DSK{i + 1:03d}',
                'desk_name': (
                    f'{config.DESK_REGIONS[i % len(config.DESK_REGIONS)]} desk'
                ),
                'region': config.DESK_REGIONS[i % len(config.DESK_REGIONS)],
                # roster[0] of desk i is rm index i (round-robin), so the head
                # is guaranteed to be a member of its own desk
                'head_rm_id': f'RM{i + 1:03d}',
            }
        )

    rms = _build_rms(cfg, desks)
    desk_by_id = {d['desk_id']: d for d in desks}

    clients = []
    accounts = []
    initial_rm: dict[str, str] = {}
    account_seq = 0
    for i in range(cfg.n_clients):
        client_id = f'CLI{i + 1:05d}'
        domicile = pick_weighted(
            rng, (('FR', 0.35), ('DE', 0.30), ('CH', 0.25), ('LU', 0.10))
        )
        person = person_fr if domicile in ('FR', 'CH', 'LU') else person_de
        segment = pick_weighted(rng, (('private', 0.75), ('entity', 0.25)))
        name = person.full_name() if segment == 'private' else finance_de.company()
        risk_profile = pick_weighted(rng, config.RISK_PROFILES)
        # Draw unconditionally so the rng stream does not depend on whether the
        # GLEIF corpus is present -- a fixture run and a full run must consume
        # the same draws or the determinism pin becomes corpus-dependent.
        lei_pool = (entity_leis or {}).get(domicile, ())
        lei_pick = int(rng.integers(max(len(lei_pool), 1)))
        lei = lei_pool[lei_pick] if segment == 'entity' and lei_pool else None

        # one RM owns the client relationship; the client books on that RM's desk
        rm = rms[int(rng.integers(cfg.n_rms))]
        desk = desk_by_id[rm['desk_id']]
        initial_rm[client_id] = rm['rm_id']

        client_accounts = []
        n_accounts = 1
        while (
            n_accounts < config.MAX_ACCOUNTS_PER_CLIENT
            and rng.random() < config.ACCOUNT_EXTRA_P
        ):
            n_accounts += 1
        for _ in range(n_accounts):
            account_seq += 1
            mandate = pick_weighted(rng, config.MANDATE_MIX)
            # --scale means "fewer clients", not "poorer clients". Scaling the
            # per-account cash as well made AUM move roughly quadratically, so
            # a 0.05 run was not a small bank but a different one.
            opening_cash = float(
                np.round(
                    rng.lognormal(
                        mean=np.log(config.OPENING_CASH_MEDIAN),
                        sigma=config.OPENING_CASH_SIGMA,
                    ),
                    2,
                )
            )
            opened = _opened_date(rng, cfg, sessions)
            client_accounts.append(
                {
                    'account_id': f'ACC{account_seq:05d}',
                    'client_id': client_id,
                    'desk_id': desk['desk_id'],
                    'rm_id': rm['rm_id'],  # provisional; snapshot fixed below
                    'base_currency': pick_weighted(rng, config.ACCOUNT_BASE_CCY),
                    'mandate_type': mandate,
                    # a discretionary mandate maps the client's suitability
                    # profile to the house model of that profile -- that IS
                    # how the mandate works, so the column is derived rather
                    # than drawn
                    # None, not '': a non-discretionary account HAS no house
                    # model, and an empty string makes not_null pass on absence
                    # while relationships still fails on it.
                    'house_model': (
                        f'MODEL_{risk_profile.upper()}'
                        if mandate == 'discretionary'
                        else None
                    ),
                    'opened_date': opened,
                    # a book that predates the window arrives as a transfer of
                    # securities, not as a deposit -- so this is the value that
                    # ARRIVED, which is why it is no longer called opening_cash
                    'arrival_book_value': opening_cash,
                    # declared, not inferred. Every model that wants the
                    # migrated cohort was otherwise re-deriving
                    # `opened_date < first_session` for itself.
                    'migrated': opened < sessions[0],
                }
            )

        clients.append(
            {
                'client_id': client_id,
                'client_name': name,
                'client_type': segment,
                'domicile_country': domicile,
                # entity clients carry a real GLEIF LEI from their domicile;
                # private clients have none, and NULL says so honestly. This
                # is the join that lets DIM_CLIENT reach DIM_COUNTERPARTY.
                'lei': lei,
                # suitability profile: what the client may hold. mandate_type
                # says who decides; risk_profile says what the target
                # allocation is, and real frameworks key the model to this.
                'risk_profile': risk_profile,
                'client_since': min(a['opened_date'] for a in client_accounts),
            }
        )
        accounts.extend(client_accounts)

    # ownership event log: onboarding row per client + mid-window reassignments
    assignments = [
        {
            'client_id': c['client_id'],
            'rm_id': initial_rm[c['client_id']],
            'assigned_date': c['client_since'],
        }
        for c in clients
    ]
    assignments.extend(
        _reassignments(cfg, rng_assign, clients, initial_rm, rms, sessions)
    )

    # account.rm_id = the RM of record at the account's opening (denormalized
    # snapshot; accounts opened after a reassignment capture the new RM)
    log: dict[str, list[tuple[str, str]]] = {}
    for a in assignments:
        log.setdefault(a['client_id'], []).append((a['assigned_date'], a['rm_id']))
    for entries in log.values():
        entries.sort()
    for acc in accounts:
        history = [e for e in log[acc['client_id']] if e[0] <= acc['opened_date']]
        acc['rm_id'] = history[-1][1]

    return desks, rms, clients, accounts, assignments
