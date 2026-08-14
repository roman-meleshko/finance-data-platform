"""All tunables for the synthetic book in one place.

Portfolio-shape anchoring: SEC Form 13F structured data, Mar-May 2026 window
(9,716 filers) gives the institutional medians -- 94.5 positions per filer,
top-5 share 34.7%, top-10 51.9%, largest position 10.8%. Position counts here
are private-client sized (13F counts are institutional), and concentration is
set deliberately ABOVE the institutional anchor. Measured top-5 value shares
land advisory lowest (broad-pool books), with discretionary and execution-only
more concentrated for different reasons -- model leaders versus temperament.
An earlier comment here claimed discretionary would be the most diversified;
the data refused to confirm it, and the claim now follows the measurement.
No public position-level private-client dataset exists to calibrate against;
the 13F medians anchor the floor.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PARQUET = REPO_ROOT / 'data' / 'parquet'
DEFAULT_OUT = DEFAULT_PARQUET / 'generated'

# --- book shape (scaled by --scale) ---
N_CLIENTS = 120
# Three desks and six RMs, not ten and thirty: a private-banking RM carries a
# book of dozens of relationships and hundreds of millions, so 30 RMs over 120
# clients was an org chart roughly twenty times overstaffed. Fewer RMs also
# thickens every per-RM analytical cell, which scaling the client count cannot
# do (RM count scales with clients, so the ratio is scale-invariant).
N_DESKS = 3
N_RMS = 6
MAX_ACCOUNTS_PER_CLIENT = 3
ACCOUNT_EXTRA_P = 0.35  # chance of each additional account beyond the first

# --- relationship managers ---
# Two titles, not three. Roster position is i // n_desks, so at six RMs over
# three desks only positions 0 and 1 exist and a third title could never be
# reached at any scale (n_desks is capped at N_DESKS). Six people are two
# seniority layers; inventing a third is the same overstaffing mistake the desk
# count was cut to fix.
RM_TITLES = ('senior_director', 'director')  # by roster position
REASSIGNMENT_SHARE = 0.10   # share of clients whose RM changes inside the window
RM_LOCALES = {              # desk region -> name locale for the RM roster
    'Geneva': 'fr', 'Lausanne': 'fr',
    'Zurich': 'de',
}

# --- client risk profile (the thing that actually drives allocation) ---
# mandate_type says WHO decides; risk_profile says WHAT the target allocation
# is. Real suitability frameworks (MiFID II appropriateness) key the model book
# to the profile, not to the mandate.
# Weighted so that every profile is a cell big enough to carry the claim made
# about it. A realistic 8% aggressive share left nine clients, and nine clients
# cannot demonstrate an allocation ordering -- one large account moves it.
RISK_PROFILES = (
    ('conservative', 0.22),
    ('balanced', 0.40),
    ('growth', 0.24),
    ('aggressive', 0.14),
)
# A profile is only meaningful if it changes what the account holds. These are
# target allocations by asset class, and they tilt every buying pool: a
# conservative book is bond- and fund-heavy, an aggressive one equity-heavy.
# The tilt is corrected to VALUE terms: a par-quoted note with a 100k minimum
# denomination books several slices' worth in one pick, so pick-probability
# alone overshot chunky classes threefold. Each class's weight is divided by
# its typical fill value relative to the standard slice (trades.py), and the
# delivered value allocation is measured after generation, not assumed.
RISK_PROFILE_ALLOCATION = {
    'conservative': {
        'bond': 0.42, 'fund': 0.31, 'structured': 0.15, 'equity': 0.10,
        'certificate': 0.02,
    },
    'balanced': {
        'bond': 0.26, 'fund': 0.30, 'structured': 0.11, 'equity': 0.30,
        'certificate': 0.03,
    },
    'growth': {
        'bond': 0.11, 'fund': 0.25, 'structured': 0.09, 'equity': 0.50,
        'certificate': 0.05,
    },
    'aggressive': {
        'bond': 0.03, 'fund': 0.14, 'structured': 0.11, 'equity': 0.62,
        'certificate': 0.10,
    },
}

MANDATE_MIX = (
    ('discretionary', 0.40),
    ('advisory', 0.40),
    ('execution_only', 0.20),
)
# One model book per risk profile, which is how a discretionary mandate works:
# the suitability profile selects the house model and the model is the book.
HOUSE_MODEL_SIZE = 30  # names in a model book. A model that spans a third of
                       # the universe is not a model: at 400 names, portfolios
                       # sharing a model overlapped barely more than portfolios
                       # that did not (Jaccard 0.115 vs 0.099). Real house
                       # models hold 20-40 positions.

# Exactly n_desks entries, and n_desks is capped at N_DESKS: a longer list was
# not a bigger bank, it was seven regions the code could never reach. A
# Geneva/Zurich/Lausanne footprint is what a Swiss private bank of this size
# looks like.
DESK_REGIONS = ('Geneva', 'Zurich', 'Lausanne')

ACCOUNT_BASE_CCY = (('EUR', 0.70), ('CHF', 0.20), ('USD', 0.10))
OPENING_CASH_MEDIAN = 2_000_000.0  # EUR-equivalent, log-normal around this
OPENING_CASH_SIGMA = 0.9

# --- account lifecycle ---
PRE_WINDOW_OPEN_SHARE = 0.70   # share of accounts opened before the data window
PRE_WINDOW_OPEN_DAYS = (30, 1500)  # opened this many calendar days before start
MIGRATION_SESSIONS = 60        # pre-window books arrive on-platform in cohorts
                               # across the first ~3 months, not all on day one
FUNDING_SESSIONS = 15          # sessions an account spends building its book
FUNDING_FILL = 1.25            # funding-phase trade budget = target positions x this
FUNDING_BUY_BIAS = 0.95
FUNDING_P_REPEAT_DAMP = 0.15   # while filling the book, repeat-buying is damped
FUNDING_SIZE_SIGMA = 0.30      # funding buys are near-uniform slices
BUILD_PICK_ATTEMPTS = 4        # redraws before a building account forfeits a
                               # slot to an unaffordable minimum denomination

# --- instrument universe ---
UNIVERSE_TARGET = 1200
UNIVERSE_CLASS_WEIGHTS = (
    ('equity', 0.35),
    ('fund', 0.28),
    ('structured', 0.15),   # Zertifikate: the bulk of what FIRDS files under D
    ('bond', 0.10),         # real debt in nominal denominations
    ('certificate', 0.12),
)
MIN_ISSUERS = 30  # sample must not collapse onto a handful of issuers

# --- portfolio shape ---
POSITIONS_PER_ACCOUNT_MEDIAN = 22
POSITIONS_PER_ACCOUNT_SIGMA = 0.55
POPULARITY_ALPHA = 1.1  # Zipf-ish exponent over the whole universe (flow concentration)

# --- mandate behaviour (activity, repeat-buying, concentration profile) ---
# base_rate    expected trades per session in steady state (before seasonality)
# p_repeat     chance a buy goes into an already-held or core-affinity name
# core_n       how many "core picks" the account is attached to (lo, hi)
# core_w       extra weight on core picks when repeating
# pop_exp      exponent applied to global popularity inside this mandate's pool
#              (<1 flattens = diversifies, >1 sharpens = concentrates)
# size_sigma   log-normal sigma of buy sizes (chunkiness)
# qe_boost     extra quarter-end activity (rebalancing bias)
# target_mult scales the position-count target: execution-only books hold
# fewer, chunkier positions; discretionary follow the fuller house models.
MANDATE_PARAMS = {
    'discretionary': {
        'base_rate': 0.100, 'p_repeat': 0.20, 'core_n': (4, 8), 'core_w': 2.0,
        'pop_exp': 0.5, 'size_sigma': 0.35, 'qe_boost': 2.0, 'target_mult': 1.0,
    },
    'advisory': {
        'base_rate': 0.055, 'p_repeat': 0.18, 'core_n': (3, 6), 'core_w': 1.5,
        'pop_exp': 0.8, 'size_sigma': 0.35, 'qe_boost': 1.3, 'target_mult': 1.0,
    },
    'execution_only': {
        'base_rate': 0.035, 'p_repeat': 0.32, 'core_n': (2, 5), 'core_w': 2.5,
        'pop_exp': 1.1, 'size_sigma': 0.45, 'qe_boost': 1.0, 'target_mult': 0.85,
    },
}
DORMANT_SHARE = 0.14    # funded, then near-inactive -- real books have them
DORMANT_FACTOR = 0.03
BURST_DAYS_PER_YEAR = (4, 9)   # execution-only burst days PER ~YEAR of window
BURST_TRADES = (2, 5)

# --- seasonality ---
# Quarter-end lift comes from the per-mandate qe_boost values above; at the
# 40/40/20 mandate mix they blend to ~1.5x in the final sessions of a quarter.
AUGUST_FACTOR = 0.6
QUARTER_END_SESSIONS = 5       # final sessions of Mar/Jun/Sep/Dec

# --- income events & fee sweeps (gen_cash_event) ---
# Instrument-intrinsic facts derive from the ISIN's sha256, not from the seed:
# the same bond pays the same coupon in every simulated world; only who holds
# it at the record date varies by seed. Funds are accumulation classes and
# certificates pay nothing -- both by design, disclosed.
COUPON_RATE_RANGE = (1.0, 4.5)      # % p.a., stepped in eighths (bond convention)
COUPON_RATE_STEP = 0.125
# Dividends are declared as an ABSOLUTE amount per share, the way a real board
# declares them ("EUR 1.20 per share"); yield is a derived statistic, not an
# input. Deriving the amount from yield x price made a stock that doubled pay
# twice the dividend, which is causality backwards.
DIVIDEND_PER_SHARE_RANGE = (0.15, 3.20)
DIVIDEND_PER_SHARE_STEP = 0.05
DIVIDEND_MONTHS = (4, 6)            # European AGM season: Apr-Jun
DIVIDEND_PAY_LAG_SESSIONS = 10      # ex date -> pay date: entitlement snaps at ex,
                                    # cash arrives 10 sessions later (equities only;
                                    # bonds pay on the coupon date itself)

# --- fees: the private-bank revenue stack ---
# An all-in discretionary mandate does NOT also bill per-trade commission --
# that double charge is the churning conflict MiFID II exists to suppress. So
# discretionary pays a management fee and no brokerage; advisory and
# execution-only pay brokerage and no management fee. Every account pays
# custody, which is the fee real private banks earn regardless of mandate.
MGMT_FEE_BP_ANNUAL = 90.0           # discretionary only, swept quarterly
CUSTODY_FEE_BP_ANNUAL = 25.0        # all mandates, swept quarterly on holdings
BROKERAGE_MANDATES = ('advisory', 'execution_only')

# --- trade mechanics ---
SELL_FULL_P = 0.30              # share of sells that liquidate the position
SELL_BETA = (2.0, 2.0)          # partial-sale fraction ~ Beta(a, b), clamped --
SELL_FRAC_CLAMP = (0.05, 0.95)  # a continuous mix, not three histogram spikes
TRADE_PRICE_NOISE_BP = 10.0    # execution noise around the daily close
# Brokerage declines with ticket size, the way every real schedule does: a
# flat rate at every size made the minimum fee reach 84% of the smallest
# tickets. Bands are (upper bound of EUR-equivalent gross, bp).
FEE_TIERS = ((100_000.0, 25.0), (1_000_000.0, 15.0), (float('inf'), 8.0))
FEE_BP = 15.0                  # mid-tier rate; kept for headroom estimates
FEE_MIN = 25.0
# House-level concentration: reject a buy that would push one ISIN or one
# issuer LEI past this share of everything invested so far. Enforced only
# once the book is big enough for a share to mean anything. 5%, the familiar
# UCITS-style issuer limit, not 3%: sixty-odd discretionary accounts all
# follow the same four 30-name models, so a tighter cap binds on the model
# leaders and starves the model books themselves.
HOUSE_EXPOSURE_CAP = 0.05
HOUSE_CAP_MIN_BOOK = 20_000_000.0
# Deploy idle cash: above this base-currency share of account value, the buy
# probability floors at the deploy rate -- deposits are meant to be invested,
# and without this the cash pile only ever grew.
CASH_DEPLOY_SHARE = 0.15
CASH_DEPLOY_P_BUY = 0.70
SETTLE_LAG_SESSIONS = 2
MIN_TRADE_VALUE = 2000.0       # below this, a buy is not worth booking
CASH_BUFFER = 0.98             # spend at most this share of available cash on one buy

# --- FX: accounts hold per-currency cash, so buying abroad converts first ---
# A real private-banking account holds a cash sub-account per currency. Buying
# a JPY instrument out of a EUR account books an FX conversion leg first, and
# the bank earns a spread on it. Without this the ledger silently spends
# currencies it was never funded in.
FX_MARGIN_BP = 25.0            # bank's spread over the reference rate
FX_BUFFER = 1.02               # convert this multiple of the immediate need

# --- external client flows (what makes Net New Money computable) ---
# NNM is the private-banking industry's headline growth metric. It is the sum
# of external flows -- client money arriving and leaving -- and it is
# definitionally NOT market performance, income or fees.
MIGRATION_CASH_SHARE = 0.06        # a transferred book arrives mostly as
                                   # securities; this much comes as cash
DEPOSIT_RATE_PER_YEAR = 0.55       # expected top-up deposits per account-year
DEPOSIT_FRACTION = (0.04, 0.20)    # each sized as this share of current AUM
WITHDRAWAL_RATE_PER_YEAR = 0.30    # expected withdrawals per account-year
WITHDRAWAL_FRACTION = (0.03, 0.15)
ATTRITION_SHARE = 0.05             # accounts that close inside the window

# --- price simulation ---
MARKET_VOL_ANNUAL = 0.15
# The market factor's cumulative return must land in this band, PER YEAR of
# window, or the path is redrawn (prices.py explains why). +2% to +18%/yr says
# "an unremarkable, mildly positive market" -- wide enough that the draw is
# real, narrow enough that a -1.47-sigma bear market cannot silently make
# every performance KPI negative.
MARKET_CUMRET_BAND = (0.02, 0.18)
MARKET_MAX_REDRAWS = 512
# A feed with perfect coverage tests nothing: per instrument, ~0.5% of session
# rows repeat the previous close (price_source='carried_forward') and ~0.1%
# are genuinely absent. ISIN-hash-derived, so stable across seeds.
PRICE_STALE_PER_MILLE = 5
PRICE_MISSING_PER_MILLE = 1
# A small positive drift, disclosed as the long-run equity risk premium. Zero
# drift made the headline two-year return a coin flip on the seed (this window
# drew roughly -2 sigma), which is a property of the fixture, not of the book.
MARKET_DRIFT_ANNUAL = 0.05
TRADING_DAYS_PER_YEAR = 252
PULL_TO_PAR = True   # percent-of-par instruments converge on 100 as their real
                     # FIRDS maturity date approaches -- the redemption value is
                     # certain, so the price cannot wander freely near the end

# per class: (beta_lo, beta_hi, total_vol_lo, total_vol_hi, start_lo, start_hi)
# 'structured' is split out of 'bond' because the FIRDS data says so: of the
# D-class instruments sampled, the overwhelming majority are Zertifikate --
# discount certificates and medium-term notes issued in units of 1, not debt
# issued in nominal denominations. They are issuer-credit wrappers with a
# payoff, they trade per unit, and a client statement lists them separately.
PRICE_PARAMS = {
    'bond': (0.10, 0.30, 0.03, 0.08, 96.0, 103.0),      # percent of par
    'structured': (0.45, 0.85, 0.10, 0.22, 20.0, 140.0),  # per unit
    'fund': (0.90, 1.00, 0.12, 0.18, 50.0, 500.0),
    'equity': (0.80, 1.20, 0.20, 0.30, 10.0, 400.0),
    'certificate': (1.00, 1.50, 0.25, 0.50, 5.0, 150.0),
}
# Fallback only. The real convention is derived per instrument from FIRDS
# debt_nominal_per_unit: an instrument issued in denominations of 1,000 or
# 100,000 is quoted as a percentage of par; one issued in units of 1 is not.
PRICE_CONVENTION = {
    'bond': 'percent_of_par',
    'structured': 'per_unit',
    'fund': 'per_unit',
    'equity': 'per_unit',
    'certificate': 'per_unit',
}
NOMINAL_PER_UNIT_PAR_FLOOR = 100.0  # >= this -> quoted percent of par
DEFAULT_NOMINAL_STEP = 1000         # when FIRDS gives no usable denomination

# --- the crypto sleeve ---
# The streaming layer revalues the book from a live Coinbase feed, so the book
# has to CONTAIN something a crypto price can revalue. Leaving that to the luck
# of the sample is how it nearly vanished: a re-draw cut the holdings to twelve
# trades. A reserved slice guarantees it survives any seed.
#
# Matching is on the instrument's real FIRDS name, and the mapping it produces
# is emitted as a table rather than left as a regular expression in a model.
CRYPTO_SLEEVE_TARGET = 18
CRYPTO_KEYWORDS = (
    ('BITCOIN', 'BTC'), ('XBT', 'BTC'), ('ETHEREUM', 'ETH'), ('ETHER ', 'ETH'),
    ('SOLANA', 'SOL'), ('CARDANO', 'ADA'), ('POLKADOT', 'DOT'),
    ('LITECOIN', 'LTC'), ('RIPPLE', 'XRP'), ('CHAINLINK', 'LINK'),
    ('AVALANCHE', 'AVAX'), ('POLYGON', 'MATIC'),
)
# Leveraged and inverse wrappers are excluded on purpose: a daily-reset 2x ETF
# is not distributable to EU retail and has no business in a discretionary
# private-banking mandate. Naming the exclusion is part of the defence.
CRYPTO_EXCLUDE_TOKENS = ('2X', '3X', '-1X', 'SHORT', 'INVERSE', 'LEVERAG', 'BEAR')

DEFECT_NAMES = (
    'duplicate_trade_id',
    'orphan_instrument',
    'broken_invariant',
    'missing_price',
    'duplicate_event_id',
    'orphan_cash_event',
    'assignment_before_onboarding',
    'snapshot_break',
    'bad_enum',
    'null_required',
)


@dataclass
class GenConfig:
    seed: int = 42
    scale: float = 1.0
    # two-year window: books migrate in the first quarter, then a long steady
    # state -- keeps funding-phase trades a minority and gives every instrument
    # two income cycles and the RM log a real point-in-time surface
    start: str = '2024-07-18'
    end: str = '2026-07-17'
    calendar: str = 'XFRA'
    parquet_dir: Path = field(default_factory=lambda: DEFAULT_PARQUET)
    out_dir: Path = field(default_factory=lambda: DEFAULT_OUT)
    defects: tuple[str, ...] = ()

    @property
    def n_clients(self) -> int:
        return max(3, round(N_CLIENTS * self.scale))

    @property
    def n_desks(self) -> int:
        return max(2, min(N_DESKS, round(N_DESKS * self.scale) or 2))

    @property
    def n_rms(self) -> int:
        return max(2, round(N_RMS * self.scale))

    @property
    def universe_target(self) -> int:
        return max(40, round(UNIVERSE_TARGET * self.scale))

    @property
    def crypto_sleeve_target(self) -> int:
        """The reserved crypto slice, scaled like every other count.

        As an absolute floor it made --scale non-uniform: at 0.05 the sleeve
        stayed 18 against a universe of 40, so a "smaller bank" was 45% crypto.
        """
        return max(1, round(CRYPTO_SLEEVE_TARGET * self.scale))
