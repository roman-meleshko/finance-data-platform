"""Deterministic synthetic book generator for a private-banking platform.

Twelve tables: gen_desk, gen_rm, gen_rm_assignment, gen_client, gen_account,
gen_trade, gen_transfer, gen_fx_trade, gen_price, gen_cash_event,
gen_position_snapshot, gen_calendar. Instruments are sampled from the real
FIRDS corpus and priced in their own currency; conversions use the real ECB
reference rates. One seed yields one combined content hash (manifest.json), and
CI re-proves that on every pull request against a committed micro-fixture.

Balances are not emitted. Positions, cash and market values are derived
downstream from the movement stream, which is what makes the warehouse
reconciliation real work rather than a restatement.

Two guarantees, both replay-proven on the delivered dataset:

- Positions never go negative. Buys are whole multiples of the instrument's
  real FIRDS dealing denomination; sells are clamped to the holding.
- Cash never goes negative FOR ANY (account, currency) PAIR at any step, on a
  trade-date basis. Cash is held in per-currency sub-accounts the way a real
  account holds it, so buying abroad books an FX conversion first rather than
  silently spending a currency the account was never funded in.

gen_position_snapshot is deliberately produced from the engine's own state,
independently of the movement stream, so the warehouse has something real to
reconcile against. A recurrence like closing = opening + movements cannot fail
when both sides are derived from the same rows; a snapshot comparison can, and
the snapshot_break defect proves it does.

Known simplifications, by design and disclosed:

- Prices are a one-factor Gaussian process with a small positive drift set to
  the long-run equity risk premium. No fat tails, no volatility clustering, no
  credit spreads. Percent-of-par instruments pull toward 100 as their real
  FIRDS maturity approaches, but there is no yield curve behind it. Fine for
  market value and exposure; VaR stays out of scope until a fatter model earns
  its place. The realised two-year return remains a draw, not a target.
- Income is one coupon per bond and one dividend per share per year, declared
  as absolute amounts and dated from the instrument's own identifier. Funds are
  accumulation classes and certificates pay nothing. Bond purchases settle with
  accrued interest; there is no ex-dividend tax treatment.
- Fees are a quarterly management fee on discretionary mandates, a quarterly
  custody fee on every account, per-trade brokerage on advisory and
  execution-only mandates only, and a margin on FX conversions. There is no net
  interest income on cash and no Lombard lending.
- External flows are client deposits and withdrawals plus securities transfers
  in at go-live. Their sum is net new money. There are no partial transfers out
  to a competitor and no in-specie distributions.
- Trades are date-grained on a single XFRA pricing calendar, settle T+2, and
  carry no intraday timestamps or bid-ask spread. Flow is independent of
  returns, and model-book rebalances are not synchronised across accounts.
- account.rm_id is the source system's denormalised snapshot at account
  opening; gen_rm_assignment is the ownership event log that supersedes it and
  goes out of step on purpose.
"""
