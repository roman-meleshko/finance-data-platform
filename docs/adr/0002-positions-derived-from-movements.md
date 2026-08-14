# 2. Positions derived from movements

Date: 2026-08-14

Status: Accepted

## Context

A custody book can source positions two ways: load the custodian's statement
as truth, or derive holdings from the movement stream and treat the statement
as a check. The platform ingests both inputs. Trades and transfers arrive as
signed events, and an independently produced month-end snapshot arrives with
73,201 position rows. A derivation that is also the loading path cannot be
verified, because closing = opening + movements computes both sides from the
same rows; that identity survived a deliberately injected oversell during
generator development, which is what exposed it as untestable.

## Decision

We will derive daily positions from a single signed movement relation and
never load positions in parallel. All movement species union into one seam
model with one schema, and every position model reads exactly that relation.
The published snapshot stays untouched as the reconciliation counterparty:
derived quantities are compared against it at every month-end cell, in both
directions, with absence counting as zero.

## Consequences

The reconciliation is the one position test that can fail, because its two
sides come from independent paths; it currently passes with zero breaks
across all 73,201 statement cells. A new movement species, such as a
corporate action, becomes one additional union branch in the seam and no
downstream model changes. Daily profit and loss and the cash ledger are
deferred until both can share one signed-flow valuation, since building that
valuation twice is how books stop reconciling. The derivation pays a spine
of roughly 1.6 million position-day rows to make positions on quiet days
queryable; that cost is deliberate.
