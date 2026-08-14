# 3. Incremental prices by partition replacement

Date: 2026-08-14

Status: Accepted

## Context

The price feed is the platform's one continuously growing table: 398,640
rows at grain (business_date, isin), one partition per business day. An
incremental model must be idempotent, because replaying a day that already
loaded is a normal operation, not an error, and it must not duplicate or
drift when that happens. dbt offers two strategies on BigQuery: merge, which
matches rows by a unique key, and insert_overwrite, which replaces whole
partitions.

## Decision

We will materialise prices incrementally with insert_overwrite, partitioned
by business_date. dbt computes which partitions an increment touches and
replaces exactly those partitions atomically, so a replayed day overwrites
itself. The model reads its own high-water mark from the built table and
selects only newer rows; --full-refresh remains the escape hatch for a full
rebuild.

## Consequences

Idempotence falls out of the storage layout rather than row-level
bookkeeping: re-running with nothing new processes zero rows. The choice
binds the model's grain to the partition column, which holds here because
the day is both the load unit and the partition; merge remains the right
strategy when grain and partition diverge, at row-matching cost. The same
replace-the-unit argument already governs the raw layer's WRITE_TRUNCATE
loads, so the platform handles replay the same way at both boundaries.
