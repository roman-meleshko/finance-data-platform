# 1. Partition and cluster the instrument dimension

Date: 2026-08-13

Status: Accepted

## Context

The instrument dimension holds 24.6 million versions across 17 million
instrument and venue keys. Every fact referencing an instrument resolves it as
of a date, so that lookup runs once per position per day and its cost
multiplies by the size of the book rather than being paid once. BigQuery bills
by bytes scanned, and an unorganised table is read in full for each lookup.

Two physical layouts can reduce the read, on different axes. Partitioning
splits the table into separately addressable files by a date column.
Clustering sorts rows within each file so blocks carry usable ranges for the
chosen columns. They can be combined.

## Decision

We will partition by `valid_from` at day granularity, and cluster by `isin`
and `trading_venue_mic` in that order. Clustering order is precedence rather
than a set, and `isin` leads because it is the selective column every fact
join filters on first.

We will floor the base seed's `valid_from` to 1960-01-01. Time unit
partitioning covers 1960-01-01 to 2159-12-31, so an earlier value would place
the 14.7 million base rows, 60% of the table, outside the partition scheme
entirely.

## Consequences

A single as-of lookup scans 489 MB rather than 7.43 GB, a fifteen fold
reduction, measured on 2026-08-13 with cached results disabled. At current
rates a thousand such lookups cost 2.78 USD rather than 42.22.

Clustering earns nearly all of that, and partitioning contributes little here.
The base seed is a single partition holding 60% of the rows, and every lookup
for a date after 1960 must read it, so no date predicate can prune below that
floor. Clustering has no equivalent floor because ISINs are evenly
distributed.

The gain depends on how downstream queries are written, which is the sharpest
consequence. The conventional as-of predicate, `D between valid_from and
valid_to`, leaves the partitioning column as a bound rather than the subject
of a comparison, which the query planner does not recognise; written as two
explicit comparisons the same lookup scans 279 MB. The dimension's schema
documentation carries that instruction, because every mart joining to it is
affected.

Clustering is maintained as rows are written, so incremental rebuilds pay a
reorganisation cost a heap table would not. The floored base date also appears
in the partition list as though it had been published, which is why versions
carry a `version_source` column to disambiguate the seed from real events.
