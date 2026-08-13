# firds_micro

KB-scale slice of the reference data, committed so CI can run the generator
without the real corpus. Curated 2026-08-10 from the FULINS 2026-07-18 shred
and the ECB reference-rate file, with:

```sql
-- per class, isin-ordered, only instruments surviving past 2026-07-17
SELECT isin, trading_venue_mic, cfi_code, cfi_category, notional_ccy,
       issuer_lei, first_trade_dt, termination_dt, full_name,
       debt_nominal_per_unit, debt_maturity_dt
FROM firds_instrument
WHERE substr(cfi_code,1,1) = 'C'  ORDER BY isin, trading_venue_mic LIMIT 120
-- UNION ALL: 'D' LIMIT 120, cfi 'ES%' LIMIT 160, other 'E' LIMIT 80
-- UNION ALL: up to 40 crypto-named instruments (BITCOIN/ETHEREUM/SOLANA,
--            excluding leveraged wrappers) so CI exercises the crypto sleeve
--            and the mapping table -- without them the fixture produced 12
--            tables while production produced 13
-- firds_underlying: rows whose parent_isin is in the instrument slice
-- ecb_fxref:       every rate published 2026-04-01 .. 2026-07-31
```

Extended 2026-08-10 evening with two slices the original curation could not
exercise:

```sql
-- firds_instrument/maturing_bonds.parquet: 12 par-quoted D-class rows whose
-- debt_maturity_dt falls INSIDE the fixture window (2026-05-21 .. 2026-07-15),
-- live at the window start and not terminated before its end. Without them
-- the maturity liveness cut and the redemption event were code CI never ran.
-- Currency restricted so every row has an ECB rate in the slice.
SELECT <same 11 columns>
FROM firds_instrument
WHERE substr(debt_maturity_dt,1,10) BETWEEN '2026-05-01' AND '2026-07-17'
  AND substr(first_trade_dt,1,10) <= '2026-05-01'
  AND (termination_dt IS NULL OR termination_dt = ''
       OR substr(termination_dt,1,10) >= '2026-07-17')
  AND substr(cfi_code,1,1) = 'D'
  AND TRY_CAST(debt_nominal_per_unit AS double) >= 100
  AND notional_ccy IN ('EUR','USD','CHF','GBP')
ORDER BY isin, trading_venue_mic LIMIT 12

-- gleif/gleif_entity.parquet: 15 ACTIVE + ISSUED entities per client
-- domicile (FR/DE/CH/LU), full 21-column schema, lei-ordered. Entity clients
-- draw their LEI from this; without the slice the column is all-null in CI
-- and the feature is untested.
```

Contents: 532 instrument rows (the 520-row base slice plus the 12 maturing
bonds; multi-venue duplicates kept on purpose to exercise the sampler's
occurrence dedup), 60 GLEIF entities, plus 2,465 FX rates over 29 currencies.
A fixture run produces all thirteen tables. The FX rates are not optional:
cash is held per currency, so the generator converts before it can buy
abroad. Consumed by tests/test_determinism.py.
