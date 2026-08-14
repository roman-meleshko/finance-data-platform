with position_snapshots as (

    select * from {{ ref('stg_generated__position_snapshots') }}

),

dim_instruments as (

    select
        isin,
        instrument_version_sk,
        valid_from,
        valid_to
    from {{ ref('dim_instruments') }}

),

wide_position_snapshots as (

    select

        ---------- ids
        position_snapshots.account_id,
        position_snapshots.isin,
        instruments.instrument_version_sk,

        ---------- strings
        position_snapshots.position_currency,

        ---------- numerics
        position_snapshots.quantity,

        ---------- booleans
        position_snapshots.is_month_end,

        ---------- dates
        position_snapshots.snapshot_date

    from position_snapshots
    left join dim_instruments as instruments
        on position_snapshots.isin = instruments.isin
            and position_snapshots.snapshot_date >= instruments.valid_from
            and position_snapshots.snapshot_date <= instruments.valid_to

)

select * from wide_position_snapshots
