with trades as (

    select
        isin,
        trading_venue_mic
    from {{ ref('stg_generated__trades') }}

),

transfers as (

    select
        isin,
        trading_venue_mic
    from {{ ref('stg_generated__transfers') }}

),

instruments_versioned as (

    select * from {{ ref('int_esma_firds__instruments_versioned') }}

),

entities as (

    select
        lei,
        legal_name,
        legal_country
    from {{ ref('stg_gleif__entities') }}

),

venues as (

    select
        mic,
        market_name,
        country_code,
        city
    from {{ ref('stg_iso_mic__venues') }}

),

crypto_mappings as (

    select
        isin,
        underlying_symbol
    from {{ ref('stg_generated__crypto_mappings') }}

),

book_keys as (

    select
        isin,
        trading_venue_mic
    from trades

    union distinct

    select
        isin,
        trading_venue_mic
    from transfers

),

versions as (

    select v.*
    from instruments_versioned as v
    inner join book_keys as b
        on v.isin = b.isin
            and v.trading_venue_mic = b.trading_venue_mic

),

enriched as (

    select

        ---------- version identity
        v.instrument_version_sk,
        v.isin,
        v.trading_venue_mic,
        v.version_no,
        v.version_source,
        v.valid_from,
        v.valid_to,
        v.is_current,
        v.is_terminated,

        ---------- instrument
        v.instrument_full_name,
        v.instrument_short_name,
        v.cfi_code,
        v.cfi_category,
        case v.cfi_category
            when 'C' then 'collective_investment'
            when 'D' then 'debt'
            when 'E' then 'equity'
        end as asset_class,
        v.notional_currency,
        v.option_type,
        v.option_exercise_style,
        v.delivery_type,
        v.is_commodity_derivative,
        v.total_issued_nominal_amount,
        v.nominal_value_per_unit,
        v.price_multiplier,
        v.debt_maturity_date,
        v.derivative_expiry_date,
        v.first_traded_at,
        v.terminated_at,

        ---------- issuer
        v.issuer_lei,
        g.legal_name as issuer_name,
        g.legal_country as issuer_country,

        ---------- venue
        m.market_name as venue_name,
        m.country_code as venue_country,
        m.city as venue_city,

        ---------- crypto link
        c.underlying_symbol is not null as is_crypto_linked,
        c.underlying_symbol as crypto_underlying_symbol

    from versions as v
    left join entities as g
        on v.issuer_lei = g.lei
    left join venues as m
        on v.trading_venue_mic = m.mic
    left join crypto_mappings as c
        on v.isin = c.isin

)

select * from enriched
