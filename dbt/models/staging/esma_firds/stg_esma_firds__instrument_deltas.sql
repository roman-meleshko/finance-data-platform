with source as (

    select * from {{ source('esma_firds', 'firds_instrument_delta') }}

),

renamed as (

    select

        ---------- ids
        isin,
        trading_venue_mic,
        issuer_lei,
        relevant_trading_venue as relevant_trading_venue_mic,
        relevant_competent_authority,
        record_sequence,

        ---------- strings
        record_type,
        full_name as instrument_full_name,
        short_name as instrument_short_name,
        cfi_code,
        cfi_category,
        notional_ccy as notional_currency,
        option_type,
        option_exercise_style,
        delivery_type,
        source_file,

        ---------- numerics
        safe_cast(debt_total_issued_nominal as numeric) as total_issued_nominal_amount,
        safe_cast(debt_nominal_per_unit as numeric) as nominal_value_per_unit,
        safe_cast(price_multiplier as numeric) as price_multiplier,

        ---------- booleans
        commodity_deriv_indicator = 'true' as is_commodity_derivative,
        issuer_requested = 'true' as is_issuer_requested,
        coalesce(never_published = 'true', false) as is_never_published,

        ---------- dates
        {{ parse_source_date('debt_maturity_dt') }} as debt_maturity_date,
        {{ parse_source_date('derivative_expiry_dt') }} as derivative_expiry_date,
        {{ parse_source_date('valid_from') }} as register_valid_from_date,
        safe_cast(publication_date as date) as publication_date,

        ---------- timestamps
        {{ parse_source_timestamp('admission_approval_dt') }} as admission_approved_at,
        {{ parse_source_timestamp('admission_request_dt') }} as admission_requested_at,
        {{ parse_source_timestamp('first_trade_dt') }} as first_traded_at,
        {{ parse_source_timestamp('termination_dt') }} as terminated_at,
        cast(ingested_at as timestamp) as ingested_at

    from source

)

select * from renamed
