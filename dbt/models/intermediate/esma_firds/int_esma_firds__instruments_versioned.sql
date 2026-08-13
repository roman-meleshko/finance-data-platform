{{ config(
    materialized='table',
    partition_by={'field': 'valid_from', 'data_type': 'date'},
    cluster_by=['isin', 'trading_venue_mic'],
) }}

with events as (

    select
        isin,
        trading_venue_mic,
        'BASE' as version_source,
        date '1960-01-01' as effective_date,
        0 as record_sequence,
        false as is_never_published,
        instrument_full_name,
        instrument_short_name,
        cfi_code,
        cfi_category,
        notional_currency,
        issuer_lei,
        relevant_trading_venue_mic,
        relevant_competent_authority,
        option_type,
        option_exercise_style,
        delivery_type,
        is_commodity_derivative,
        is_issuer_requested,
        total_issued_nominal_amount,
        nominal_value_per_unit,
        price_multiplier,
        debt_maturity_date,
        derivative_expiry_date,
        register_valid_from_date,
        admission_approved_at,
        admission_requested_at,
        first_traded_at,
        terminated_at,
        source_file
    from {{ ref('stg_esma_firds__instruments') }}

    union all

    select
        isin,
        trading_venue_mic,
        record_type as version_source,
        publication_date as effective_date,
        record_sequence,
        is_never_published,
        instrument_full_name,
        instrument_short_name,
        cfi_code,
        cfi_category,
        notional_currency,
        issuer_lei,
        relevant_trading_venue_mic,
        relevant_competent_authority,
        option_type,
        option_exercise_style,
        delivery_type,
        is_commodity_derivative,
        is_issuer_requested,
        total_issued_nominal_amount,
        nominal_value_per_unit,
        price_multiplier,
        debt_maturity_date,
        derivative_expiry_date,
        register_valid_from_date,
        admission_approved_at,
        admission_requested_at,
        first_traded_at,
        terminated_at,
        source_file
    from {{ ref('stg_esma_firds__instrument_deltas') }}
    where publication_date > date '{{ var("firds_base_date") }}'
        or (
            publication_date = date '{{ var("firds_base_date") }}'
            and record_type in ('TERMNTD', 'CANC')
        )
),

cancelled as (
    -- errata are retroactive: a cancelled key has no history at all,
    -- which is why this cannot live inside the window function
    select distinct
        isin,
        trading_venue_mic
    from events
    where version_source = 'CANC'
),

live as (
    select e.*
    from events as e
    left join cancelled as c using (isin, trading_venue_mic)
    where c.isin is null
),

versioned as (
    select
        *,
        row_number() over w as version_no,
        lead(effective_date) over w as next_effective_date
    from live
    window w as (
        partition by isin, trading_venue_mic
        order by effective_date, record_sequence
    )
),

versioned_clean as (

    select
        {{ dbt_utils.generate_surrogate_key(
            ['isin', 'trading_venue_mic', 'effective_date']
        ) }} as instrument_version_sk,
        isin,
        trading_venue_mic,
        version_no,
        version_source,
        effective_date as valid_from,
        coalesce(
            date_sub(next_effective_date, interval 1 day),
            date '9999-12-31'
        ) as valid_to,
        next_effective_date is null as is_current,
        version_source = 'TERMNTD' as is_terminated,
        is_never_published,
        instrument_full_name,
        instrument_short_name,
        cfi_code,
        cfi_category,
        notional_currency,
        issuer_lei,
        relevant_trading_venue_mic,
        relevant_competent_authority,
        option_type,
        option_exercise_style,
        delivery_type,
        is_commodity_derivative,
        is_issuer_requested,
        total_issued_nominal_amount,
        nominal_value_per_unit,
        price_multiplier,
        debt_maturity_date,
        derivative_expiry_date,
        register_valid_from_date,
        admission_approved_at,
        admission_requested_at,
        first_traded_at,
        terminated_at,
        source_file
    from versioned

)

select * from versioned_clean
