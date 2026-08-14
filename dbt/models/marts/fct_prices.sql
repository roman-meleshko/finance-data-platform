{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'business_date', 'data_type': 'date'},
) }}

with prices as (

    select * from {{ ref('stg_generated__prices') }}

),

windowed as (

    select

        ---------- ids
        isin,

        ---------- strings
        price_currency,
        price_source,
        price_convention,

        ---------- numerics
        price,

        ---------- dates
        prices.business_date

    from prices
    where prices.business_date <= date '{{ var("price_through", "9999-12-31") }}'
        {% if is_incremental() %}
            and prices.business_date > (
                -- bare on purpose: the subquery scopes it to {{ this }},
                -- and BigQuery rejects a path-qualified column here
                select coalesce(max(business_date), date '1900-01-01')  -- noqa: RF02
                from {{ this }}
            )
        {% endif %}

)

select * from windowed
