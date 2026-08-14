with calendar as (

    select * from {{ ref('stg_generated__calendar_days') }}

),

enriched as (

    select

        ---------- ids
        calendar_date as date_key,

        ---------- numerics
        extract(year from calendar_date) as calendar_year,
        extract(quarter from calendar_date) as calendar_quarter,
        extract(month from calendar_date) as calendar_month,
        extract(dayofweek from calendar_date) as day_of_week,

        ---------- strings
        format_date('%B', calendar_date) as month_name,
        format_date('%A', calendar_date) as day_name,

        ---------- booleans
        is_trading_day,
        calendar_date = last_day(calendar_date, month) as is_month_end,
        calendar_date = max(
            if(is_trading_day, calendar_date, null)
        ) over (
            partition by date_trunc(calendar_date, month)
        ) as is_last_trading_day_of_month

    from calendar

)

select * from enriched
