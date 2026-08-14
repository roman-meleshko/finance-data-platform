with trades as (

    select * from {{ ref('stg_generated__trades') }}

),

transfers as (

    select * from {{ ref('stg_generated__transfers') }}

),

unioned as (

    select
        account_id,
        isin,
        trading_venue_mic,
        trade_date as movement_date,
        case
            when trade_side = 'BUY' then quantity
            else -quantity
        end as quantity_delta,
        'TRADE' as movement_type,
        trade_id as movement_id
    from trades

    union all

    select
        account_id,
        isin,
        trading_venue_mic,
        transfer_date as movement_date,
        case
            when transfer_direction = 'IN' then quantity
            else -quantity
        end as quantity_delta,
        'TRANSFER' as movement_type,
        transfer_id as movement_id
    from transfers

)

select * from unioned
