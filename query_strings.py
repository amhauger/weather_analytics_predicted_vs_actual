"""
HELPERS
"""

begin = """
begin;
"""

commit = """
commit;
"""

rollback = """
rollback;
"""

"""
GETTERS
"""
get_most_recent_prediction_for_store_and_datetime = """
select
    prediction_date as date_time,
    car_count as predicted_car_count
from
    public.weather_iq_predictions
where
    prediction_date = '{}'
and
    location_number = '{}'
order by
    created_at desc
limit 1
"""
get_orders_by_store_number = """
select 
    date_trunc('hour', convert_timezone('UTC', dl.timezone_id, o.created_at)) as date_time,
    count(distinct o.order_id) as car_count 
from 
    dw.orders o
inner join 
    dw.dim_locations dl on o.location_id = dl.location_id
where 
    o.created_at >= '{}'::date
and
    o.created_at < getdate()
and
    o.deleted_at is null
and
    o.is_business_hours = 'true'
and
    o.service_item_id is not null
and
    dl.location_number = '{}'
group by 1
order by 1
"""

get_store_numbers_from_prediction_table = """
select 
    distinct(location_number)
from
    public.weather_iq_predictions
"""