-- 1. Вывести динамику количества поездок помесячно

select date_month,
       trips_amount,
       trips_amount - COALESCE(LAG(trips_amount) OVER (ORDER BY date_month), trips_amount) trips_changes
from (
    select date_format(CAST(trip_start_timestamp as timestamp), '%Y-%m') date_month,
           count(*) trips_amount
    from chicago_taxi_trips_parquet
    group by 1
);

-- 2. Вывести топ-10 компаний (company) по выручке (trip_total)

select company,
    sum(trip_total) as trip_total
from chicago_taxi_trips_parquet
group by company
order by 2 desc 
limit 10;

-- 3. Подсчитать долю поездок <5, 5-15, 16-25, 26-100 миль

with tm as (
select case when trip_miles < 5 then '1_trip_miles_lt4'
            when trip_miles between 5 and 15 then '2_trip_miles5_15'
            when trip_miles between 16 and 25 then '3_trip_miles16_25'
            when trip_miles between 26 and 100 then '4_trip_miles26_100'
            else 'other' end as trip_miles_category
from chicago_taxi_trips_parquet
)
select trip_miles_category,
count(*) cnt,
count(*)/cast(sum(count(*)) over() as double) as part
from tm
group by trip_miles_category
order by 1;
