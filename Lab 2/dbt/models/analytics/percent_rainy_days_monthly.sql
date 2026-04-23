{{ config(materialized='table')}}

SELECT
    city,
    date_trunc('month', weather_date) AS month_start,
    COUNT(*) AS days_recorded,
    SUM(CASE
            WHEN COALESCE(precipitation_sum, 0) > 0 THEN 1
            ELSE 0
        END) AS rainy_days,
    ROUND(100.0 * SUM(CASE
                        WHEN COALESCE(precipitation_sum, 0) > 0 THEN 1
                        ELSE 0 END) / NULLIF(COUNT(*), 0),2) AS pct_rainy_days

FROM {{ ref('stg_weather_data')}}
WHERE city in ('San Jose', 'Lake Tahoe')
GROUP BY 1, 2
ORDER BY 2, 1