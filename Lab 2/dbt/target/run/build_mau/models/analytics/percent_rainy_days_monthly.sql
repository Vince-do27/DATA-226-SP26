
  
    

        create or replace transient table USER_DB_BOA.analytics.percent_rainy_days_monthly
         as
        (SELECT
    city,
    date_trunc('month', date) AS month_start,
    COUNT(*) AS days_recorded,
    SUM(CASE
            when coalesce(precipitation, 0) > 0 then 1
            else 0
        end) as rainy_days,
    ROUND(100.0 * SUM(case
                        when coalesce(precipitation, 0) > 0 then 1
                        else 0 end) / NULLIF(COUNT(*), 0),2) AS pct_rainy_days

FROM USER_DB_BOA.raw.weather_data_lab1
WHERE city in ('San Jose', 'Lake Tahoe')
GROUP BY 1, 2
ORDER BY 2, 1
        );
      
  