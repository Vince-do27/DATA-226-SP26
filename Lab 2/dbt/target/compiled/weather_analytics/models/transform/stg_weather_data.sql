

SELECT
    CAST(date AS date) AS weather_date,
    TRIM(city) AS city,
    CAST(temp_max AS float) AS temp_max,
    CAST(temp_min AS float) AS temp_min,
    CAST(precipitation AS float) AS precipitation_sum,
    CAST(weather_code AS integer) AS weather_code
FROM USER_DB_BOA.RAW.weather_data_lab1