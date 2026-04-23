SELECT city, weather_code, count(weather_code) AS total_occurances,
	MD5(
        city || '|' || weather_code
    ) AS snapshot_id
FROM {{ ref('stg_weather_data') }}
GROUP BY city, weather_code
ORDER BY weather_code