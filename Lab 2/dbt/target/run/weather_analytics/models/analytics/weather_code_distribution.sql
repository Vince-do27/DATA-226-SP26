
  
    

        create or replace transient table USER_DB_BOA.analytics.weather_code_distribution
         as
        (SELECT city, weather_code, count(weather_code) AS total_occurances,
	MD5(
        city || '|' || weather_code
    ) AS snapshot_id
FROM USER_DB_BOA.analytics.stg_weather_data
GROUP BY city, weather_code
ORDER BY weather_code
        );
      
  