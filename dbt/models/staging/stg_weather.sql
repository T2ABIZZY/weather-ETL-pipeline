SELECT DATE(forecast.date) AS date_value,
       forecast.city AS city_name,
       round(forecast.temperature_2m_max, 2) AS temperature_2m_max,
       round(forecast.temperature_2m_min, 2) AS temperature_2m_min,
       FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', TIMESTAMP_SECONDS(forecast.sunrise)) AS sunrise,
       FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', TIMESTAMP_SECONDS(forecast.sunset)) AS sunset
FROM 
    {{source('raw_data', 'forecast')}}