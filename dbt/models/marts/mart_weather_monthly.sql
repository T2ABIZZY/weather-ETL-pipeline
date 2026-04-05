SELECT
  stg_weather.city_name,
  EXTRACT(YEAR FROM stg_weather.date_value) AS year,
  EXTRACT(MONTH FROM stg_weather.date_value) AS month,
  round(AVG(stg_weather.temperature_2m_max), 1) AS avg_temperature_2m_max,
  round(AVG(stg_weather.temperature_2m_min), 1) AS avg_temperature_2m_min,
  round(AVG(TIMESTAMP_DIFF(
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M', stg_weather.sunset),
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M', stg_weather.sunrise),
    MINUTE
  )), 0) AS avg_diff_minutes
from 
    {{ref('stg_weather')}}
GROUP BY stg_weather.city_name, year, month