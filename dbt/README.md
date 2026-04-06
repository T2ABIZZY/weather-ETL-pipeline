# Weather dbt Project

dbt transformation layer for the Weather ETL Pipeline.

## Models

- `staging/stg_weather` — cleans and formats raw weather data from BigQuery
- `marts/mart_weather_monthly` — monthly aggregations per city (avg temperatures, daylight duration)

## Usage
```bash
# Run all models
dbt run

# Run tests
dbt test

# Run specific model
dbt run --select stg_weather
```

## Connection

Connects to BigQuery using a service account. See `profiles.yml` for configuration.