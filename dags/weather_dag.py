import json
from airflow.decorators import dag, task
import pendulum
import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
from datetime import datetime
from io import BytesIO
from google.cloud import storage, bigquery


@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["weather"],
)
def weather_pipeline():
    @task()
    def fetch_city_coordinates(cities):
        city_coordinates = []
        for city in cities:
            url = "https://geocoding-api.open-meteo.com/v1/search"
            params = {
                "name": city,
            }
            response = requests_cache.CachedSession('.cache').get(url, params = params)
            response.raise_for_status()
            data = response.json()
            if not data.get("results"):
                raise ValueError(f"City '{city}' not found.")
            city_coordinates.append({
                "city": city,
                "lat": float(data["results"][0]["latitude"]),
                "lon": float(data["results"][0]["longitude"]),
            })

        return city_coordinates
        
    @task()
    def fetch_weather_data(data):
        cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
        retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
        openmeteo = openmeteo_requests.Client(session = retry_session)


        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": [item["lat"] for item in data],
            "longitude": [item["lon"] for item in data],
            "daily": ["temperature_2m_max", "temperature_2m_min", "sunrise", "sunset"],
            "timezone": "Europe/Berlin",
            "forecast_days": 14,
        }
        responses = openmeteo.weather_api(url, params = params)

        all_cities = []
        for i, response in enumerate(responses):
            city_name = data[i]["city"]
            print(f"\nCoordinates: {response.Latitude()}°N {response.Longitude()}°E")
            print(f"Elevation: {response.Elevation()} m asl")
            print(f"Timezone: {response.Timezone()}{response.TimezoneAbbreviation()}")
            print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")
            
            # Process daily data. The order of variables needs to be the same as requested.
            daily = response.Daily()
            daily_temperature_2m_max = daily.Variables(0).ValuesAsNumpy()
            daily_temperature_2m_min = daily.Variables(1).ValuesAsNumpy()
            daily_sunrise = daily.Variables(2).ValuesInt64AsNumpy()
            daily_sunset = daily.Variables(3).ValuesInt64AsNumpy()
            
            daily_data = {"date": pd.date_range(
                start = pd.to_datetime(daily.Time() + response.UtcOffsetSeconds(), unit = "s", utc = True),
                end =  pd.to_datetime(daily.TimeEnd() + response.UtcOffsetSeconds(), unit = "s", utc = True),
                freq = pd.Timedelta(seconds = daily.Interval()),
                inclusive = "left"
            )}
            
            daily_data["temperature_2m_max"] = daily_temperature_2m_max
            daily_data["temperature_2m_min"] = daily_temperature_2m_min
            daily_data["sunrise"] = daily_sunrise
            daily_data["sunset"] = daily_sunset
            daily_data["city"] = city_name
            
            daily_dataframe = pd.DataFrame(data = daily_data)
            city_records = json.loads(
                daily_dataframe.to_json(orient="records", date_format="iso")
            )
            all_cities.append({
                "city": city_name,
                "data": city_records,
            })

        return all_cities

    @task()
    def upload_to_gcs(all_cities):
        client = storage.Client()
        bucket = client.bucket("etl-weather-data")
        today = datetime.today().strftime('%Y-%m-%d')
        bolb_paths = []
        for city in all_cities:
            city_name = city["city"]
            city_data = city["data"]
            df_city = pd.DataFrame(city_data)
            buffer = BytesIO()
            df_city.to_parquet(buffer, index=False)
            buffer.seek(0)
            blob_path = f"weather/{today}/{city_name}.parquet"
            blob = bucket.blob(blob_path)
            blob.upload_from_file(buffer, content_type="application/octet-stream")
            print(f"Uploaded {blob_path}")
            bolb_paths.append(blob_path)
        return bolb_paths

    @task()
    def load_to_bigquery(bolb_paths):
        print("load_to_bigquery...")

    cities = ["Paris", "Lyon", "Marseille", "Toulouse", "Bordeaux"]
    fetch = fetch_city_coordinates(cities)
    weather = fetch_weather_data(fetch)
    upload = upload_to_gcs(weather)
    load = load_to_bigquery(upload)

    fetch >> weather >> upload >> load

weather_pipeline()