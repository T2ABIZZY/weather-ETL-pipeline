from airflow.decorators import dag, task
import pendulum

@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["weather"],
)
def weather_pipeline():
    @task()
    def fetch_weather():
        print("fetching weather...")

    @task()
    def upload_to_gcs():
        print("upload_to_gcs...")

    @task()
    def load_to_bigquery():
        print("load_to_bigquery...")

    fetch = fetch_weather()
    upload = upload_to_gcs()
    load = load_to_bigquery()

    fetch >> upload >> load

weather_pipeline()