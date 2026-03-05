from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
#import snowflake.connector
import requests
import pandas as pd

default_args = {
	'owner': 'vdo27',
	'email': ['vincent.do@sjsu.edu'],
	'retries': 1,
	'retry_delay': timedelta(minutes=3),
}

def return_snowflake_conn():
    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

#The Extract Method
@task
def get_past_60_days_weather(latitude, longitude, city):
    """Get the past 60 days of weather"""

    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "past_days": 60,
        "forecast_days": 0,  # only past weather
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "weather_code"
            ],
        "timezone": "America/Los_Angeles"
    }

    response = requests.get(url, params=params)
    
    if response.status_code != 200:
        raise RuntimeError(f'API request failed: {response.status_code}')

    return response.json()


@task
def combine_data(dataOne, dataTwo):
    return pd.concat([dataOne, dataTwo], ignore_index=True)

#the Transform method
@task
def transform(raw_data, latitude, longitude, city):
    if 'daily' not in raw_data:
        raise ValueError("Missing 'daily' key in API response")

    data = raw_data['daily']

# Convert data into DataFrame
    df = pd.DataFrame({
		"latitude": latitude,
		"longitude": longitude,
     	"date": data["time"],
     	"temp_max": data["temperature_2m_max"],
     	"temp_min": data["temperature_2m_min"],
     	"precipitation": data["precipitation_sum"],
     	"weather_code": data["weather_code"],
		"city": city
	})

    df["date"] = pd.to_datetime(df["date"])
	
    return df

#The Load Method
@task
def load(con, target_table, records):
    # full refresh
    try:
        con.execute("BEGIN;")
        con.execute(f"""CREATE TABLE IF NOT EXISTS {target_table} (
			latitude		FLOAT,
			longitude		FLOAT,
			date			DATE,
			temp_max		FLOAT,
			temp_min		FLOAT,
			precipitation	FLOAT,
			weather_code	VARCHAR(3),
			city			VARCHAR(100),
			PRIMARY KEY (latitude, longitude, date));""")
    
        # Delete all records
        con.execute(f"""DELETE FROM {target_table}""")

        # Insert rows from DataFrame
        for i,r in records.iterrows():
            latitude = r['latitude']
            longitude = r['longitude']
            date = r['date']
            temp_max = r['temp_max']
            temp_min = r['temp_min']
            precipitation = r['precipitation']
            weather_code = r['weather_code']
            city = r['city']

            sql = f"""INSERT INTO {target_table} (latitude, longitude, date, temp_max, temp_min, precipitation, weather_code, city)
			VALUES ('{latitude}', '{longitude}','{date}','{temp_max}','{temp_min}','{precipitation}','{weather_code}','{city}')"""
            con.execute(sql)
        con.execute("COMMIT;")
        print(f"Loaded {len(records)} records into {target_table}")

    except Exception as e:
        con.execute("ROLLBACK;")
        print(e)
        raise e

    
with DAG(
    dag_id = 'Open_Meteo',
    start_date = datetime(2026,2,23),
    catchup=False,
    tags=['ETL'],
    default_args=default_args,
    schedule = '30 7 * * *'
) as dag:
    latis = Variable.get("LATISAN")         # San Jose Latitude
    longis = Variable.get("LONGSAN")        # San Jose Longitude
    latit = Variable.get("Latitude")        # South Lake Tahoe Latitude
    longit = Variable.get("Longitude")        # South Lake Tahoe Longitude
    san = Variable.get("SANJOSE")
    tah = Variable.get("LAKETAHOE")    
    
    target_table = "USER_DB_BOA.raw.weather_data_lab1"

    con = return_snowflake_conn()

    datas_raw = get_past_60_days_weather(latis, longis, san)
    datat_raw = get_past_60_days_weather(latit, longit, tah)

    datas_df = transform(datas_raw, latis, longis, san)
    datat_df = transform(datat_raw, latit, longit, tah)

    data = combine_data(datas_df, datat_df)

    load(con, target_table, data)