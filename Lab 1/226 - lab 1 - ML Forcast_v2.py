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
    # cur = conn.cursor()
    # cur.execute("USE WAREHOUSE BOA_query_wh")  # replace with your warehouse
    return conn.cursor()

@task
def make_format():
    con = return_snowflake_conn()
    create_format_sql = f"""CREATE OR REPLACE FILE FORMAT city_weather
        TYPE = 'CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = 1
        FIELD_DELIMITER = ','
        NULL_IF = ('NULL', '')
        DATE_FORMAT = 'MM/DD/YY';"""
    con.execute(create_format_sql)
    con.close()

@task
def train(con, train_input_table, train_view, model_name):
    # con = return_snowflake_conn()
    create_view_sql = f"""CREATE OR REPLACE VIEW {train_view} AS 
        (SELECT date as ds, temp_max, city
        FROM {train_input_table});"""
    
    create_model_sql = f"""CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {model_name} (
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{train_view}'),
        SERIES_COLNAME => 'city',
        TIMESTAMP_COLNAME => 'ds',
        TARGET_COLNAME => 'temp_max',
        CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
    );"""
    
    con.execute(create_view_sql)
    con.execute(create_model_sql)
    con.close()
    print(f"Model {model_name} created successfully with view {train_view}")


@task
def predict(con, model_name, forecast_table):
    # con = return_snowflake_conn()
    make_prediction_sql = f"""BEGIN
        CALL {model_name}!FORECAST(
            FORECASTING_PERIODS => 7,
            -- Here we set the prediction interval.
            CONFIG_OBJECT => {{'prediction_interval': 0.95}}
        );
        -- These steps store the predictions to a table.
        LET x := SQLID;
        CREATE OR REPLACE TABLE {forecast_table} AS SELECT * FROM TABLE(RESULT_SCAN(:x));
    END;"""
    con.execute(make_prediction_sql)
    con.close()
    print(f"Predictions generated and stored in {forecast_table} using model {model_name}")

  
@task
def present(con, train_input_table, forecast_table):
    # con = return_snowflake_conn()
    presentation = f"""SELECT city, date as ds, temp_max as actual, NULL as forecast, NULL as lower_bound, NULL as upper_bound
        FROM {train_input_table}
        UNION
        SELECT "SERIES" AS city, ts as ds, NULL AS actual, forecast, lower_bound, upper_bound
        FROM {forecast_table}
        ORDER BY ds DESC;"""
    con.execute(presentation)
    con.close()
    
    
with DAG(
    dag_id = 'ML_forecast',
    start_date = datetime(2026,2,23),
    catchup=False,
    tags=['ML', 'ETL'],
    schedule = '30 7 * * *'
) as dag:
    train_input_table = "USER_DB_BOA.raw.weather_data_lab1"
    train_view = "USER_DB_BOA.raw.weather_data_view_lab1"
    model_name = "USER_DB_BOA.analytics.forecast_model_lab1"
    forecast_table = "USER_DB_BOA.analytics.forecast_data_lab1"
    cur = return_snowflake_conn()
    
    #make_format()
    t1 = train(cur, train_input_table, train_view, model_name)
    t2 = predict(cur, model_name, forecast_table)
    t3 = present(cur, train_input_table, forecast_table)
    
    t1 >> t2 >> t3