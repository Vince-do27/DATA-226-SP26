# Fuel Price Analytics Pipeline

DATA 226 Group Project - Group 9

GitHub repository: 

This project is an end-to-end data analytics pipeline for fuel price analysis. In simple words, the project collects real fuel and energy market data, stores it in Snowflake, transforms it with dbt, creates forecasting output with Snowflake ML, and prepares final tables for dashboarding in Tableau or another BI tool.

The project is built around a very clear idea:

- EIA tells us what actual U.S. fuel prices were historically.
- Yahoo Finance through yfinance tells us what is happening in the current energy market.
- Snowflake stores the data.
- Airflow runs the pipelines.
- dbt creates clean analytics tables.
- Snowflake ML creates a forecast.
- Tableau or another BI tool visualizes the final results.

## 1. Problem Statement

Fuel prices affect almost everyone. When gasoline and diesel prices increase, people spend more money on transportation. Businesses also pay more for shipping, delivery, trucking, aviation, and supply chain operations. Because of this, it is useful to track fuel prices over time and understand what may be driving those price changes.

This project answers five main questions:

1. How are U.S. gasoline and diesel prices changing over time?
2. Which U.S. regions are more expensive or cheaper than others?
3. Are fuel prices stable or volatile?
4. How are crude oil prices related to retail gasoline prices?
5. What could regular gasoline prices look like over the next 12 weeks?

## 2. Data Sources

The project uses two real data sources. This meets the project requirement that asks for at least one historical data source and one real-time or frequently updated data source.

### 2.1 Historical Data Source: EIA API

The first data source is the U.S. Energy Information Administration API, usually called the EIA API.

Source URL:
https://api.eia.gov/v2/petroleum/pri/gnd/data/

EIA is a U.S. government source, so it is reliable and official. In this project, EIA is the historical or archive dataset.

The EIA API gives weekly fuel prices. The project pulls:

- U.S. national regular gasoline price
- U.S. national midgrade gasoline price
- U.S. national premium gasoline price
- U.S. national diesel price
- Regional regular gasoline prices by PADD region

The regional rows include:

- EAST_COAST
- MIDWEST
- GULF_COAST
- ROCKY_MOUNTAIN
- WEST_COAST
- CALIFORNIA

How much data is pulled:

- National prices: latest 200 weekly records
- Regional prices: latest 104 weekly records per region

Update frequency:

- Weekly, usually every Wednesday

Airflow DAG that uses this source:

```text
FuelPrice_EIA_ETL
```

Snowflake tables loaded by this source:

```text
USER_DB_BOA.RAW.FUEL_PRICES
USER_DB_BOA.RAW.REGIONAL_FUEL_PRICES
```

Important note:

`RAW.FUEL_PRICES` has only `US_NATIONAL` in the `REGION` column because this table stores national average prices. Regional prices are not missing. They are stored separately in `RAW.REGIONAL_FUEL_PRICES`.

### 2.2 Current / Near-Real-Time Data Source: Yahoo Finance through yfinance

The second data source is Yahoo Finance, accessed through the Python library `yfinance`.

Source URL:
https://finance.yahoo.com/

This is used as current or near-real-time market data. It is not gas station pump price data. Instead, it gives market prices for crude oil, gasoline futures, and energy sector instruments. This helps explain why gasoline prices may move up or down.

Update frequency:

- Yahoo Finance market data updates daily after market close.
- The Airflow DAG is scheduled daily.

Airflow DAG that uses this source:

```text
FuelPrice_Realtime_ETL
```

Snowflake table loaded by this source:

```text
USER_DB_BOA.RAW.ENERGY_MARKET_PRICES
```

The project pulls four tickers:

| Ticker | Meaning | Simple Explanation |
|---|---|---|
| CL=F | WTI Crude Oil Futures | U.S. crude oil benchmark |
| BZ=F | Brent Crude Oil Futures | Global crude oil benchmark |
| XLE | Energy Select Sector ETF | Tracks major energy companies |
| UGA | United States Gasoline Fund ETF | Tracks gasoline futures market movement |

#### CL=F: WTI Crude Oil Futures

`CL=F` represents WTI crude oil futures. WTI means West Texas Intermediate. It is one of the most important crude oil benchmarks in the United States.

Crude oil is the main raw material used to make gasoline and diesel. When crude oil becomes more expensive, gasoline prices often increase later. This is why `CL=F` is useful for fuel price analysis.

Simple explanation:

```text
CL=F tells us what is happening to U.S. crude oil prices.
```

#### BZ=F: Brent Crude Oil Futures

`BZ=F` represents Brent crude oil futures. Brent is a major global crude oil benchmark.

This helps the project understand global oil market pressure. Even though the project focuses on U.S. fuel prices, global crude oil prices can still affect U.S. gasoline and diesel markets.

Simple explanation:

```text
BZ=F tells us what is happening to global crude oil prices.
```

#### XLE: Energy Sector ETF

`XLE` is the Energy Select Sector ETF. It tracks large U.S. energy companies such as ExxonMobil and Chevron.

XLE does not directly represent gasoline pump prices. Instead, it shows how the broader energy sector is performing in the stock market.

Simple explanation:

```text
XLE tells us how major energy companies are performing.
```

#### UGA: U.S. Gasoline Fund ETF

`UGA` is the United States Gasoline Fund ETF. It is connected to gasoline futures.

This makes it useful because it is closer to gasoline market movement than a general stock market indicator.

Simple explanation:

```text
UGA tells us how the gasoline futures market is moving.
```

## 3. High-Level Architecture

The full system works like this:

```text
EIA API
  -> Airflow FuelPrice_EIA_ETL DAG
  -> Snowflake RAW.FUEL_PRICES
  -> Snowflake RAW.REGIONAL_FUEL_PRICES

Yahoo Finance / yfinance
  -> Airflow FuelPrice_Realtime_ETL DAG
  -> Snowflake RAW.ENERGY_MARKET_PRICES

RAW.FUEL_PRICES
  -> Airflow FuelPrice_TrainPredict DAG
  -> Snowflake ADHOC.FUEL_PRICE_TRAIN_VIEW
  -> Snowflake ADHOC.FUEL_PRICE_FORECAST
  -> Snowflake ANALYTICS.FUEL_PRICE_FINAL
  -> Snowflake ANALYTICS.FUEL_PRICE_MODEL_METRICS

RAW tables
  -> dbt models
  -> Snowflake DBT analytics tables

DBT and ANALYTICS tables
  -> Tableau / BI dashboard
```

## 4. Airflow DAGs

The project has four DAG files.

### 4.1 DAG 1: FuelPrice_EIA_ETL

File:

```text
dags/fuel_eia_etl_pipeline.py
```

Purpose:

This DAG pulls real weekly fuel price data from the EIA API and loads it into Snowflake.

Schedule:

```text
0 6 * * 3
```

This means it runs every Wednesday at 6:00 AM UTC.

Why Wednesday:

EIA weekly fuel price data usually updates weekly, so running weekly is appropriate.

Tasks:

```text
extract_national_prices
transform_national
load_national_prices

extract_regional_prices
transform_regional
load_regional_prices
```

Source and target mapping:

| Step | Source | Target |
|---|---|---|
| extract_national_prices | EIA API national fuel series | Airflow task memory |
| transform_national | Raw EIA JSON | Clean weekly tuples |
| load_national_prices | Clean national records | USER_DB_BOA.RAW.FUEL_PRICES |
| extract_regional_prices | EIA API regional series | Airflow task memory |
| transform_regional | Raw EIA JSON | Clean regional tuples |
| load_regional_prices | Clean regional records | USER_DB_BOA.RAW.REGIONAL_FUEL_PRICES |

This DAG uses MERGE logic, so re-running it updates existing rows instead of creating duplicate rows.

### 4.2 DAG 2: FuelPrice_Realtime_ETL

File:

```text
dags/fuel_realtime_etl_pipeline.py
```

Purpose:

This DAG pulls current energy market data from Yahoo Finance through `yfinance`.

Schedule:

```text
0 8 * * *
```

This means it runs daily at 8:00 AM UTC.

Tasks:

```text
extract_energy_market
transform_energy_market
load_energy_market
```

Source and target mapping:

| Step | Source | Target |
|---|---|---|
| extract_energy_market | yfinance tickers CL=F, BZ=F, XLE, UGA | Airflow task memory |
| transform_energy_market | yfinance dataframe rows | Clean ticker records |
| load_energy_market | Clean ticker records | USER_DB_BOA.RAW.ENERGY_MARKET_PRICES |

The target table stores:

- WEEK_DATE
- TICKER
- TICKER_NAME
- OPEN_PRICE
- HIGH_PRICE
- LOW_PRICE
- CLOSE_PRICE
- VOLUME
- LOAD_TS

This DAG also uses MERGE logic with `WEEK_DATE + TICKER` as the key.

### 4.3 DAG 3: FuelPrice_TrainPredict

File:

```text
dags/fuel_price_forecast.py
```

Purpose:

This DAG uses Snowflake ML Forecast to create a 12-week fuel price forecast.

Main source table:

```text
USER_DB_BOA.RAW.FUEL_PRICES
```

Main output tables:

```text
USER_DB_BOA.ADHOC.FUEL_PRICE_FORECAST
USER_DB_BOA.ANALYTICS.FUEL_PRICE_FINAL
USER_DB_BOA.ANALYTICS.FUEL_PRICE_MODEL_METRICS
```

What it does:

1. Creates a training view from historical fuel prices.
2. Trains a Snowflake ML forecast model.
3. Generates future forecast rows.
4. Combines historical actuals and forecasted rows into one final analytics table.

Final table:

```text
USER_DB_BOA.ANALYTICS.FUEL_PRICE_FINAL
```

This is one of the most important dashboard tables because it contains both historical values and forecast values.

### 4.4 DAG 4: FuelPrice_DBT

File:

```text
dags/fuel_dbt_dag.py
```

Purpose:

This DAG runs the dbt ELT layer.

Tasks:

```text
dbt_run
dbt_test
dbt_snapshot
```

Schedule:

```text
0 10 * * *
```

This means it runs daily at 10:00 AM UTC.

Important technical note:

This DAG requires `dbt` and `dbt-snowflake` to be installed inside the Airflow container. If dbt is not installed in the Airflow container, this DAG will fail with:

```text
dbt: command not found
```

That does not mean the dbt project is wrong. It means the Airflow container does not currently have the dbt command available.

For the class project, you can run dbt manually from your local terminal using:

```bash
cd /Users/kshitijarohandeshmukh/Downloads/DATA226_Data_Warehouse/gp9/fuel_price_project/dbt

dbt debug --profiles-dir . --project-dir .
dbt run --profiles-dir . --project-dir .
dbt test --profiles-dir . --project-dir .
dbt snapshot --profiles-dir . --project-dir .
```

## 5. Snowflake Schemas and Tables

Database:

```text
USER_DB_BOA
```

Warehouse:

```text
BOA_WH
```

Schemas:

| Schema | Purpose |
|---|---|
| RAW | Raw data loaded by Airflow ETL DAGs |
| ADHOC | Intermediate ML forecast objects |
| ANALYTICS | Final forecast and model metric tables |
| DBT | dbt transformation and snapshot tables |

### 5.1 RAW Tables

| Table | Loaded By | Meaning |
|---|---|---|
| RAW.FUEL_PRICES | FuelPrice_EIA_ETL | National U.S. weekly fuel prices |
| RAW.REGIONAL_FUEL_PRICES | FuelPrice_EIA_ETL | Regional weekly regular gasoline prices |
| RAW.ENERGY_MARKET_PRICES | FuelPrice_Realtime_ETL | yfinance energy market ticker data |

### 5.2 ADHOC Tables

| Table | Created/Used By | Meaning |
|---|---|---|
| ADHOC.FUEL_PRICE_TRAIN_VIEW | FuelPrice_TrainPredict | Clean training view for Snowflake ML |
| ADHOC.FUEL_PRICE_FORECAST | FuelPrice_TrainPredict | Raw forecast result output |

### 5.3 ANALYTICS Tables

| Table | Created/Used By | Meaning |
|---|---|---|
| ANALYTICS.FUEL_PRICE_FINAL | FuelPrice_TrainPredict | Historical and forecast rows together |
| ANALYTICS.FUEL_PRICE_MODEL_METRICS | FuelPrice_TrainPredict | Forecast model metrics |

### 5.4 DBT Tables

| Table | Created By | Meaning |
|---|---|---|
| DBT.PRICE_MOVING_AVG | price_moving_avg.sql | Rolling 4, 12, and 52 week averages |
| DBT.PRICE_VOLATILITY | price_volatility.sql | Week-over-week changes and volatility |
| DBT.CRUDE_CORRELATION | crude_correlation.sql | Relationship between gasoline and WTI crude oil |
| DBT.REGIONAL_COMPARISON | regional_comparison.sql | Regional prices compared with national average |
| DBT.FUEL_PRICES_SNAPSHOT | fuel_prices_snapshot.sql | SCD Type 2 snapshot of fuel price changes |

## 6. dbt Models Explained

### 6.1 price_moving_avg.sql

This model uses `RAW.FUEL_PRICES` and creates rolling averages.

It helps smooth out weekly price changes so the dashboard can show longer-term trends.

### 6.2 price_volatility.sql

This model uses `RAW.FUEL_PRICES` and calculates week-over-week price changes.

It helps identify when prices are changing quickly.

### 6.3 crude_correlation.sql

This model joins gasoline prices with `RAW.ENERGY_MARKET_PRICES` where:

```sql
TICKER = 'CL=F'
```

This means the model compares retail gasoline prices with WTI crude oil futures.

This is logical because crude oil is the largest raw material input for gasoline.

### 6.4 regional_comparison.sql

This model compares `RAW.REGIONAL_FUEL_PRICES` with the national average in `RAW.FUEL_PRICES`.

It helps answer questions such as:

- Which regions are above the national average?
- Which regions are below the national average?
- Which region has the biggest spread?

### 6.5 fuel_prices_snapshot.sql

This snapshot tracks changes in `RAW.FUEL_PRICES` over time.

It is an SCD Type 2 style table, which means it keeps historical versions when values change.

## 7. How To Run The Project

### Step 1: Start Airflow

From the project root:

```bash
cd /Users/kshitijarohandeshmukh/Downloads/DATA226_Data_Warehouse/gp9/fuel_price_project

docker compose up -d
```

Open Airflow:

```text
http://localhost:8081
```

Default login is usually:

```text
username: airflow
password: airflow
```

### Step 2: Create Airflow Variable

In Airflow UI:

```text
Admin -> Variables
```

Create:

```text
Key: eia_api_key
Value: your EIA API key
```

### Step 3: Create Airflow Snowflake Connection

In Airflow UI:

```text
Admin -> Connections
```

Create or edit:

```text
Connection Id: snowflake_conn
Connection Type: Snowflake
Login: BOA
Password: your changed Snowflake password
Schema: RAW
```

Extra JSON:

```json
{
  "account": "SFEDU02-EAB27764",
  "database": "USER_DB_BOA",
  "warehouse": "BOA_QUERY_WH",
  "role": "TRAINING_ROLE"
}
```

Why schema should be RAW:

The ETL DAGs load raw data first. The DAG code also runs `USE SCHEMA RAW`, but setting the connection schema to RAW keeps the connection aligned with the main ETL layer.

### Step 4: Run Snowflake Setup SQL

Run:

```text
snowflake_setup/snowflake_setup.sql
```

This creates schemas and tables in Snowflake.

### Step 5: Run Airflow DAGs

Run these in order:

1. `FuelPrice_EIA_ETL`
2. `FuelPrice_Realtime_ETL`
3. `FuelPrice_TrainPredict`
4. `FuelPrice_DBT` if dbt is installed in Airflow, otherwise run dbt locally

### Step 6: Run dbt Locally If Needed

```bash
cd /Users/kshitijarohandeshmukh/Downloads/DATA226_Data_Warehouse/gp9/fuel_price_project/dbt

dbt debug --profiles-dir . --project-dir .
dbt run --profiles-dir . --project-dir .
dbt test --profiles-dir . --project-dir .
dbt snapshot --profiles-dir . --project-dir .
```

## 8. Data Validation Queries

Use these queries in Snowflake to check that data loaded correctly.

```sql
SELECT COUNT(*) FROM USER_DB_BOA.RAW.FUEL_PRICES;
SELECT COUNT(*) FROM USER_DB_BOA.RAW.REGIONAL_FUEL_PRICES;
SELECT COUNT(*) FROM USER_DB_BOA.RAW.ENERGY_MARKET_PRICES;

SELECT *
FROM USER_DB_BOA.RAW.FUEL_PRICES
ORDER BY WEEK_DATE DESC
LIMIT 10;

SELECT REGION, COUNT(*)
FROM USER_DB_BOA.RAW.REGIONAL_FUEL_PRICES
GROUP BY REGION
ORDER BY REGION;

SELECT TICKER, COUNT(*)
FROM USER_DB_BOA.RAW.ENERGY_MARKET_PRICES
GROUP BY TICKER
ORDER BY TICKER;

SELECT COUNT(*) FROM USER_DB_BOA.DBT.PRICE_MOVING_AVG;
SELECT COUNT(*) FROM USER_DB_BOA.DBT.PRICE_VOLATILITY;
SELECT COUNT(*) FROM USER_DB_BOA.DBT.CRUDE_CORRELATION;
SELECT COUNT(*) FROM USER_DB_BOA.DBT.REGIONAL_COMPARISON;
SELECT COUNT(*) FROM USER_DB_BOA.ANALYTICS.FUEL_PRICE_FINAL;
```

## 9. Technical Review Status

Based on the current project files, the project is technically and logically strong.

What is correct:

- The fake hardcoded global prices were removed.
- yfinance is now used as a real current market data source.
- EIA is used as the historical/archive data source.
- Snowflake setup uses `USER_DB_BOA` and `BOA_QUERY_WH`.
- dbt models point to the correct RAW sources.
- `crude_correlation.sql` uses `RAW.ENERGY_MARKET_PRICES` with `TICKER = 'CL=F'`.
- `profiles.yml` uses environment variables, so the password is not hardcoded.
- `dbt_project.yml` does not force a duplicate `DBT_DBT` schema.
- The snapshot target database is `USER_DB_BOA`.

Main thing to watch:

- If `FuelPrice_DBT` fails in Airflow with `dbt: command not found`, install dbt properly in the Airflow image or run dbt locally from terminal.

## 10. Final Dashboard Tables

For Tableau or another BI tool, use these tables:

```text
USER_DB_BOA.ANALYTICS.FUEL_PRICE_FINAL
USER_DB_BOA.DBT.PRICE_MOVING_AVG
USER_DB_BOA.DBT.PRICE_VOLATILITY
USER_DB_BOA.DBT.CRUDE_CORRELATION
USER_DB_BOA.DBT.REGIONAL_COMPARISON
USER_DB_BOA.RAW.ENERGY_MARKET_PRICES
```

Recommended dashboard pages:

1. Historical national fuel price trend
2. Forecast vs actual fuel price trend
3. Regional price comparison
4. Volatility analysis
5. Crude oil vs gasoline relationship
6. Energy market ticker overview

## 11. Conclusion

This project satisfies the core requirements of the DATA 226 group project. It uses multiple data sources, builds a Snowflake warehouse, runs ETL pipelines with Airflow, builds analytics models with dbt, creates a forecast using Snowflake ML, and prepares dashboard-ready tables.

The strongest part of the project is that it does not only show fuel prices. It also connects fuel prices to crude oil and energy market signals, which makes the analysis more meaningful.
