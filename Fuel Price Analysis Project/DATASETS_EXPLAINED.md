# DATASETS USED — FUEL PRICE ANALYTICS
# DATA 226 Group Project

=======================================================
DATASET 1 — EIA API (Historical + Regional Fuel Prices)
=======================================================

What:
U.S. Energy Information Administration Open Data API

URL:
https://api.eia.gov/v2/petroleum/pri/gnd/data/

Cost:
Free

Needs:
EIA API key

Stored in Airflow as:
eia_api_key

Why we use it:
EIA is an official U.S. government data source. It gives reliable weekly gasoline and diesel prices. This is our main historical/archive dataset.

What data it gives us:

1. Weekly U.S. national average prices for:
   - Regular gasoline, USD per gallon
   - Midgrade gasoline, USD per gallon
   - Premium gasoline, USD per gallon
   - Diesel, USD per gallon

2. Weekly U.S. regional regular gasoline prices by PADD region:
   - EAST_COAST
   - MIDWEST
   - GULF_COAST
   - ROCKY_MOUNTAIN
   - WEST_COAST
   - CALIFORNIA

How far back we pull:
- National fuel prices: latest 200 weekly records
- Regional fuel prices: latest 104 weekly records per region

Update frequency:
Weekly, usually every Wednesday

Used in Airflow DAG:
FuelPrice_EIA_ETL

Loads into Snowflake tables:
USER_DB_FERRET.RAW.FUEL_PRICES
USER_DB_FERRET.RAW.REGIONAL_FUEL_PRICES

Table meaning:

RAW.FUEL_PRICES:
Stores national U.S. weekly average gasoline and diesel prices.
This table has REGION = US_NATIONAL because it is national-level data.

RAW.REGIONAL_FUEL_PRICES:
Stores regional weekly regular gasoline prices.
This table contains regions such as EAST_COAST, MIDWEST, GULF_COAST, ROCKY_MOUNTAIN, WEST_COAST, and CALIFORNIA.

=======================================================
DATASET 2 — Yahoo Finance via yfinance
Current / Near-Real-Time Energy Market Data
=======================================================

What:
Yahoo Finance market data accessed through the yfinance Python library

URL:
https://finance.yahoo.com/

Cost:
Free

Needs:
No API key
No signup

Why we use it:
EIA gives pump price history, but fuel prices are also affected by the energy market. yfinance gives current market signals for crude oil, gasoline futures, and energy companies. This helps explain why pump prices may move up or down.

Important note:
This is not gas station pump price data. It is current financial market data related to oil, gasoline, and the energy sector.

What data it gives us:

Ticker CL=F:
WTI Crude Oil Futures

Meaning:
WTI means West Texas Intermediate. It is a major U.S. crude oil benchmark.

Why it matters:
Crude oil is the main raw material used to make gasoline and diesel. If crude oil prices rise, gasoline prices often rise later.

Ticker BZ=F:
Brent Crude Oil Futures

Meaning:
Brent is a major global crude oil benchmark.

Why it matters:
It helps show global crude oil market pressure, not just U.S. oil prices.

Ticker XLE:
Energy Select Sector ETF

Meaning:
XLE tracks large U.S. energy companies such as ExxonMobil and Chevron.

Why it matters:
It shows how the broader energy sector is performing.

Ticker UGA:
United States Gasoline Fund ETF

Meaning:
UGA tracks gasoline futures market movement.

Why it matters:
It gives a market-based signal related to gasoline prices.

How far back we pull:
4 years of weekly OHLCV data

OHLCV means:
- Open price
- High price
- Low price
- Close price
- Volume

Update frequency:
Market data updates daily after market close.
Our Airflow DAG runs daily.

Used in Airflow DAG:
FuelPrice_Realtime_ETL

Loads into Snowflake table:
USER_DB_FERRET.RAW.ENERGY_MARKET_PRICES

Table meaning:

RAW.ENERGY_MARKET_PRICES:
Stores all four tickers in one table:
CL=F, BZ=F, XLE, UGA

Each row represents one ticker for one week.

=======================================================
SUMMARY TABLE
=======================================================

DAG                         Data Source                  Table Loaded                                Type
-------------------------   --------------------------   ----------------------------------------   ----------------------------
FuelPrice_EIA_ETL           EIA API                      RAW.FUEL_PRICES                            Historical / Archive
FuelPrice_EIA_ETL           EIA API                      RAW.REGIONAL_FUEL_PRICES                   Historical / Regional
FuelPrice_Realtime_ETL      Yahoo Finance / yfinance     RAW.ENERGY_MARKET_PRICES                  Current / Near-Real-Time
FuelPrice_TrainPredict      RAW.FUEL_PRICES              ANALYTICS.FUEL_PRICE_FINAL                 Forecast Output
FuelPrice_DBT               RAW tables                   DBT.PRICE_MOVING_AVG                       ELT Analytics
FuelPrice_DBT               RAW tables                   DBT.PRICE_VOLATILITY                       ELT Analytics
FuelPrice_DBT               RAW tables                   DBT.CRUDE_CORRELATION                      ELT Analytics
FuelPrice_DBT               RAW tables                   DBT.REGIONAL_COMPARISON                    ELT Analytics

=======================================================
WHY THIS MEETS THE PROJECT REQUIREMENT
=======================================================

Project requirement:
Use at least two different data sources:
1. One historical/archive data source
2. One real-time or frequently updated data source

Source 1:
EIA API

Type:
Historical / Archive

Reason:
It provides official weekly gasoline and diesel price history from the U.S. government.

Source 2:
Yahoo Finance through yfinance

Type:
Current / Near-Real-Time Market Data

Reason:
It provides daily-updated energy market prices for crude oil, gasoline futures, and energy sector indicators.

Together, these two sources help answer:

What are fuel pump prices doing?
EIA answers this.

Why might fuel prices be changing?
yfinance energy market data helps explain this.

=======================================================
FINAL SIMPLE EXPLANATION
=======================================================

In simple words:

EIA tells us the actual historical gasoline and diesel prices.

yfinance tells us what is happening right now in the energy market.

EIA is used for fuel price history and forecasting.

yfinance is used to understand crude oil and gasoline market movement.

Together, they make the project stronger because we are not only showing fuel prices, but also connecting those prices to real market drivers.
