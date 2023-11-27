## How to run

Execute following line in the terminal:

`docker-compose up`

## Variables and connections

All variables and connections have to be added directly through the Airflow UI.

Variables:
- WEATHER_API_KEY - api key to the OpenWeather (One Call API 3.0)

Connections:
- postrges_conn - connection to the PostgreSQL database
- weather_conn - http connection to OpenWeather

## Screenshots and results

All screenshots of the results can be found in the folder