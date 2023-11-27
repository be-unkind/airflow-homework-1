## Overview

This repo contains implementation of simple DAG to process data about temperature, cloudiness, humidity and wind speed in different sities (execute daily, catchup with start day 3 days ago), and write these observations to the PostgreSQL database.

## How to run

Execute following line in the terminal (in homework-1 folder):

`docker-compose up`

## Variables and connections

All variables and connections have to be added directly through the Airflow UI.

Variables:
- WEATHER_API_KEY - api key to the OpenWeather (One Call API 3.0)

Connections:
- postrges_conn - connection to the PostgreSQL database
- weather_conn - http connection to OpenWeather

## Screenshots and results

All screenshots of the results can be found in the folder [screenshots](https://github.com/be-unkind/airflow-homework-1/tree/main/screenshots)
