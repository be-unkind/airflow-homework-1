import json
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor

from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow.macros import ds_format

from keys import *

def _process_weather(ti, city):
    info = ti.xcom_pull(f"{city}_group.extract_data_{city}")
    data = info["data"][0]

    timestamp = datetime.utcfromtimestamp(data["dt"])
    temp = float(data["temp"])
    humidity = float(data["humidity"])
    clouds = float(data["clouds"])
    windspeed = float(data["wind_speed"])

    return str(city), timestamp, temp, humidity, clouds, windspeed

def _show_table(**kwargs):
    hook = PostgresHook(postgres_conn_id="postgres_conn")
    result = hook.get_records("""
                              SELECT * FROM measures;
                              """
                              )
    for row in result:
        print(row)

with DAG(dag_id="weather_dag", schedule_interval="@daily", start_date=days_ago(3), catchup=True) as dag: 
    db_create = PostgresOperator(task_id="create_table",
                                 postgres_conn_id="postgres_conn",
                                 sql="""
                                     CREATE TABLE IF NOT EXISTS measures (
                                         city TEXT,
                                         timestamp TIMESTAMP,
                                         temp FLOAT,
                                         humidity FLOAT,
                                         clouds FLOAT,
                                         windspeed FLOAT
                                     );
                                 """
                                 )
    
    check_api = HttpSensor(task_id="check_api",
                           http_conn_id="weather_conn",
                           endpoint="data/3.0/onecall",
                           request_params={"lat": 0, "lon": 0, "appid": Variable.get("WEATHER_API_KEY")}
                           )
    
    with TaskGroup(group_id='lviv_group') as lviv_group:
        formatted_timestamp = '{{ execution_date.int_timestamp }}'

        extract_data_lviv = SimpleHttpOperator(task_id="extract_data_lviv",
                                               http_conn_id="weather_conn",
                                               endpoint="data/3.0/onecall/timemachine",
                                               data={"lat": LVIV_LAT, "lon": LVIV_LON, "dt": formatted_timestamp, "appid": Variable.get("WEATHER_API_KEY")},
                                               method="GET",
                                               response_filter=lambda x: json.loads(x.text),
                                               log_response=True)
        
        process_data_lviv = PythonOperator(task_id="process_data_lviv",
                                  python_callable=_process_weather,
                                  op_args=[],
                                  op_kwargs={'city': "lviv"},
                                  )
        
        inject_data_lviv = PostgresOperator(task_id="inject_data_lviv",
                                   postgres_conn_id="postgres_conn",
                                   sql="""
                                   INSERT INTO measures (city, timestamp, temp, humidity, clouds, windspeed)
                                   VALUES (%s, %s, %s, %s, %s, %s);
                                   """,
                                   parameters=[
                                       "{{ ti.xcom_pull(task_ids='lviv_group.process_data_lviv')[0] }}",
                                       "{{ ti.xcom_pull(task_ids='lviv_group.process_data_lviv')[1] }}",
                                       "{{ ti.xcom_pull(task_ids='lviv_group.process_data_lviv')[2] }}",
                                       "{{ ti.xcom_pull(task_ids='lviv_group.process_data_lviv')[3] }}",
                                       "{{ ti.xcom_pull(task_ids='lviv_group.process_data_lviv')[4] }}",
                                       "{{ ti.xcom_pull(task_ids='lviv_group.process_data_lviv')[5] }}",
                                       ],
                                   )

        extract_data_lviv >> process_data_lviv >> inject_data_lviv

    with TaskGroup(group_id='kyiv_group') as kyiv_group:
        formatted_timestamp = '{{ execution_date.int_timestamp }}'

        extract_data_kyiv = SimpleHttpOperator(task_id="extract_data_kyiv",
                                           http_conn_id="weather_conn",
                                           endpoint="data/3.0/onecall/timemachine",
                                           data={"lat": KYIV_LAT, "lon": KYIV_LON, "dt": formatted_timestamp, "appid": Variable.get("WEATHER_API_KEY")},
                                           method="GET",
                                           response_filter=lambda x: json.loads(x.text),
                                           log_response=True)
        
        process_data_kyiv = PythonOperator(task_id="process_data_kyiv",
                                  python_callable=_process_weather,
                                  op_args=[],
                                  op_kwargs={'city': "kyiv"},
                                  )
        
        inject_data_kyiv = PostgresOperator(task_id="inject_data_kyiv",
                                   postgres_conn_id="postgres_conn",
                                   sql="""
                                   INSERT INTO measures (city, timestamp, temp, humidity, clouds, windspeed)
                                   VALUES (%s, %s, %s, %s, %s, %s);
                                   """,
                                   parameters=[
                                       "{{ ti.xcom_pull(task_ids='kyiv_group.process_data_kyiv')[0] }}",
                                       "{{ ti.xcom_pull(task_ids='kyiv_group.process_data_kyiv')[1] }}",
                                       "{{ ti.xcom_pull(task_ids='kyiv_group.process_data_kyiv')[2] }}",
                                       "{{ ti.xcom_pull(task_ids='kyiv_group.process_data_kyiv')[3] }}",
                                       "{{ ti.xcom_pull(task_ids='kyiv_group.process_data_kyiv')[4] }}",
                                       "{{ ti.xcom_pull(task_ids='kyiv_group.process_data_kyiv')[5] }}",
                                       ],
                                   )

        extract_data_kyiv >> process_data_kyiv >> inject_data_kyiv

    with TaskGroup(group_id='odesa_group') as odesa_group:
        formatted_timestamp = '{{ execution_date.int_timestamp }}'

        extract_data_odesa = SimpleHttpOperator(task_id="extract_data_odesa",
                                           http_conn_id="weather_conn",
                                           endpoint="data/3.0/onecall/timemachine",
                                           data={"lat": ODESA_LAT, "lon": ODESA_LON, "dt": formatted_timestamp, "appid": Variable.get("WEATHER_API_KEY")},
                                           method="GET",
                                           response_filter=lambda x: json.loads(x.text),
                                           log_response=True)
        
        process_data_odesa = PythonOperator(task_id="process_data_odesa",
                                  python_callable=_process_weather,
                                  op_args=[],
                                  op_kwargs={'city': "odesa"},
                                  )
        
        inject_data_odesa = PostgresOperator(task_id="inject_data_odesa",
                                   postgres_conn_id="postgres_conn",
                                   sql="""
                                   INSERT INTO measures (city, timestamp, temp, humidity, clouds, windspeed)
                                   VALUES (%s, %s, %s, %s, %s, %s);
                                   """,
                                   parameters=[
                                       "{{ ti.xcom_pull(task_ids='odesa_group.process_data_odesa')[0] }}",
                                       "{{ ti.xcom_pull(task_ids='odesa_group.process_data_odesa')[1] }}",
                                       "{{ ti.xcom_pull(task_ids='odesa_group.process_data_odesa')[2] }}",
                                       "{{ ti.xcom_pull(task_ids='odesa_group.process_data_odesa')[3] }}",
                                       "{{ ti.xcom_pull(task_ids='odesa_group.process_data_odesa')[4] }}",
                                       "{{ ti.xcom_pull(task_ids='odesa_group.process_data_odesa')[5] }}",
                                       ],
                                   )

        extract_data_odesa >> process_data_odesa >> inject_data_odesa

    with TaskGroup(group_id='kharkiv_group') as kharkiv_group:
        formatted_timestamp = '{{ execution_date.int_timestamp }}'

        extract_data_kharkiv = SimpleHttpOperator(task_id="extract_data_kharkiv",
                                           http_conn_id="weather_conn",
                                           endpoint="data/3.0/onecall/timemachine",
                                           data={"lat": KHARKIV_LAT, "lon": KHARKIV_LON, "dt": formatted_timestamp, "appid": Variable.get("WEATHER_API_KEY")},
                                           method="GET",
                                           response_filter=lambda x: json.loads(x.text),
                                           log_response=True)
        
        process_data_kharkiv = PythonOperator(task_id="process_data_kharkiv",
                                  python_callable=_process_weather,
                                  op_args=[],
                                  op_kwargs={'city': "kharkiv"},
                                  )
        
        inject_data_kharkiv = PostgresOperator(task_id="inject_data_kharkiv",
                                   postgres_conn_id="postgres_conn",
                                   sql="""
                                   INSERT INTO measures (city, timestamp, temp, humidity, clouds, windspeed)
                                   VALUES (%s, %s, %s, %s, %s, %s);
                                   """,
                                   parameters=[
                                       "{{ ti.xcom_pull(task_ids='kharkiv_group.process_data_kharkiv')[0] }}",
                                       "{{ ti.xcom_pull(task_ids='kharkiv_group.process_data_kharkiv')[1] }}",
                                       "{{ ti.xcom_pull(task_ids='kharkiv_group.process_data_kharkiv')[2] }}",
                                       "{{ ti.xcom_pull(task_ids='kharkiv_group.process_data_kharkiv')[3] }}",
                                       "{{ ti.xcom_pull(task_ids='kharkiv_group.process_data_kharkiv')[4] }}",
                                       "{{ ti.xcom_pull(task_ids='kharkiv_group.process_data_kharkiv')[5] }}",
                                       ],
                                   )

        extract_data_kharkiv >> process_data_kharkiv >> inject_data_kharkiv
    
    with TaskGroup(group_id='zhmerynka_group') as zhmerynka_group:
        formatted_timestamp = '{{ execution_date.int_timestamp }}'

        extract_data_zhmerynka = SimpleHttpOperator(task_id="extract_data_zhmerynka",
                                           http_conn_id="weather_conn",
                                           endpoint="data/3.0/onecall/timemachine",
                                           data={"lat": ZHMERYNKA_LAT, "lon": ZHMERYNKA_LON, "dt": formatted_timestamp, "appid": Variable.get("WEATHER_API_KEY")},
                                           method="GET",
                                           response_filter=lambda x: json.loads(x.text),
                                           log_response=True)
        
        process_data_zhmerynka = PythonOperator(task_id="process_data_zhmerynka",
                                  python_callable=_process_weather,
                                  op_args=[],
                                  op_kwargs={'city': "zhmerynka"},
                                  )
        
        inject_data_zhmerynka = PostgresOperator(task_id="inject_data_zhmerynka",
                                   postgres_conn_id="postgres_conn",
                                   sql="""
                                   INSERT INTO measures (city, timestamp, temp, humidity, clouds, windspeed)
                                   VALUES (%s, %s, %s, %s, %s, %s);
                                   """,
                                   parameters=[
                                       "{{ ti.xcom_pull(task_ids='zhmerynka_group.process_data_zhmerynka')[0] }}",
                                       "{{ ti.xcom_pull(task_ids='zhmerynka_group.process_data_zhmerynka')[1] }}",
                                       "{{ ti.xcom_pull(task_ids='zhmerynka_group.process_data_zhmerynka')[2] }}",
                                       "{{ ti.xcom_pull(task_ids='zhmerynka_group.process_data_zhmerynka')[3] }}",
                                       "{{ ti.xcom_pull(task_ids='zhmerynka_group.process_data_zhmerynka')[4] }}",
                                       "{{ ti.xcom_pull(task_ids='zhmerynka_group.process_data_zhmerynka')[5] }}",
                                       ],
                                   )

        extract_data_zhmerynka >> process_data_zhmerynka >> inject_data_zhmerynka

    show_table = PythonOperator(task_id="show_table",
                                python_callable=_show_table,
                                provide_context=True,
                                )
    
    check_api >> db_create >> [lviv_group, kyiv_group, odesa_group, kharkiv_group, zhmerynka_group] >> show_table

