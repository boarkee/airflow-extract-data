from airflow import DAG
import pendulum # exact datetime
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator 
from airflow.macros import ds_add
from os.path import join
import pandas as pd

with DAG(
    'climate_data',
    start_date=pendulum.datetime(2022, 8, 22, tz='UTC'), 
    schedule_interval='0 0 * * 1' # execute every monday at midnight
    ) as dag:

    task_1 = BashOperator(
        task_id='create_folder',
        bash_command='mkdir -p "/home/boarkee/Documents/airflow/week={{data_interval_end.strftime("%Y-%m-%d")}}"'
    )

    def extract_data(data_interval_end):
        city = 'Boston'
        key = 'JVL7P8VKPVSTWDDDM5VPL43EF'

        URL = join('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
                    f'{city}/{data_interval_end}/{ds_add(data_interval_end, 7)}?unitGroup=metric&include=days&key={key}&contentType=csv')

        df = pd.read_csv(URL)

        file_path = f'/home/boarkee/Documents/airflow/week={data_interval_end}/'


        df.to_csv(file_path + 'raw_data.csv')
        df[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(file_path + 'temperatures.csv')
        df[['datetime', 'description', 'icon']].to_csv(file_path + 'condition.csv')     

    task_2 = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        op_kwargs={'data_interval_end' : '{{data_interval_end.strftime("%Y-%m-%d")}}'}
    )

    task_1 >> task_2