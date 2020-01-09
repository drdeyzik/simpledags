import datetime
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.operators import python_operator


default_dag_args = {
    'start_date': airflow.utils.dates.days_ago(0)
}

with airflow.DAG(
        'weather_dag2',
        schedule_interval=datetime.timedelta(minutes=15),
        default_args=default_dag_args) as dag:
    def pipeline():
        import requests

	#2 hours from current time is the limit imposed by Weather HOD API
	end_time = datetime.datetime.now() - timedelta(hours=2)
	start_time = end_time - timedelta(minutes=15)
        url = 'https://us-central1-moovestage.cloudfunctions.net/function6'
        requests.post(url, data={'start_time': start_time.strftime('%Y%m%d%H%M'), 'end_time': end_time.strftime('%Y%m%d%H%M')})


    task = python_operator.PythonOperator(
	task_id='weather_pipeline',
	python_callable=pipeline)
