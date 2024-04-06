from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import requests
import json


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 1, 1),
    'quantity': int,
}

def captura_conta_dados(**kwargs):
    parameters = kwargs['dag_run'].conf
    # Processamento dos parâmetros
    print(parameters)
    # url = "https://data.cityofnewyork.us/resource/rc75-m7u3.json"
    # response = requests.get(url)
    # df = pd.DataFrame(json.loads(response.content))
    print(parameters['quantity'])
    # qtd = len(response.quantity)
    return parameters['quantity']

def e_valida(ti):
    qtd = ti.xcom_pull(task_ids = 'captura_conta_dados')
    if(qtd > 1000):
        return 'valido'
    return 'nvalido'

with DAG('01_testing_api', schedule_interval = None, catchup= False, default_args=default_args) as dag:

    captura_conta_dados = PythonOperator(
        task_id = 'captura_conta_dados',
        provide_context=True,
        python_callable = captura_conta_dados
    )

    e_valida = BranchPythonOperator(
        task_id = 'e_valida',
        python_callable = e_valida
    )

    valido = BashOperator(
        task_id = "valido",
        bash_command = "echo 'Quantidade Ok'"
    )

    nvalido = BashOperator(
        task_id = "nvalido",
        bash_command = "echo 'Quantidade não Ok'"
    )

    captura_conta_dados >> e_valida >> [valido, nvalido]