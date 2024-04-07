from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
import pandas as pd
import requests
import json

default_args = {
    'userName': str,
    'password': str,
    'start_date': datetime(2021, 1, 1),
    'sourceMarketRegion': str,
    'apiKey': str,
}

def getting_bookings_for_users(**kwargs):
    parameters = kwargs['dag_run'].conf
    gygiaPrivateKey = Variable.get("GIGYA_PRIVATE_KEY")
    GIGYA_URL = Variable.get("GIGYA_URL") + '/accounts.login'
    
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    # Dados do formulário
    data = {
        'loginID': parameters['userName'],
        'password': parameters['password'],
        'apiKey': parameters['apiKey'],
    }
    
    try:
      response = requests.post(GIGYA_URL, headers=headers, data=data)
      response.raise_for_status() 
      print('POST request successful!')
      if response.text:  # Verifica se há conteúdo na resposta
        print('POST request successful!')
        user = response.json()
        print('userID:', user['UID'])  # Converte a resposta para JSON e imprime
      else:
        print('Empty response received.')
    except requests.exceptions.RequestException as e:
      print('POST request failed:', e)

    return parameters['sourceMarketRegion']

def preload_booking_card_images(ti):
    sourceMarketRegion = ti.xcom_pull(task_ids = 'getting_bookings_for_users')
    if(sourceMarketRegion == 'isCR'):
        return 'isCR'
    return 'isNR'

def retrieve_associated_bookings(ti):
    sourceMarketRegion = ti.xcom_pull(task_ids = 'preload_booking_card_images')
    if(sourceMarketRegion == 'is_central_region'):
        return 'is_central_region'
    return 'isNR'

def is_central_region(ti):
    sourceMarketRegion = ti.xcom_pull(task_ids = 'retrieve_associated_bookings')
    if(sourceMarketRegion == 'isCentralRegion'):
        return 'get_all_cr_bookings'
    return 'isNR'

def get_all_cr_bookings(ti):
    sourceMarketRegion = ti.xcom_pull(task_ids = 'is_central_region')
    if(sourceMarketRegion == 'isCentralRegion'):
        return 'isCentralRegion'
    return 'isNR'

def get_all_wr_bookings(ti):
    sourceMarketRegion = ti.xcom_pull(task_ids = 'retrieve_associated_bookings')
    if(sourceMarketRegion == 'isNR'):
        return 'isNR'
    return 'isWR'

def get_all_nr_bookings(ti):
    sourceMarketRegion = ti.xcom_pull(task_ids = 'is_northern_region')
    if(sourceMarketRegion == 'isNR'):
        return 'isNR'
    return 'isWR'    

def get_all_nordic_bookings(ti):
    sourceMarketRegion = ti.xcom_pull(task_ids = 'get_all_nordic_bookings')
    if(sourceMarketRegion == 'isNR'):
        return 'isNR'
    return 'isWR'

def get_all_uk_bookings(ti):
    sourceMarketRegion = ti.xcom_pull(task_ids = 'get_all_uk_bookings')
    if(sourceMarketRegion == 'isNR'):
        return 'isNR'
    return 'isWR'

def get_all_wr_bookings(ti):
    sourceMarketRegion = ti.xcom_pull(task_ids = 'is_western_region')
    if(sourceMarketRegion == 'isWR'):
        return 'isWR'
    return 'isNR'

def group_all_bookings(ti):
    sourceMarketRegion = ti.xcom_pull(task_ids = 'get_all_wr_bookings')
    if(sourceMarketRegion == 'isWR'):
        return 'isWR'
    return 'isNR'

with DAG('booking_list', schedule_interval = None, catchup= False, default_args=default_args) as dag:

    # preloadBookingCardImages
    # getAssociateBookingsAuthToken
    # Retrieved associated bookings. Number of bookings: ${bookingList.length}
    # getAllWRBookings
    # getAllLegacyBeBookings
    # getAllUnknownBookings

    getting_bookings_for_users = PythonOperator(
        task_id = 'getting_bookings_for_users',
        provide_context=True,
        python_callable = getting_bookings_for_users
    )

    preload_booking_card_images = PythonOperator(
        task_id = 'preload_booking_card_images',
        python_callable = preload_booking_card_images
    )

    retrieve_associated_bookings = BranchPythonOperator(
        task_id = 'retrieve_associated_bookings',
        python_callable = retrieve_associated_bookings
    )

    is_central_region = BranchPythonOperator(
        task_id = "is_central_region",
        python_callable = is_central_region
    )

    get_all_cr_bookings = PythonOperator(
        task_id = "get_all_cr_bookings",
        python_callable = get_all_cr_bookings
    )

    is_northern_region = BranchPythonOperator(
        task_id = "is_northern_region",
        python_callable = get_all_nr_bookings
    )

    get_all_nordic_bookings = BranchPythonOperator(
        task_id = "get_all_nordic_bookings",
        python_callable = get_all_nordic_bookings
    )

    get_all_uk_bookings = BranchPythonOperator(
        task_id = "get_all_uk_bookings",
        python_callable = get_all_uk_bookings
    )

    is_western_region = BranchPythonOperator(
        task_id = "is_western_region",
        python_callable = get_all_wr_bookings
    )

    get_all_wr_bookings = BranchPythonOperator(
        task_id = "get_all_wr_bookings",
        python_callable = get_all_wr_bookings
    )

    group_all_bookings = PythonOperator(
        task_id='group_all_bookings',
        python_callable=group_all_bookings,
        provide_context=True
    )

    getting_bookings_for_users >> preload_booking_card_images >> retrieve_associated_bookings
    retrieve_associated_bookings >> is_central_region
    is_central_region >> get_all_cr_bookings
    is_northern_region >> get_all_nordic_bookings
    is_northern_region >> get_all_uk_bookings
    retrieve_associated_bookings >> is_northern_region
    retrieve_associated_bookings >> is_western_region
    is_western_region >> get_all_wr_bookings
    [get_all_cr_bookings, get_all_wr_bookings, get_all_uk_bookings, get_all_nordic_bookings] >> group_all_bookings