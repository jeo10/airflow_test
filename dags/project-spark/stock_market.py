import json, requests
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sensors.base import PokeReturnValue
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def _get_stock_prices(url, symbol):
    url = f"{url}{symbol}?metrics=high?&interval=1d&range=1y"
    api = BaseHook.get_connection('stock_api')
    response = requests.get(url, headers=api.extra_dejson['headers'])
    return json.dumps(response.json()['chart']['result'][0])

def _store_prices(stock):
    bucket_name = "airflow-test"
    hook = S3Hook(aws_conn_id='minio_s3')
    if not hook.check_for_bucket(bucket_name):
        hook.create_bucket(bucket_name)
    stock = json.loads(stock)
    symbol = stock['meta']['symbol']
    data = json.dumps(stock, ensure_ascii=False).encode('utf-8')
    hook.load_bytes(
        bytes_data=data,
        key=f'{symbol}/prices.json',
        bucket_name=bucket_name,
        replace=True,
        encrypt=False
    )
    return f'{bucket_name}/{symbol}',


@dag(
    start_date=datetime(2021, 12, 12, 0, 0),
    schedule='@daily',
    catchup=False,
    tags=['stock_market']
)
def stock_market():
    symbol = 'NVDA'

    @task.sensor(poke_interval=30, timeout=300, mode='poke')
    def is_api_available() -> PokeReturnValue:
        api = BaseHook.get_connection('stock_api')
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        print(url)
        response = requests.get(url, headers=api.extra_dejson['headers'])
        condition = response.json()['finance']['result'] is None
        return PokeReturnValue(is_done=condition, xcom_value=url)

    get_stock_prices = PythonOperator(
        task_id='get_stock_prices',
        python_callable=_get_stock_prices,
        op_kwargs={'url': '{{ti.xcom_pull(task_ids="is_api_available")}}', 'symbol': symbol},
    )

    store_prices = PythonOperator(
        task_id='store_prices',
        python_callable=_store_prices,
        op_kwargs={'stock': '{{ti.xcom_pull(task_ids="get_stock_prices")}}'}
    )

    is_api_available() >> get_stock_prices >> store_prices

stock_market()