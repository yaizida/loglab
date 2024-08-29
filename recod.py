from datetime import datetime

import pendulum

from airflow.decorators import task, dag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.sensors.sql import SqlSensor


@dag(
    dag_id='record',
    schedule='0 0 * *   * ',
    catchup=False,
    concurrency=4,
    start_date=pendulum.yesterday("Europe/Moscow"),
)
def record():
    @task
    def get_table_list(table_name):
        print(f"Загрузка данных из таблицы {table_name}")
        # Добавьте код для загрузки данных из таблицы table_name
        # Например:
        # postgres_hook = PostgresHook(postgres_conn_id='list_download')
        # data = postgres_hook.get_records(f"SELECT * FROM {table_name}")
        # # Обработайте полученные данные (сохраните, отправьте и т.д.)

    for table_name in Variable.get('list_download', deserialize_json=True):
        record_exists_sensor = SqlSensor(
            task_id=f'check_record_exists_{table_name}',
            conn_id='PostgresBI',
            # Запрос должен вернуть не нулевое количество строк
            sql=f"SELECT COUNT(*) FROM test.{table_name}",
        )
        get_table_list(table_name) << record_exists_sensor


record()
