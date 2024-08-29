from datetime import datetime

import pendulum

from airflow.decorators import task, dag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


@dag(
    dag_id='get_tables',
    schedule='0 0 *  *   * ',
    catchup=False,
    concurrency=4,
    start_date=pendulum.yesterday("Europe/Moscow"),
)
def dinamyc_get_table():
    POSTGRES_CONN_ID = 'PostgresBI'
    PG_HOOK = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    @task
    def get_table_list():
        from airflow.models import Variable

        db_df = PG_HOOK.get_pandas_df(sql="""SELECT table_name
                                        FROM information_schema.tables
                                        WHERE table_schema = 'test'
                                        ORDER BY table_name;""")
        db_table_list = db_df['table_name'].tolist()
        # Обнуляем спискок
        Variable.set('list_download', [], serialize_json=True)
        # Добавляем в переменную список таблиц
        Variable.set('list_download', db_table_list, serialize_json=True)
        return db_table_list

    @task
    def add_one(x: str):
        db_df = PG_HOOK.get_pandas_df(sql=f"SELECT * FROM test.{x}")
        print(db_df)

    values = get_table_list()
    trigger_dag = TriggerDagRunOperator(
        task_id='trigger',
        trigger_dag_id='record',
    )
    values >> trigger_dag
    add_one.expand(x=values)


dinamyc_get_table()
