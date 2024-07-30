from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from clickhouse_driver import Client
import pandas as pd

import clickhouse_connect
import psycopg2
import json

default_args ={
    'owner': 'pozdnyakov',
    'start_date': datetime(2024, 7, 22)
}

dag = DAG(
    dag_id= 'my_dag',
    default_args= default_args,
    schedule_interval= '5 * * * *',
    description= 'study dag',
    catchup= False,
    max_active_runs= 1
)

def main():
    print('WTF!!!!!!!!!!!!!!!!!!!!!!!!!!')
    client_ch = Client('some-clickhouse-server'
                       , port=9000
                       , verify=False
                       , database='default'
                       , settings={'numpy_columns':False, 'use_numpy': True}
                       , compression=False)
    
    client_ch.execute(f"""INSERT INTO report.tareTransfer_loc_crop
                              select *
                              from default.tareTransfer_loc
                              where dt > now() - interval 7 day
                       """)

    result = client_ch.execute(f"""select tare_id
                                        , dt
                                        , place_cod
                                        , any(wh_tare_status_type) wh_tare_status_type
                                        , any(wh_tare_entry)       wh_tare_entry
                                   from report.tareTransfer_loc_crop
                                   group by tare_id, dt, place_cod""")

    columns = ['tare_id', 'dt', 'place_cod', 'wh_tare_status_type', 'wh_tare_entry']
    df = pd.DataFrame(result, columns=columns)

    print(df.head())
    print(df.columns)

    df['dt_date'] = df['dt'].dt.strftime('%Y-%m-%d')

    agg_df = df.groupby([ 'place_cod', 'dt_date', 'wh_tare_status_type', 'wh_tare_entry']).agg(qty_tares=('tare_id', 'nunique')).reset_index()
    agg_json = agg_df.to_json(orient='records')

    conn_params = {
        'dbname': 'default',
        'user': 'default',
        'password': '123445',
        'host': 'docker_pg',
        'port': '5432'
    }

    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()

    call_proc_query = """CALL sync.taretransfer_agg_insert(%s);"""

    try:
        cursor.execute(call_proc_query, [agg_json])
        conn.commit()
        print("Данные успешно переданы и обработаны процедурой")
    except Exception as e:
        print(f"Ошибка при выполнении процедуры: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

task = PythonOperator(task_id='my_dag', python_callable=main, dag=dag)
