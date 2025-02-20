from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow import DAG

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../scripts')))

from scripts.captura_dados import captura_quantidade_dados
from scripts.processamento import mover_para_raw
from scripts.limpeza_dados import limpar_e_validar_dados
from scripts.gerar_relatorio import gerar_relatorio

dag = DAG(
    'atracacoes_e_cargas_2021',
    start_date=datetime(2021, 12, 1),
    schedule_interval='30 * * * *',
    catchup=False
)

task_captura_quantidade_dados = PythonOperator(
    task_id='captura_quantidade_dados',
    python_callable=captura_quantidade_dados,
    provide_context=True,
    dag=dag
)

task_mover_para_raw = PythonOperator(
    task_id='mover_para_raw',
    python_callable=mover_para_raw,
    dag=dag
)

task_limpar_e_validar = PythonOperator(
    task_id='limpar_e_validar',
    python_callable=limpar_e_validar_dados,
    dag=dag
)

task_gerar_relatorio = PythonOperator(
    task_id='gerar_relatorio',
    python_callable=gerar_relatorio,
    dag=dag
)

task_captura_quantidade_dados >> task_mover_para_raw >> task_limpar_e_validar >> task_gerar_relatorio
