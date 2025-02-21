from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow import DAG

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../scripts')))

from scripts.captura_dados import captura_quantidade_dados
from scripts.processamento import processar_txt_para_parquet
from scripts.limpeza_dados import limpar_e_validar_dados
from scripts.gerar_relatorio import gerar_relatorio

dag = DAG(
    'atracacoes_e_cargas',
    start_date=datetime(2025, 2, 15),
    schedule_interval=None,
    catchup=False
)

task_captura_quantidade_dados = PythonOperator(
    task_id='captura_quantidade_dados',
    python_callable=captura_quantidade_dados,
    provide_context=True,
    dag=dag
)

task_processar_txt_para_parquet = PythonOperator(
    task_id='processar_txt_para_parquet',
    python_callable=processar_txt_para_parquet,
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

task_captura_quantidade_dados >> task_processar_txt_para_parquet >> task_limpar_e_validar >> task_gerar_relatorio
