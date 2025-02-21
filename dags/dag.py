from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow import DAG
import os

from scripts.captura_dados import captura_quantidade_dados
from scripts.processamento import mover_para_raw
from scripts.limpeza_dados import limpar_e_validar_dados
from scripts.gerar_relatorio import gerar_relatorio
from scripts.descompactar_zip import baixar_descompactar_zip

dag = DAG(
    'atracacoes_e_cargas',
    start_date=datetime(2025, 2, 15),
    schedule_interval=None,
    catchup=False
)

baixar_descompactar_task = PythonOperator(
    task_id='baixar_descompactar_arquivo_zip',
    python_callable=baixar_descompactar_zip,  # Função que será chamada
    dag=dag
)

# task_captura_quantidade_dados = PythonOperator(
#     task_id='captura_quantidade_dados',
#     python_callable=captura_quantidade_dados,
#     dag=dag
# )

# task_processar_txt_para_parquet = PythonOperator(
#     task_id='processar_txt_para_parquet',
#     python_callable=mover_para_raw,
#     dag=dag
# )

# task_limpar_e_validar = PythonOperator(
#     task_id='limpar_e_validar',
#     python_callable=limpar_e_validar_dados,
#     dag=dag
# )

# task_gerar_relatorio = PythonOperator(
#     task_id='gerar_relatorio',
#     python_callable=gerar_relatorio,
#     dag=dag
# )

baixar_descompactar_zip
# task_captura_quantidade_dados >> task_processar_txt_para_parquet >> task_limpar_e_validar >> task_gerar_relatorio
