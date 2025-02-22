from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.utils.task_group import TaskGroup

from scripts.captura_dados import captura_dados
from scripts.processamento import processar_dados
from scripts.limpeza_dados import limpar_e_validar_dados
from scripts.gerar_relatorio import gerar_relatorio
from scripts.descompactar_zip import descompactar_zip_2021
from scripts.descompactar_zip import descompactar_zip_2022
from scripts.descompactar_zip import descompactar_zip_2023

dag = DAG(
    'atracacoes_e_cargas',
    start_date=datetime(2025, 2, 15),
    schedule_interval=None,
    catchup=False
)

with TaskGroup("descompactacao", dag=dag) as descompactacao_group:
    descompactar_arquivo_2021 = PythonOperator(
        task_id='descompactar_arquivo_2021',
        python_callable=descompactar_zip_2021,
        dag=dag 
    )

    descompactar_arquivo_2022 = PythonOperator(
        task_id='descompactar_arquivo_2022',
        python_callable=descompactar_zip_2022,
        dag=dag 
    )

    descompactar_arquivo_2023 = PythonOperator(
        task_id='descompactar_arquivo_2023',
        python_callable=descompactar_zip_2023,
        dag=dag 
    )

captura_quantidade_dados = PythonOperator(
    task_id='captura_dados',
    python_callable=captura_dados,
    dag=dag
)

processamento_de_dados = PythonOperator(
    task_id='processar_dados',
    python_callable=processar_dados,
    dag=dag
)

task_limpar_e_validar_dados = PythonOperator(
    task_id='limpar_e_validar',
    python_callable=limpar_e_validar_dados,
    dag=dag
)

task_gerar_relatorio = PythonOperator(
    task_id='gerar_relatorio',
    python_callable=gerar_relatorio,
    dag=dag
)

descompactar_arquivo_2021 >> descompactar_arquivo_2022 >> descompactar_arquivo_2023 >> captura_quantidade_dados >> processamento_de_dados >> task_limpar_e_validar_dados >> task_gerar_relatorio