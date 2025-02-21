import os
import pandas as pd

BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../datalake'))

def gerar_relatorio():
    # Caminhos dos arquivos na camada 'trusted'
    trusted_path_atracacao = os.path.join(BASE_PATH, 'trusted', 'atracacao.parquet')
    trusted_path_carga = os.path.join(BASE_PATH, 'trusted', 'carga.parquet')
    
    # Caminho do relatório na camada 'business'
    business_path = os.path.join(BASE_PATH, 'business', 'relatorio_atracacao_carga_2021-2023.parquet')

    os.makedirs(os.path.join(BASE_PATH, 'business'), exist_ok=True)

    # Gerar o relatório a partir de Atracação
    if os.path.exists(trusted_path_atracacao):
        df_atracacao = pd.read_parquet(trusted_path_atracacao)

        # Converter as datas para o tipo datetime
        df_atracacao['Data Atracação'] = pd.to_datetime(df_atracacao['Data Atracação'], errors='coerce')
        df_atracacao['Data Chegada'] = pd.to_datetime(df_atracacao['Data Chegada'], errors='coerce')
        df_atracacao['Data Desatracação'] = pd.to_datetime(df_atracacao['Data Desatracação'], errors='coerce')

        # Agrupar por mês e contar as ocorrências
        resumo_atracacao = df_atracacao.groupby(df_atracacao['Data Atracação'].dt.to_period("M")).agg(
            total_atracacoes=('Data Atracação', 'count'),
            total_chegadas=('Data Chegada', 'count'),
            total_desatracacoes=('Data Desatracação', 'count')
        ).reset_index()

        resumo_atracacao['Data Atracação'] = resumo_atracacao['Data Atracação'].dt.strftime('%Y-%m-%d')

    else:
        print("⚠️ Arquivo de atracação não encontrado na camada 'trusted'")

    # Gerar o relatório a partir de Carga
    if os.path.exists(trusted_path_carga):
        df_carga = pd.read_parquet(trusted_path_carga)

        # Agrupar por mês e somar os valores
        resumo_carga = df_carga.groupby(df_carga['Data Chegada'].dt.to_period("M")).agg(
            total_peso=('VLPesoCargaBruta', 'sum'),
            total_volume=('QTVolume', 'sum')
        ).reset_index()

        resumo_carga['Data Chegada'] = resumo_carga['Data Chegada'].dt.strftime('%Y-%m-%d')

    else:
        print("⚠️ Arquivo de carga não encontrado na camada 'trusted'")

    # Combinar os dois resumos (Atracação e Carga)
    if 'resumo_atracacao' in locals() and 'resumo_carga' in locals():
        relatorio_final = pd.merge(resumo_atracacao, resumo_carga, left_on='Data Atracação', right_on='Data Chegada', how='outer')
        relatorio_final = relatorio_final.drop(columns=['Data Chegada'])

        # Salvar o relatório combinado na camada 'business'
        relatorio_final.to_parquet(business_path, index=False, engine="pyarrow")
        print(f"✅ Relatório gerado na camada 'business': {business_path}")
    else:
        print("⚠️ Não foi possível gerar o relatório devido à falta de dados de atracação ou carga.")

# Executar a geração do relatório
gerar_relatorio()
