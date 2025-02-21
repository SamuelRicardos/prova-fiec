import os
import pandas as pd

BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../datalake'))

def gerar_relatorio():
    trusted_path = os.path.join(BASE_PATH, 'trusted', 'dados_covid_ny.parquet')
    business_path = os.path.join(BASE_PATH, 'business', 'relatorio_covid_ny.parquet')

    os.makedirs(os.path.join(BASE_PATH, 'business'), exist_ok=True)

    if os.path.exists(trusted_path):
        df = pd.read_parquet(trusted_path)

        df['date_of_interest'] = pd.to_datetime(df['date_of_interest'], format='%Y-%m-%d', errors='coerce')

        resumo = df.groupby(df['date_of_interest'].dt.to_period("M")).agg(
            total_casos=('case_count', 'sum'),
            total_hospitalizacoes=('hospitalized_count', 'sum')
        ).reset_index()

        resumo['date_of_interest'] = resumo['date_of_interest'].dt.strftime('%Y-%m-%d')

        resumo.to_parquet(business_path, index=False, engine="pyarrow")
        
        print(f"✅ Relatório gerado na camada 'business': {business_path}")
    else:
        print("⚠️ Arquivo não encontrado na camada 'trusted'")
