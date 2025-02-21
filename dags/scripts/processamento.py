import pandas as pd
import os

BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../datalake'))

def mover_para_raw():
    landing_path = os.path.join(BASE_PATH, 'landing', 'dados_covid_ny.csv')
    raw_path = os.path.join(BASE_PATH, 'raw', 'dados_covid_ny.parquet')

    os.makedirs(os.path.join(BASE_PATH, 'raw'), exist_ok=True)

    if os.path.exists(landing_path):
        df = pd.read_csv(landing_path)

        df['date_of_interest'] = pd.to_datetime(df['date_of_interest'])

        df.to_parquet(raw_path, engine="pyarrow", index=False)

        os.remove(landing_path)

        print(f"Arquivo convertido e movido para camada 'raw': {raw_path}")
    else:
        print("Arquivo não encontrado na camada 'landing'")
