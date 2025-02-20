import pandas as pd
import os

BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../datalake'))
LANDING_PATH = os.path.join(BASE_PATH, 'landing')
RAW_PATH = os.path.join(BASE_PATH, 'raw')

# Criar a pasta raw se não existir
os.makedirs(RAW_PATH, exist_ok=True)

def processar_txt_para_parquet():
    arquivos_txt = [f for f in os.listdir(LANDING_PATH) if f.endswith('.txt')]

    if not arquivos_txt:
        print("Nenhum arquivo .txt encontrado na camada 'landing'.")
        return 0

    for arquivo in arquivos_txt:
        origem = os.path.join(LANDING_PATH, arquivo)
        destino = os.path.join(RAW_PATH, arquivo.replace('.txt', '.parquet'))
        
        try:
            df = pd.read_csv(origem, delimiter=';', encoding='utf-8')  # Ajuste o delimitador conforme necessário
            df.to_parquet(destino, engine="pyarrow", index=False)
            
            os.remove(origem)  # Remove o arquivo original após a conversão
            
            print(f"Arquivo convertido e movido para camada 'raw': {destino}")
        except Exception as e:
            print(f"Erro ao processar {arquivo}: {e}")

    return len(arquivos_txt)

# Executar o processamento
quantidade = processar_txt_para_parquet()
print(f"Total de arquivos convertidos: {quantidade}")
