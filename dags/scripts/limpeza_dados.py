import os
import pandas as pd

# Caminho base para o Data Lake
BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../datalake'))
RAW_PATH = os.path.join(BASE_PATH, 'raw')
TRUSTED_PATH = os.path.join(BASE_PATH, 'trusted')

# Criar a pasta trusted se não existir
os.makedirs(TRUSTED_PATH, exist_ok=True)

def limpar_e_validar_dados():
    # Listar todos os arquivos .parquet na camada 'raw'
    arquivos_parquet = [f for f in os.listdir(RAW_PATH) if f.endswith('.parquet')]

    if not arquivos_parquet:
        print("Nenhum arquivo .parquet encontrado na camada 'raw'.")
        return 0

    for arquivo in arquivos_parquet:
        origem = os.path.join(RAW_PATH, arquivo)
        destino = os.path.join(TRUSTED_PATH, arquivo)

        try:
            # Carregar o arquivo parquet
            df = pd.read_parquet(origem, engine="pyarrow")

            # Checando o nome do arquivo para determinar o tipo (Atracação ou Carga)
            if 'Atracacao' in arquivo:
                # Limpeza específica para Atracação
                df['Data Atracação'] = pd.to_datetime(df['Data Atracação'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
                df['Data Chegada'] = pd.to_datetime(df['Data Chegada'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
                df['Data Desatracação'] = pd.to_datetime(df['Data Desatracação'], format='%d/%m/%Y %H:%M:%S', errors='coerce')

                # Remover registros com valores ausentes
                df = df.dropna(subset=['Data Atracação', 'Data Chegada', 'Data Desatracação'])

            elif 'Carga' in arquivo:
                # Limpeza específica para Carga
                df['VLPesoCargaBruta'] = pd.to_numeric(df['VLPesoCargaBruta'], errors='coerce')
                df['QTVolume'] = pd.to_numeric(df['QTVolume'], errors='coerce')

                # Remover registros com valores ausentes
                df = df.dropna(subset=['VLPesoCargaBruta', 'QTVolume'])

            # Salvar o dataframe limpo na camada 'trusted'
            df.to_parquet(destino, engine="pyarrow", index=False)
            
            # Remover o arquivo original da camada 'raw' após processamento
            os.remove(origem)
            
            print(f"✅ Dados limpos e salvos na camada 'trusted': {destino}")
        except Exception as e:
            print(f"Erro ao processar {arquivo}: {e}")

    return len(arquivos_parquet)

# Executar a limpeza e validação
quantidade = limpar_e_validar_dados()
print(f"Total de arquivos limpos e movidos para 'trusted': {quantidade}")