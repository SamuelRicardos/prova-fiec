import os
import shutil

# Caminho para a pasta 'data' na raiz do projeto
BASE_PATH = os.path.abspath(os.path.dirname(__file__))  # Caminho para a raiz do projeto
DATA_PATH = os.path.join(BASE_PATH, '../../data')  # Caminho para a pasta 'data'
LANDING_PATH = os.path.join(BASE_PATH, 'datalake', 'landing')  # Caminho para a pasta 'landing' dentro de 'datalake'

def captura_quantidade_dados():
    # Verificando se a pasta 'data' existe e contém arquivos .txt
    arquivos_txt = [f for f in os.listdir(DATA_PATH) if f.endswith('.txt')]

    if not arquivos_txt:
        print("Nenhum arquivo .txt encontrado na pasta 'data'.")
        return 0

    os.makedirs(LANDING_PATH, exist_ok=True)  # Garantir que a pasta landing existe

    for arquivo in arquivos_txt:
        origem = os.path.join(DATA_PATH, arquivo)
        destino = os.path.join(LANDING_PATH, arquivo)

        try:
            # Movendo o arquivo .txt da pasta 'data' para a pasta 'landing'
            shutil.move(origem, destino)
            print(f"Arquivo {arquivo} movido para a camada 'landing'.")
        except Exception as e:
            print(f"Erro ao mover {arquivo}: {e}")

    return len(arquivos_txt)