import os
import shutil

BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../datalake'))
DATA_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../data'))
LANDING_PATH = os.path.join(BASE_PATH, 'landing')

# Criar a pasta landing se não existir
os.makedirs(LANDING_PATH, exist_ok=True)

def captura_quantidade_dados():
    arquivos_txt = [dados for dados in os.listdir(DATA_PATH) if dados.endswith('.txt')]
    
    if not arquivos_txt:
        print("Nenhum arquivo .txt encontrado na pasta data.")
        return 0
    
    for arquivo in arquivos_txt:
        origem = os.path.join(DATA_PATH, arquivo)
        destino = os.path.join(LANDING_PATH, arquivo)
        
        shutil.copy(origem, destino)
        
        print(f"Arquivo movido para landing: {destino}")

    return len(arquivos_txt)

quantidade = captura_quantidade_dados()
print(f"Total de arquivos movidos: {quantidade}")
