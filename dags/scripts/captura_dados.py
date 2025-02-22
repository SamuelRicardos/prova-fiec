import os
import shutil

# Caminho para a pasta 'reserva' (localização dos arquivos .txt)
BASE_PATH = os.path.abspath(os.path.dirname(__file__))  # Caminho para a raiz do projeto
RESERVA_PATH = os.path.join(BASE_PATH, '../../reserva')  # Caminho para a pasta 'reserva'
LANDING_PATH = os.path.join(BASE_PATH, '../datalake', 'landing')  # Caminho para a pasta 'landing' dentro de 'datalake'

# Lista de arquivos permitidos para processamento
ARQUIVOS_PERMITIDOS = {
    "2021Atracacao.txt", "2021Carga.txt",
    "2022Atracacao.txt", "2022Carga.txt",
    "2023Atracacao.txt", "2023Carga.txt"
}

def captura_dados():
    # Verificando se a pasta 'reserva' existe e contém arquivos permitidos
    arquivos_txt = [f for f in os.listdir(RESERVA_PATH) if f in ARQUIVOS_PERMITIDOS]

    if not arquivos_txt:
        print("Nenhum arquivo permitido encontrado na pasta 'reserva'.")
        return 0

    os.makedirs(LANDING_PATH, exist_ok=True)  # Garantir que a pasta landing exista

    for arquivo in arquivos_txt:
        origem = os.path.join(RESERVA_PATH, arquivo)
        destino = os.path.join(LANDING_PATH, arquivo)

        try:
            # Movendo o arquivo permitido da pasta 'reserva' para a pasta 'landing'
            shutil.move(origem, destino)
            print(f"Arquivo {arquivo} movido para a camada 'landing'.")
        except Exception as e:
            print(f"Erro ao mover {arquivo}: {e}")

    return len(arquivos_txt)

# Executando a função para mover os dados
if __name__ == "__main__":
    captura_dados()