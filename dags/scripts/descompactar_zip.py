import requests
import zipfile
import os
from io import BytesIO

# Caminho para a pasta 'reserva' dentro do seu projeto
RESERVA_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'reserva')

# URL do arquivo zip
zip_url = 'https://web3.antaq.gov.br/ea/txt/2021.zip'

def descompactar_zip_2021():
 # Enviar uma solicitação GET para obter o arquivo zip
    response = requests.get(zip_url)

    # Verificar se o download foi bem-sucedido
    if response.status_code == 200:
        print("Arquivo zip recebido com sucesso!")
        
        # Abrir o arquivo zip diretamente na memória
        with zipfile.ZipFile(BytesIO(response.content)) as zf:
            # Listar o conteúdo do zip (nomes dos arquivos dentro do zip)
            print("Conteúdo do arquivo zip:")
            for file_name in zf.namelist():
                print(file_name)
                
                # Verificar se o arquivo já existe na pasta 'reserva'
                file_path = os.path.join(RESERVA_PATH, file_name)
                if not os.path.exists(file_path):  # Se o arquivo não existe, extrair
                    # Caso o arquivo dentro do zip seja um arquivo .txt
                    if file_name.endswith('.txt'):
                        with zf.open(file_name) as file:
                            # Ler e salvar o arquivo .txt na pasta 'reserva'
                            file_content = file.read()

                            # Salvar o arquivo na pasta 'reserva'
                            with open(file_path, 'wb') as output_file:
                                output_file.write(file_content)
                            
                            print(f"Arquivo {file_name} salvo em {file_path}")
                else:
                    print(f"Arquivo {file_name} já existe em {file_path}, pulando extração.")
    else:
        print(f"Falha ao baixar o arquivo. Status: {response.status_code}")