import requests
import json
import s3fs

# Endpoint da API
url = "https://api.openbrewerydb.org/breweries"

# Configurações MinIO
minio_url = "http://minio:9000"
access_key = "minioadmin"
secret_key = "minioadmin"
bucket_name = "bronze"


# Função de leitura para API
def fetch_all_data(api_url):
    all_breweries = []  
    next_page = api_url  
    
    while next_page:
        # Faz a requisição à API
        response = requests.get(next_page)

        # Verifica se a requisição foi bem-sucedida
        if response.status_code == 200:
            data = response.json()

            # Se a resposta for uma lista, significa que não há paginação
            if isinstance(data, list):
                all_breweries.extend(data)
                next_page = None  
            # Se a resposta for um dicionário, tenta pegar o campo 'next'
            elif isinstance(data, dict):
                next_page = data.get('next', None)
                if next_page:
                    print(f"Próxima página: {next_page}")
                else:
                    print("Não há mais páginas.")
                    
                # Se houver outras informações, podemos pegar e processar
                breweries = data.get('breweries', [])
                all_breweries.extend(breweries)
        
        else:
            print(f"Erro na requisição: {response.status_code}")
            break

    # Exibe o número total de cervejarias processadas
    print(f"Total de cervejarias coletadas: {len(all_breweries)}")
    return all_breweries
    
# Função para salvar os dados no MinIO
def save_data_to_file(data, file_path):
    fs = s3fs.S3FileSystem(
        key=access_key,
        secret=secret_key,
        client_kwargs={'endpoint_url': minio_url})
  
    with fs.open(file_path, 'w', encoding="utf-8") as file:
        json.dump(data, file, indent=4)
    print(f"Dados salvos em {file_path}")

# Caminho os dados serão salvos
file_path = f"{bucket_name}/breweries.json"

# Obtém todos os dados paginados e salva no arquivo
breweries_data = fetch_all_data(url)
save_data_to_file(breweries_data, file_path)