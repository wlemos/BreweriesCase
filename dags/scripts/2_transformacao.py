import pandas as pd
import s3fs
import json

# Configurações do MinIO
minio_url = "http://minio:9000"
access_key = "minioadmin"
secret_key = "minioadmin"
bucket_leitura = "bronze"
bucket_gravacao = "silver"

# Caminho de leitura e gravação
path_leitura = f"{bucket_leitura}/breweries.json"
path_gravacao = f"{bucket_gravacao}/breweries"

try: 
    fs = s3fs.S3FileSystem(
        key=access_key,
        secret=secret_key,
        client_kwargs={'endpoint_url': minio_url}
    )
    print("Sistema de arquivos S3 configurado")

    # lendo JSON
    try:
        with fs.open(path_leitura, 'r') as file:
            breweries = json.load(file)
        print(f"Arquivo JSON lido com sucesso de {path_leitura}")
    except FileNotFoundError:
        print(f"Erro: Arquivo {path_leitura} não encontrado no MinIO.")
        raise
    except json.JSONDecodeError:
        print(f"Erro: Falha ao decodificar o JSON do arquivo {path_leitura}.")
        raise

    # JSON para DataFrame do Pandas
    try:
        breweries_data = pd.DataFrame(breweries)
        print("JSON convertido com sucesso para DataFrame do Pandas")
    except ValueError as e:
        print(f"Erro: Falha ao converter JSON para DataFrame - {e}")
        raise

    # Remover arquivos
    try:
        if fs.exists(path_gravacao):
            fs.rm(path_gravacao, recursive=True)
            print(f"Arquivos existentes em {path_gravacao} removidos")
    except Exception as e:
        print(f"Erro ao remover arquivos existentes em {path_gravacao}: {e}")
        raise

    # Gravar o DataFrame como Parquet particionado
    try:
        breweries_data.to_parquet(
            f"s3://{path_gravacao}",
            partition_cols=['state'],
            storage_options={
                "key": access_key,
                "secret": secret_key,
                "client_kwargs": {
                    "endpoint_url": minio_url
                }
            },
            engine='pyarrow'
        )
        print(f"DataFrame gravado com sucesso em Parquet em s3://{path_gravacao}")
    except Exception as e:
        print(f"Erro ao gravar o DataFrame em Parquet: {e}")
        raise    

except Exception as e:
    print(f"Ocorreu um erro: {e}")
