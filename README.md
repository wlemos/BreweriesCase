# BreweriesCase - Projeto de Pipeline de Dados, Docker, Airflow e MinIO

Este projeto consiste em um pipeline de dados utilizando Airflow e MinIO para realizar a leitura de dados de uma API, transformação e agregação dos dados.


## Estrutura do Projeto

- `dags/`
  - `api_data_pipeline.py`: DAG do Airflow para o pipeline de dados.
  - `scripts/`
    - `1_leituraApi.py`: Script para leitura de dados da API.
    - `2_transformacao.py`: Script para transformação dos dados.
    - `3_tabelaAgregada.py`: Script para agregação dos dados.
- `docker-compose.yml`: Configuração do Docker Compose para o Airflow e MinIO.
- `Dockerfile`: Instalação das bibliotecas Python utilizadas

## Pré-requisitos

- Docker Desktop instalado.

## Configuração

### Passo 1: Clonar o Repositório

```bash
git clone <URL_DO_SEU_REPOSITORIO>
cd <NOME_DO_SEU_REPOSITORIO>
```

### Passo 2: Inicializar o Airflow
```bash
docker-compose up airflow-init
```

### Passo 3: Iniciar os Contêineres
```bash
docker-compose up -d
```

### Passo 4: Acessando as URLs

*Airflow* - http://localhost:8080/home
- `username`: admin
- `password`: admin

*MinIO* - http://localhost:9001/login
- `username`: minioadmin
- `password`: minioadmin

### Passo 5: Execução do Pipeline
Executar o pipeline `api_data_pipeline`.
Após a execução do Pipeline os Buckets do MinIO devem conter os itens em cada camada.


# Monitoramento Qualidades
- Validação do esquema (Verificiar se os dados recebidos possuem o esquema esperado)
- Validação dos valores (Verificar a consistência dos dados, se campos obrigatórios não estão nulos)
- Percentual de valores nulos em colunas críticas
- Contagem de duplicidades nas chaves
- Erros de tipos de dados (ex: string, int, data)

# Monitoramento Pipeline
- Registrar os logs detalhados de cada etapa
- Alertas por E-mail (Configurar para envio de email caso o Pipeline falhe ou finalize corretamente)
- Tempo de execução do pipeline
- Tempo de ingestão por etapa


Este `README.md` deve ajudar a configurar e executar o projeto.
