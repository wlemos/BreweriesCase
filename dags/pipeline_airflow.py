from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

# Definindo argumentos padrão
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Definindo o DAG
with DAG(
    'api_data_pipeline',
    default_args=default_args,
    description='Pipeline para leitura, transformação e agregação de dados da API',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:
    
    # Definindo as tarefas do pipeline

    # Tarefa de início
    inicio = DummyOperator(
        task_id='inicio',
        dag=dag,
    )

    # Tarefa de leitura da API
    task_leitura_api = BashOperator(
        task_id='API',
        bash_command='python /opt/airflow/dags/scripts/1_leituraApi.py',
        on_failure_callback=lambda context: print(f"Erro na tarefa leitura_api: {context['exception']}")
    )

    # Tarefa de transformação dos dados
    task_transformacao = BashOperator(
        task_id='Transformação',
        bash_command='python /opt/airflow/dags/scripts/2_transformacao.py',
        on_failure_callback=lambda context: print(f"Erro na tarefa transformacao: {context['exception']}")
    )

    # Tarefa de agregação dos dados
    task_agregacao = BashOperator(
        task_id='Agregado',
        bash_command='python /opt/airflow/dags/scripts/3_tabelaAgregada.py',
        on_failure_callback=lambda context: print(f"Erro na tarefa agregacao: {context['exception']}")
    )

    # Tarefa de fim
    fim = DummyOperator(
        task_id='fim',
        dag=dag,
    )

    # Definindo a ordem das tarefas no pipeline
    inicio >> task_leitura_api >> task_transformacao >> task_agregacao >> fim
