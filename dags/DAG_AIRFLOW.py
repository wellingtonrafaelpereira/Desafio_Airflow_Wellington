from airflow.utils.edgemodifier import Label
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable
import sqlite3
import pandas as pd
import os  # Adicionado para verificar a existência do arquivo

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,  # Desabilitar envio de e-mail em caso de falha
    'email_on_retry': False,    # Desabilitar envio de e-mail em caso de repetição
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

## Do not change the code below this line ---------------------!!#
def export_final_answer():
    import base64

    # Verificar se o arquivo count.txt existe
    if not os.path.exists('count.txt'):
        # Se não existir, crie o arquivo com valor padrão (0)
        with open('count.txt', 'w') as f:
            f.write("0\n")  # Escreve 0 ou outro valor que você quiser

    # Import count
    with open('count.txt') as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    message = my_email + count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open("final_output.txt", "w") as f:
        f.write(base64_message)
    return None
## Do not change the code above this line-----------------------##


# Função para extrair os dados da tabela 'Order' e salvar em CSV
def extract_orders_to_csv():
    # Caminho para o banco de dados SQLite
    db_path = r'/mnt/c/Users/wrpen/OneDrive/Área de Trabalho/desafio7/airflow_tooltorial/data/Northwind_small.sqlite'

    # Conectar ao banco de dados
    conn = sqlite3.connect(db_path)
    # Executar a consulta SQL para ler a tabela 'Order'
    query = 'SELECT * FROM "Order"'
    # Carregar os dados em um DataFrame
    df = pd.read_sql_query(query, conn)
    # Salvar os dados em um arquivo CSV
    output_path = r'/mnt/c/Users/wrpen/OneDrive/Área de Trabalho/desafio7/airflow_tooltorial/data/output_orders.csv'

    df.to_csv(output_path, index=False)
    # Fechar a conexão com o banco de dados
    conn.close()

# Função para ler os dados da tabela 'OrderDetail', fazer o JOIN e calcular a soma da quantidade
def calculate_quantity_rio():
    # Caminho para o banco de dados SQLite
    db_path = r'/mnt/c/Users/wrpen/OneDrive/Área de Trabalho/desafio7/airflow_tooltorial/data/Northwind_small.sqlite'

    # Conectar ao banco de dados
    conn = sqlite3.connect(db_path)

    # Carregar os dados da tabela 'OrderDetail'
    order_detail_query = 'SELECT * FROM "OrderDetail"'
    df_order_detail = pd.read_sql_query(order_detail_query, conn)

    # Carregar os dados da tabela 'Order' que foi exportada anteriormente
    df_orders = pd.read_csv(r'/mnt/c/Users/wrpen/OneDrive/Área de Trabalho/desafio7/airflow_tooltorial/data/output_orders.csv')

    # Fazer o JOIN entre as tabelas com a nova instrução
    merged_df = pd.merge(df_orders, df_order_detail, how='inner', left_on='Id', right_on='OrderId')

    # Filtrar os dados para a cidade do Rio de Janeiro
    rio_sales = merged_df[merged_df['ShipCity'] == 'Rio de Janeiro']

    # Calcular a soma da quantidade vendida
    total_quantity = rio_sales['Quantity'].sum()

    # Exportar o resultado para o arquivo count.txt
    with open('count.txt', 'w') as f:
        f.write(str(total_quantity))  # Escreve a soma em formato texto

    # Fechar a conexão com o banco de dados
    conn.close()


with DAG(
    'DesafioAirflow',
    default_args=default_args,
    description='Desafio de Airflow da Indicium',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    dag.doc_md = """
        Esse é o desafio de Airflow da Indicium.
    """
    
    # Task para extrair os dados da tabela 'Order' para CSV
    extract_orders = PythonOperator(
        task_id='extract_orders_to_csv',
        python_callable=extract_orders_to_csv,
        provide_context=True
    )

    # Task para calcular a quantidade vendida para o Rio de Janeiro
    calculate_quantity = PythonOperator(
        task_id='calculate_quantity_rio',
        python_callable=calculate_quantity_rio,
        provide_context=True
    )

    export_final_output = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_answer,
        provide_context=True
    )

    # Definir a ordem das tasks
    extract_orders >> calculate_quantity >> export_final_output
