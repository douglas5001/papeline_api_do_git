import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# Verifica se as variáveis de ambiente foram carregadas corretamente
# print(f"HOST: {os.getenv('HOST')}")
# print(f"DATABASE: {os.getenv('DATABASE')}")
# print(f"USER: {os.getenv('USER')}")
# print(f"PASSWORD: {os.getenv('PASSWORD')}")


def sqlConnector():
    server = os.getenv('HOST')
    database = os.getenv('DATABASE')
    #username = os.getenv('USER')
    password = os.getenv('PASSWORD')

    engine = create_engine(f'mysql+pymysql://root:{password}@{server}:3306/{database}')
    return engine


def load_data(df_data):
    table_name = 'usuario'
    schema = 'mysql_python'

    #     # Obtém a data atual no formato DDMMYYY
    #     current_date = datetime.now().strftime('%d-%m-%Y')

    #     # Define o caminho do diretório base
    #     base_dir = '/Users/douglasportella/date/ouro'

    #     # Constrói o caminho completo para o diretório do dia atual
    #     dir_path = os.path.join(base_dir, current_date)

    #     # Encontra o único arquivo Excel na pasta
    #     excel_files = glob.glob(os.path.join(dir_path, '*.xlsx'))

    #     if len(excel_files) != 1:
    #         raise FileNotFoundError(f"Esperado um único arquivo Excel em {dir_path}, mas encontrado {len(excel_files)} arquivos.")

    #     file_path = excel_files[0]

    #     # Lê o arquivo Excel
    #     df_data = pd.read_excel(file_path)

    try:
        table_name = 'usuario'
        schema = 'mysql_python'
        engine = sqlConnector()
        df_data.to_sql(name=table_name, index=False, con=engine, schema=schema, if_exists='append', method='multi',
                       chunksize=((2100 // len(df_data.columns)) - 1))
        logging.info(f'Inserted into {schema}.{table_name}')
    except Exception as e:
        logging.info('Error loading data into the database:', e)

    try:
        engine = sqlConnector()
        with engine.connect() as conn:
            conn.execute(text(f"""
                DELETE t
                FROM {schema}.{table_name} t
                LEFT JOIN (
                    SELECT id_principal, MAX(inserted_at) AS max_data
                    FROM {schema}.{table_name}
                    GROUP BY id_principal
                ) cte
                ON t.id_principal = cte.id_principal AND t.inserted_at <> cte.max_data
                WHERE cte.id_principal IS NOT NULL
            """))
            conn.close()
        logging.info('Data deduplication completed successfully.')
    except Exception as e:
        logging.info('Error deduplicating data:', e)
