import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import pandas as pd

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

def sqlConnector():
    # server = os.getenv('HOST')
    # database = os.getenv('DATABASE')
    # username = os.getenv('USER')
    # password = os.getenv('PASSWORD')

    # Conexão com o PostgreSQL
    engine = create_engine(f'postgresql+psycopg2://root:postgres@127.0.0.1/mydatabase')
    return engine

def load_data_postgres(df_data):
    table_name = 'usuario'
    schema = 'python_sql'  # Esquema padrão no PostgreSQL

    try:
        engine = sqlConnector()
        df_data.to_sql(name=table_name, index=False, con=engine, schema=schema, if_exists='append', method='multi',
                       chunksize=((2100 // len(df_data.columns)) - 1))
        print(f'Inserted into {schema}.{table_name}')
    except Exception as e:
        print('Error loading data into the database:', e)

    try:
        engine = sqlConnector()
        with engine.connect() as conn:
            conn.execute(text(f"""
                DELETE FROM {schema}.{table_name}
                USING (
                    SELECT id, inserted_at, 
                    ROW_NUMBER() OVER (PARTITION BY id ORDER BY inserted_at DESC) as rnum
                    FROM {schema}.{table_name}
                ) t
                WHERE {schema}.{table_name}.id = t.id AND {schema}.{table_name}.inserted_at = t.inserted_at AND t.rnum > 1;
            """))
        print('Data deduplication completed successfully.')
    except Exception as e:
        print('Error deduplicating data:', e)
