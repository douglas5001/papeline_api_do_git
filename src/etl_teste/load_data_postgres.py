import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import pandas as pd

# Carrega as variáveis de ambiente do arquivo .env
# load_dotenv()

def sqlConnector():
    # Conexão com o PostgreSQL
    engine = create_engine('postgresql+psycopg2://root:postgres@127.0.0.1/mydatabase')
    return engine

def load_data_postgres(df_data):
    table_name = 'usuario'
    schema = 'python_sql'

    try:
        engine = sqlConnector()
        df_data.to_sql(name=table_name, index=False, con=engine, schema=schema, if_exists='append', method='multi',
                       chunksize=((2100 // len(df_data.columns)) - 1))
        print(f'Inserted into {schema}.{table_name}')
    except Exception as e:
        print('Error loading data into the database:', e)

    try:
        conn = engine.connect()
        conn.execute(text(f"""
            WITH cte AS (SELECT id, max(inserted_at) AS max_data FROM {schema}.{table_name} GROUP BY id)
            DELETE FROM {schema}.{table_name} t USING cte WHERE t.id = cte.id AND t.inserted_at <> cte.max_data
            RETURNING *;
        """))
        conn.commit()
        conn.close()
        print('Dados duplicados foram removidos')
    except Exception as e:
        print('Error deduplicating data:', e)
