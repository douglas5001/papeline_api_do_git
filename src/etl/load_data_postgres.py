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

    # try:
    #     engine = sqlConnector()
    #     df_data.to_sql(name=table_name, index=False, con=engine, schema=schema, if_exists='append', method='multi',
    #                    chunksize=((2100 // len(df_data.columns)) - 1))
    #     print(f'Inserted into {schema}.{table_name}')
    # except Exception as e:
    #     print('Error loading data into the database:', e)

    try:
        engine = sqlConnector()
        with engine.connect() as conn:
            for index, row in df_data.iterrows():
                # Verificar se o ID já existe
                result = conn.execute(text(f"""
                    SELECT 1 FROM {schema}.{table_name} WHERE id = :id
                """), {"id": row['id']})

                if result.fetchone():
                    # Atualizar a linha existente
                    conn.execute(text(f"""
                        UPDATE {schema}.{table_name}
                        SET userid = :userid, title = :title, body = :body, inserted_at = :inserted_at
                        WHERE id = :id
                    """), {
                        "userid": row['userid'],
                        "title": row['title'],
                        "body": row['body'],
                        "inserted_at": row['inserted_at'],
                        "id": row['id']
                    })
                else:
                    # Inserir novo dado
                    conn.execute(text(f"""
                        INSERT INTO {schema}.{table_name} (userid, id, title, body, inserted_at)
                        VALUES (:userid, :id, :title, :body, :inserted_at)
                    """), {
                        "userid": row['userid'],
                        "id": row['id'],
                        "title": row['title'],
                        "body": row['body'],
                        "inserted_at": row['inserted_at']
                    })
            conn.close()
        print('Data deduplication completed successfully.')
    except Exception as e:
        print('Error deduplicating data:', e)