import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, select, update
from sqlalchemy.dialects.mysql import insert
from datetime import datetime

# Configuração da conexão com o banco de dados
engine = create_engine('mysql+pymysql://root:root@127.0.0.1:3306/mysql_python')

# Exemplo de dataframe
data = {
    'id': [1, 2, 3],
    'nome': ['Alice', 'Bob', 'Charlie'],
    'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com'],
    'insert_data': [datetime(2024, 7, 14), datetime(2024, 7, 14), datetime(2024, 7, 14)]
}
df = pd.DataFrame(data)

# Nome da tabela no banco de dados
table_name = 'minha_tabela'

# Conexão ao banco de dados
conn = engine.connect()
metadata = MetaData()
table = Table(table_name, metadata, autoload_with=engine)

for index, row in df.iterrows():
    stmt = insert(table).values(
        id=row['id'],
        nome=row['nome'],
        email=row['email'],
        insert_data=row['insert_data']
    )
    on_duplicate_key_stmt = stmt.on_duplicate_key_update(
        nome=row['nome'],
        email=row['email'],
        insert_data=row['insert_data']
    )

    # Executa o statement
    conn.execute(on_duplicate_key_stmt)

# Fecha a conexão
conn.close()
