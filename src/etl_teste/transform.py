# src/transform.py
import os

import pandas as pd
from datetime import datetime
import glob
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def transform_data(df):
    #     # Obtém a data atual no formato YYYYMMDD
    #     current_date = datetime.now().strftime('%d-%m-%Y')

    #     # Define o caminho do diretório base
    #     base_dir = '/Users/douglasportella/date/bronze'

    #     # Constrói o caminho completo para o diretório do dia atual
    #     dir_path = os.path.join(base_dir, current_date)

    #     # Encontra o único arquivo Excel na pasta
    #     excel_files = glob.glob(os.path.join(dir_path, '*.xlsx'))

    #     if len(excel_files) != 1:
    #         raise FileNotFoundError(f"Esperado um único arquivo Excel em {dir_path}, mas encontrado {len(excel_files)} arquivos.")

    #     file_path = excel_files[0]

    #     # Lê o arquivo Excel
    #     df = pd.read_excel(file_path)

    df_cols = pd.DataFrame(columns=['userId', 'id', 'title', 'body'])
    df_data = pd.concat([df_cols, df], ignore_index=True, join='inner')

    df_data = df_data.rename({
        'userId': 'userid',
        'id': 'id',
        'title': 'title',
        'body': 'body'
    }, axis=1)

    df_data = df_data.astype('string')
    df_data['inserted_at'] = datetime.now()
    # df_data['cpf'] = df_data['cpf'].str.replace('.', '', regex=False).str.replace('-', '', regex=False)

    today = datetime.now().date()
    directory = r"/app/date/ouro"
    #directory = r"/Users/douglasportella/date/ouro"
    directory_today = os.path.join(directory, today.strftime('%d-%m-%Y'))

    # Cria o diretório se não existir
    if not os.path.exists(directory_today):
        os.makedirs(directory_today)

    timestamp = datetime.now().strftime('%d-%m-%Y_%H-%M-%S')
    csv_file = os.path.join(directory_today, f"dados_coletados_{timestamp}.xlsx")
    df_data.to_excel(csv_file, index=False)
    logging.info("transformacao feita!!")

    return df_data

