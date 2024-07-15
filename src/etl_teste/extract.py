# src/extract.py

import pandas as pd
import requests
import time
from datetime import datetime
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# def extract_data():
#     data = pd.read_csv('usuarios.csv')
#     df = pd.DataFrame(data)
#     return df

def extract_data():
    def collect_date(page):
        url = "https://jsonplaceholder.typicode.com/posts"
        params = {
            "_page": page
        }

        response = requests.get(url, params)
        return response

    # PARA O LIMITE DE REQUISICOES POR PAGE
    limite_diferentes_consultas = 12
    tempo_limite_minutos = 15
    requisicoes_diferentes_consultas = 0
    tempo_inicio = time.time()

    today = datetime.now().date()
    #directory = r"/Users/douglasportella/date/bronze"
    directory = r"/app/date/bronze"
    directory_today = os.path.join(directory, today.strftime('%d-%m-%Y'))

    # Cria o diretório se não existir
    if not os.path.exists(directory_today):
        os.makedirs(directory_today)

    new_table = pd.DataFrame()
    page = 1
    while True:

        ## LIMINTE DE REQUISICOES A CADA 10 PAGE
        tempo_decorrido = time.time() - tempo_inicio
        if requisicoes_diferentes_consultas >= limite_diferentes_consultas:
            if tempo_decorrido < (tempo_limite_minutos * 60):
                tempo_espera = (tempo_limite_minutos * 60) - tempo_decorrido
                logging.info(
                    f"Limite de requisições para consultas diferentes atingido. Esperando {tempo_espera:.1f} segundos.")
                time.sleep(tempo_espera)
            requisicoes_diferentes_consultas = 0
            tempo_inicio = time.time()

        try:
            response = collect_date(page)
            logging.info(f"requisicao feita na pagina {page}")
        except requests.RequestException as e:
            logging.info(f"erro ao realizar requisicao {e}")
            break

        if response.status_code == 200:
            logging.info("Requisicao 200 bem sucedida")
        elif response.status_code == 429:
            logging.info("Requisicao bloqueada")
            time.sleep(60 * 60)
            continue
        else:
            logging.info("Erro ao acessar a api")
            logging.info(f"Erro ao acessar a API: {response.status_code}")
        try:
            converted_file = response.json()
        except ValueError as e:
            logging.info(f"Erro ao decodificar json", e)
            break
        if not converted_file:
            logging.info(F"Nenum dado recebido na padina {page}")
            break

        table = pd.DataFrame(converted_file)
        new_table = pd.concat([new_table, table], ignore_index=True)
        page += 1
        requisicoes_diferentes_consultas += 1

    timestamp = datetime.now().strftime('%d-%m-%Y_%H-%M-%S')
    csv_file = os.path.join(directory_today, f"dados_coletados_{timestamp}.xlsx")
    new_table.to_excel(csv_file, index=False)

    return new_table