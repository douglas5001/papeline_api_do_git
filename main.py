import schedule
import time
from datetime import datetime
from src.etl.extract import extract_data
from src.etl.transform import transform_data
from src.etl.load_data_postgres import load_data_postgres
from src.etl.load_data import load_data
from openpyxl.workbook import Workbook
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_pipeline():
    logging.info('Starting ETL...')

    # Extraction
    data = extract_data()

    # Transformation
    df = transform_data(data)

    # Load
    load_data(df)
    # load_data_postgres(df)

    logging.info('ETL finished')

def job():
    logging.info(f"Job started at {datetime.now()}...")
    run_pipeline()
    logging.info(f"Job finished at {datetime.now()}.")

# Agenda a função job para executar todos os dias às 20:00
schedule.every().day.at("20:26").do(job)

if __name__ == "__main__":
    logging.info("Scheduler started. Running jobs at 20:00 every day.")
    try:
        # Loop infinito para manter o script rodando
        while True:
            schedule.run_pending()
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        logging.info("Scheduler stopped.")
        pass
