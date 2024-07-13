from src.etl.extract import extract_data
from src.etl.transform import transform_data
from src.etl.load_data_postgres import load_data_postgres
from src.etl.load_data import load_data
from openpyxl.workbook import Workbook

def run_pipeline():
    print('Starting ETL...')

    # Extraction
    data = extract_data()

    # Transformation
    df = transform_data(data)

    # Load
    #load_data(df)
    load_data_postgres(df)

    print('ETL finished')

if __name__ == "__main__":
    run_pipeline()