import argparse
import logging
import pandas as pd
from src.fetcher import DataFetcher
from src.transformer import DataTransformer
from src.saver import DataSaver
from src.db_manager import DatabaseManager
from settings import CSV_PATH

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_data():
    fetcher = DataFetcher()
    transformer = DataTransformer()
    saver = DataSaver()
    saver.create_empty_csv(CSV_PATH)
    
    def process_chunk(chunk_data):
        transformed_df = transformer.transform_user_data(chunk_data)
        saver.save_to_csv(transformed_df, CSV_PATH, mode='a')
    
    fetcher.fetch_random_users(chunk_callback=process_chunk)

def ingest_data():
    with DatabaseManager() as db:
        db.initialize_database()
        db.ingest_csv(CSV_PATH)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['fetch', 'ingest', 'all'])
    args = parser.parse_args()
    
    if args.action in ['fetch', 'all']:
        fetch_data()
    if args.action in ['ingest', 'all']:
        ingest_data()
