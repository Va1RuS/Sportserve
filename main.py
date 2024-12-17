import argparse
import logging
from src.fetcher import DataFetcher
from src.transformer import DataTransformer
from src.saver import DataSaver
from src.db_manager import DatabaseManager
from src.visualizer import DataVisualizer
from src.network_visualizer import NetworkVisualizer

from settings import CSV_PATH, VISUALIZATIONS_PATH
import os

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

def analyze_common_properties():
    
    visualizer = DataVisualizer(output_dir=os.path.join(VISUALIZATIONS_PATH, "common_properties"))
    
    with DatabaseManager() as db:
        patterns = db.analyze_common_properties(min_occurrence_percent=1.0)
        
        for category, data in patterns.items():
            visualizer.visualize_category(category, data)
            
        for column, values in patterns.items():
            print(f"\nMost common {column}:")
            for value in values:
                print(f"- {value['value']}: {value['count']} occurrences ({value['percentage']}%)")

def analyze_user_similarities():
    with DatabaseManager() as db:
        raw_df = db.get_users_dataframe(limit=1000)
        df = DataTransformer.prepare_for_similarity(raw_df)
        visualizer = NetworkVisualizer(output_dir=os.path.join(VISUALIZATIONS_PATH, "networks"))
        visualizer.analyze_similarities(df)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['fetch', 'ingest', 'all', 'analyze'])
    args = parser.parse_args()
    
    if args.action in ['fetch', 'all']:
        fetch_data()
    if args.action in ['ingest', 'all']:
        ingest_data()
    if args.action == 'analyze':
        analyze_common_properties()
        analyze_user_similarities()
