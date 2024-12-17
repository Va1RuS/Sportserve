import argparse
import logging
from src.fetcher import DataFetcher
from src.transformer import DataTransformer
from src.saver import DataSaver
from src.db_manager import DatabaseManager
from src.visualizer import DataVisualizer

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

def analyze_data():
    
    visualizer = DataVisualizer()
    
    with DatabaseManager() as db:
        patterns = db.analyze_common_properties(min_occurrence_percent=1.0)
        
        # Create visualizations
        for category, data in patterns.items():
            visualizer.visualize_category(category, data)
            
        # Print text results
        for column, values in patterns.items():
            print(f"\nMost common {column}:")
            for value in values:
                print(f"- {value['value']}: {value['count']} occurrences ({value['percentage']}%)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['fetch', 'ingest', 'all', 'analyze'])
    args = parser.parse_args()
    
    if args.action in ['fetch', 'all']:
        fetch_data()
    if args.action in ['ingest', 'all']:
        ingest_data()
    if args.action == 'analyze':
        analyze_data()
