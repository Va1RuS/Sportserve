import logging
from typing import List, Dict, Any
import pandas as pd
from settings import USER_SCHEMA
from sentence_transformers import SentenceTransformer


logger = logging.getLogger(__name__)

class DataTransformer:
    
    @staticmethod
    def transform_user_data(users_data: List[Dict[str, Any]]) -> pd.DataFrame:
        try:
            df = pd.json_normalize(users_data)
            df = df[list(USER_SCHEMA.keys())].rename(columns=USER_SCHEMA)
            df = DataTransformer._clean_data(df)
            return df
            
        except Exception as e:
            logger.error(f"Error transforming data: {str(e)}")
            raise

    @staticmethod
    def _clean_data(df: pd.DataFrame) -> pd.DataFrame:
        df = df.dropna()
        df = df.drop_duplicates()
        
        try:
            df['date_of_birth'] = pd.to_datetime(df['date_of_birth'])
        except Exception as e:
            logger.warning(f"Could not convert date_of_birth to datetime: {e}")
        
        return df 
    
    @staticmethod
    def prepare_for_similarity(df: pd.DataFrame) -> pd.DataFrame:
        model = SentenceTransformer('all-MiniLM-L6-v2')

        try:
            df['age'] = (pd.Timestamp.now() - pd.to_datetime(df['date_of_birth'])).dt.days / 365.25
            df['full_name'] = df['first_name'] + ' ' + df['last_name']
            
            cat_columns = ['gender', 'city', 'state', 
                          'subscription_plan', 'subscription_status', 
                          'payment_method', 'subscription_term']
            
            def create_user_description(row):
                description = f"A {int(row['age'])} year old {row['gender']} "
                description += f"working as {row['job_title']} with {row['key_skill']} skills. "
                description += f"Located in {row['city']}, {row['state']}. "
                description += f"Has a {row['subscription_plan']} {row['subscription_term']} subscription "
                description += f"which is {row['subscription_status']}, paid via {row['payment_method']}."
                return description
            
            df['user_description'] = df.apply(create_user_description, axis=1)
            
            df['user_embedding'] = model.encode(df['user_description'].tolist()).tolist()
            

            columns_to_drop = ['date_of_birth', 'job_title', 'key_skill', 'latitude', 'longitude', 'age', 'first_name', 'last_name'] + cat_columns
            df = df.drop(columns_to_drop, axis=1)

            return df

        except Exception as e:
            logger.error(f"Error preparing data for similarity matching: {str(e)}")
            raise
    

    