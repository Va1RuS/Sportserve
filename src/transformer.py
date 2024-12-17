import logging
from typing import List, Dict, Any
import pandas as pd
from settings import USER_SCHEMA

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