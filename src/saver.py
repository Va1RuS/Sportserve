import logging
from pathlib import Path
import pandas as pd
from settings import USER_SCHEMA

logger = logging.getLogger(__name__)

class DataSaver:
    
    @staticmethod
    def save_to_csv(
        df: pd.DataFrame,
        filename: str,
        mode: str = 'w',
        validate: bool = True
    ) -> None:
        try:
            Path(filename).parent.mkdir(parents=True, exist_ok=True)
            
            if validate:
                DataSaver._validate_dataframe(df)
            
            df.to_csv(
                filename,
                index=False,
                encoding='utf-8',
                mode=mode,
                header=(mode == 'w')
            )
            
            logger.info(f"Successfully saved {len(df)} records to {filename}")
            
        except Exception as e:
            logger.error(f"Error saving data to CSV: {str(e)}")
            raise

    @staticmethod
    def _validate_dataframe(df: pd.DataFrame) -> None:
        expected_columns = set(USER_SCHEMA.values())
        actual_columns = set(df.columns)
        
        missing_columns = expected_columns - actual_columns
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        extra_columns = actual_columns - expected_columns
        if extra_columns:
            logger.warning(f"Extra columns will be ignored: {extra_columns}")

    @staticmethod
    def create_empty_csv(filename: str) -> None:
        header_df = pd.DataFrame(columns=list(USER_SCHEMA.values()))
        DataSaver.save_to_csv(header_df, filename, mode='w', validate=False)