import sqlite3
from typing import List, Dict, Any
import logging
from settings import DATABASE_PATH, USER_SCHEMA, COLUMN_TYPES
import os
import pandas as pd

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, db_path: str = DATABASE_PATH):
        self.db_path = db_path
        self.connection = None
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
    def __enter__(self):
        try:
            self.connection = sqlite3.connect(self.db_path)
            return self
        except sqlite3.OperationalError as e:
            logger.error(f"Failed to open database at {self.db_path}: {str(e)}")
            raise
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            self.connection.close()
            
    def initialize_database(self):
        self._create_tables()
        self._create_indexes()
        
    def _create_tables(self):
        columns = []
        
        for col_name in USER_SCHEMA.values():
            col_type = COLUMN_TYPES.get(col_name, "TEXT")
            columns.append(f"{col_name} {col_type}")
            
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS users (
            {','.join(columns)}
        );
        """
        with self.connection:
            self.connection.execute(create_table_sql)
            
    def _create_indexes(self):
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_state ON users(state)",
            "CREATE INDEX IF NOT EXISTS idx_city ON users(city)",
            "CREATE INDEX IF NOT EXISTS idx_job_title ON users(job_title)"
        ]
        with self.connection:
            for idx in indexes:
                self.connection.execute(idx)

    def ingest_csv(self, csv_path: str, batch_size: int = 1000):
        import pandas as pd
        
        for chunk in pd.read_csv(csv_path, chunksize=batch_size):
            row_count = self.insert_users(chunk.to_dict('records'))
            logger.info(f"Inserted {row_count} records into the database")

    def insert_users(self, users_data: List[Dict[str, Any]]) -> int:
        columns = list(USER_SCHEMA.values())
        placeholders = ','.join(['?' for _ in columns])
        insert_sql = f"INSERT OR REPLACE INTO users ({','.join(columns)}) VALUES ({placeholders})"
        
        with self.connection:
            cursor = self.connection.executemany(insert_sql, [
                [user.get(col) for col in columns] for user in users_data
            ])
            return cursor.rowcount
        
    def get_record_count(self, table_name: str = "users") -> int:
        query = f"SELECT COUNT(*) FROM {table_name}"
        with self.connection:
            cursor = self.connection.execute(query)
            return cursor.fetchone()[0]

    def analyze_common_properties(self, min_occurrence_percent: float = 1.0) -> Dict[str, List[Dict[str, Any]]]:
        analysis_columns = [
            'country', 'city', 'job_title', 'subscription_plan', 
            'subscription_status', 'gender', 'payment_method'
        ]
        
        total_records = self.get_record_count()
        min_occurrences = (total_records * min_occurrence_percent) / 100
        
        results = {}
        
        for column in analysis_columns:
            query = f"""
                WITH TopResults AS (
                    SELECT 
                        {column},
                        COUNT(*) as count,
                        ROUND(COUNT(*) * 100.0 / {total_records}, 2) as percentage
                    FROM users
                    GROUP BY {column}
                    HAVING COUNT(*) >= ?
                    ORDER BY count DESC
                    LIMIT 10
                )

                SELECT * FROM TopResults
            """
            
            with self.connection:
                cursor = self.connection.execute(query, (min_occurrences,))
                patterns = [
                    {
                        'value': row[0],
                        'count': row[1],
                        'percentage': row[2]
                    }
                    for row in cursor.fetchall()
                ]
                if patterns:
                    results[column] = patterns
        
        return results

    def get_users_dataframe(self, limit: int = 1000) -> pd.DataFrame:
        query = """
        SELECT
            first_name,
            last_name,
            gender,
            date_of_birth,
            job_title,
            key_skill,
            city,
            state,
            latitude,
            longitude,
            subscription_plan,
            subscription_status,
            payment_method,
            subscription_term
        FROM users
        LIMIT ?
        """
        
        with self.connection:
            df = pd.read_sql_query(query, self.connection, params=(limit,))
        
        return df
