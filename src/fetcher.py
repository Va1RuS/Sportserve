import requests
import logging
from typing import List, Dict, Any, Callable, Optional
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import time

from settings import (
    API_URL,
    BATCH_SIZE,
    TOTAL_RECORDS,
    RETRY_ATTEMPTS,
    BACKOFF_FACTOR,
    RATE_LIMIT_DELAY
)

logger = logging.getLogger(__name__)

class DataFetcher:
    def __init__(self):
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry_strategy = Retry(
            total=RETRY_ATTEMPTS,
            backoff_factor=BACKOFF_FACTOR,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def fetch_random_users(self, chunk_callback: Optional[Callable] = None) -> Optional[List[Dict[str, Any]]]:
        all_users = []
        num_batches = (TOTAL_RECORDS + BATCH_SIZE - 1) // BATCH_SIZE

        for batch in range(num_batches):
            try:
                logger.info(f"Fetching batch {batch + 1}/{num_batches}")
                response = self.session.get(
                    API_URL,
                    params={"size": min(BATCH_SIZE, TOTAL_RECORDS - len(all_users))}
                )
                response.raise_for_status()
                batch_data = response.json()
                
                if chunk_callback:
                    chunk_callback(batch_data)
                else:
                    all_users.extend(batch_data)
                
                if len(all_users) >= TOTAL_RECORDS:
                    break
                
                time.sleep(RATE_LIMIT_DELAY)
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching data: {str(e)}")
                continue

        return all_users if not chunk_callback else None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()