from typing import Dict

API_URL = "https://random-data-api.com/api/v2/users"
BATCH_SIZE = 100
TOTAL_RECORDS = 1000
RETRY_ATTEMPTS = 2
BACKOFF_FACTOR = 1
RATE_LIMIT_DELAY = 1

DATABASE_PATH = "data/users.db"
PRIMARY_KEY_COL = "id"

CSV_PATH = "data/users.csv"

VISUALIZATIONS_PATH = "visualizations"

USER_SCHEMA: Dict[str, str] = {
    "id": "id",
    "uid": "uid",
    "password": "password",
    "first_name": "first_name",
    "last_name": "last_name",
    "username": "username",
    "email": "email",
    "avatar": "avatar",
    "gender": "gender",
    "phone_number": "phone_number",
    "social_insurance_number": "social_insurance_number",
    "date_of_birth": "date_of_birth",
    "employment.title": "job_title",
    "employment.key_skill": "key_skill",
    "address.city": "city",
    "address.street_name": "street_name",
    "address.street_address": "street_address",
    "address.zip_code": "zip_code",
    "address.state": "state",
    "address.country": "country",
    "address.coordinates.lat": "latitude",
    "address.coordinates.lng": "longitude",
    "credit_card.cc_number": "credit_card_number",
    "subscription.plan": "subscription_plan",
    "subscription.status": "subscription_status",
    "subscription.payment_method": "payment_method",
    "subscription.term": "subscription_term"
}

COLUMN_TYPES = {
    "id": "INTEGER PRIMARY KEY",
    "latitude": "REAL",
    "longitude": "REAL",
}
