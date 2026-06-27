"""
Task 1 (continued): Load Raw Data to PostgreSQL
Reads JSON files from data lake and loads them into raw schema in PostgreSQL.

Usage:
    python src/load_raw_to_postgres.py
"""

import os
import json
import logging
import glob
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

load_dotenv()

# Database configuration
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'medical_warehouse')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password')

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / 'data' / 'raw' / 'telegram_messages'
LOG_DIR = BASE_DIR / 'logs'
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / f"load_raw_{datetime.now().strftime('%Y-%m-%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def get_db_connection():
    """Create and return a PostgreSQL database connection."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise


def create_raw_table():
    """Create the raw.telegram_messages table if it doesn't exist."""
    create_table_sql = """
    CREATE SCHEMA IF NOT EXISTS raw;

    CREATE TABLE IF NOT EXISTS raw.telegram_messages (
        id SERIAL PRIMARY KEY,
        message_id BIGINT NOT NULL,
        channel_name VARCHAR(255),
        message_date TIMESTAMP,
        message_text TEXT,
        views INTEGER DEFAULT 0,
        forwards INTEGER DEFAULT 0,
        has_media BOOLEAN DEFAULT FALSE,
        image_path VARCHAR(500),
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(create_table_sql)
        conn.commit()
        logger.info("Raw table created successfully")
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        conn.rollback()
    finally:
        conn.close()


def load_json_files():
    """Read all JSON files from data lake and return combined records."""
    all_records = []

    json_pattern = str(DATA_DIR / "**" / "*.json")
    json_files = glob.glob(json_pattern, recursive=True)

    logger.info(f"Found {len(json_files)} JSON files to process")

    for file_path in json_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    all_records.extend(data)
                else:
                    all_records.append(data)
            logger.info(f"Loaded {len(data) if isinstance(data, list) else 1} records from {file_path}")
        except Exception as e:
            logger.error(f"Error reading {file_path}: {e}")

    return all_records


def insert_records(records: list):
    """Insert records into PostgreSQL raw table."""
    if not records:
        logger.warning("No records to insert")
        return

    insert_sql = """
    INSERT INTO raw.telegram_messages 
    (message_id, channel_name, message_date, message_text, views, forwards, has_media, image_path)
    VALUES %s
    ON CONFLICT (message_id) DO NOTHING;
    """

    # Prepare data tuples
    values = []
    for record in records:
        values.append((
            record.get('message_id'),
            record.get('channel_name'),
            record.get('message_date'),
            record.get('message_text', ''),
            record.get('views', 0),
            record.get('forwards', 0),
            record.get('has_media', False),
            record.get('image_path')
        ))

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, values, page_size=1000)
        conn.commit()
        logger.info(f"Successfully inserted {len(values)} records into raw.telegram_messages")
    except Exception as e:
        logger.error(f"Error inserting records: {e}")
        conn.rollback()
    finally:
        conn.close()


def main():
    """Main entry point."""
    logger.info("Starting raw data load process")

    # Create table
    create_raw_table()

    # Load JSON files
    records = load_json_files()

    # Insert into database
    insert_records(records)

    logger.info("Raw data load complete")


if __name__ == '__main__':
    main()
