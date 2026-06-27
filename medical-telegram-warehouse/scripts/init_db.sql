-- Initialize PostgreSQL database for Medical Telegram Warehouse
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

CREATE TABLE IF NOT EXISTS raw.telegram_messages (
    message_id BIGINT,
    channel_name VARCHAR(255),
    message_date TIMESTAMP,
    message_text TEXT,
    views INTEGER,
    forwards INTEGER,
    has_media BOOLEAN,
    image_path VARCHAR(500),
    scraped_at TIMESTAMP,
    PRIMARY KEY (message_id, channel_name)
);

CREATE TABLE IF NOT EXISTS raw.image_detections (
    id SERIAL PRIMARY KEY,
    message_id BIGINT,
    channel_name VARCHAR(255),
    image_path VARCHAR(500),
    detected_class VARCHAR(100),
    confidence_score DECIMAL(5,4),
    image_category VARCHAR(50),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
