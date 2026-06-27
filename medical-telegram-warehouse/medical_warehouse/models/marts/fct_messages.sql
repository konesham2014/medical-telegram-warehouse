/*
    Task 2: Fact Table - Messages

    This fact table contains the quantitative data about each Telegram message.
    It references dimension tables via foreign keys and contains the metrics
    used for analytical queries.

    Grain: One row per message
*/

WITH messages AS (
    SELECT * FROM {{ ref('stg_telegram_messages') }}
),

channels AS (
    SELECT * FROM {{ ref('dim_channels') }}
)

SELECT
    -- Degenerate dimension: message_id
    m.message_id,

    -- Foreign keys to dimension tables
    c.channel_key,
    CAST(TO_CHAR(DATE(m.message_date), 'YYYYMMDD') AS INTEGER) AS date_key,

    -- Descriptive attributes
    m.message_text,
    m.message_length,

    -- Metrics / Measures
    m.view_count,
    m.forward_count,

    -- Flags for analysis
    m.has_image AS has_image_flag,
    m.has_media AS has_media_flag,

    -- Metadata
    m.image_path,
    m.message_date AS original_timestamp

FROM messages m
LEFT JOIN channels c ON m.channel_name = c.channel_name
