/*
    Task 2: Staging Model for Telegram Messages

    This model cleans and standardizes raw Telegram message data.
    Transformations applied:
    - Cast data types appropriately
    - Standardize column names
    - Filter invalid records
    - Add calculated fields (message_length, has_image flag)
    - Standardize channel names
*/

WITH source_data AS (
    SELECT * FROM {{ source('raw', 'telegram_messages') }}
),

cleaned AS (
    SELECT
        -- Primary key
        message_id,

        -- Channel information - standardized
        TRIM(LOWER(channel_name)) AS channel_name,

        -- Date casting and formatting
        CAST(message_date AS TIMESTAMP) AS message_date,

        -- Message content
        COALESCE(message_text, '') AS message_text,

        -- Engagement metrics - ensure non-negative
        GREATEST(COALESCE(views, 0), 0) AS view_count,
        GREATEST(COALESCE(forwards, 0), 0) AS forward_count,

        -- Media flags
        COALESCE(has_media, FALSE) AS has_media,
        image_path,

        -- Calculated fields
        LENGTH(COALESCE(message_text, '')) AS message_length,
        CASE 
            WHEN image_path IS NOT NULL AND image_path != '' THEN TRUE 
            ELSE FALSE 
        END AS has_image,

        -- Date dimensions for downstream use
        EXTRACT(YEAR FROM CAST(message_date AS TIMESTAMP)) AS message_year,
        EXTRACT(MONTH FROM CAST(message_date AS TIMESTAMP)) AS message_month,
        EXTRACT(DAY FROM CAST(message_date AS TIMESTAMP)) AS message_day,
        EXTRACT(DOW FROM CAST(message_date AS TIMESTAMP)) AS day_of_week

    FROM source_data
    WHERE 
        -- Filter out invalid records
        message_id IS NOT NULL
        AND message_date IS NOT NULL
        AND LENGTH(TRIM(COALESCE(message_text, ''))) > 0
)

SELECT * FROM cleaned
