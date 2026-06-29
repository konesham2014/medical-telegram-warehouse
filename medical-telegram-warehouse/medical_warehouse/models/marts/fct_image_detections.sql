/*
    Task 3: Fact Table - Image Detections

    This fact table integrates YOLO object detection results with the
    dimensional model. It provides analytical capabilities for understanding
    visual content patterns across channels.

    Grain: One row per detection per image
*/

WITH yolo_results AS (
    SELECT * FROM {{ source('staging', 'yolo_detections') }}
),

messages AS (
    SELECT * FROM {{ ref('fct_messages') }}
),

channels AS (
    SELECT * FROM {{ ref('dim_channels') }}
),

dates AS (
    SELECT * FROM {{ ref('dim_dates') }}
)

SELECT
    -- Surrogate key
    ROW_NUMBER() OVER (ORDER BY y.message_id, y.detection_id) AS detection_id,

    -- Degenerate dimension
    y.message_id::BIGINT AS message_id,

    -- Foreign keys
    c.channel_key,
    d.date_key,

    -- Detection attributes
    y.detected_class,
    y.confidence_score,
    y.image_category,

    -- Boolean flags for analysis
    y.has_person,
    y.has_product,
    y.total_detections,

    -- Metadata
    y.image_path,
    y.processed_at

FROM yolo_results y
LEFT JOIN messages m ON y.message_id = m.message_id::TEXT
LEFT JOIN channels c ON m.channel_key = c.channel_key
LEFT JOIN dates d ON m.date_key = d.date_key
