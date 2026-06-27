-- Staging model: Clean and standardize YOLO detection results

WITH source AS (
    SELECT * FROM raw.image_detections
)

SELECT
    id,
    message_id,
    channel_name,
    image_path,
    detected_class,
    confidence_score,
    image_category,
    processed_at
FROM source
WHERE confidence_score > 0.3  -- Filter low-confidence detections
