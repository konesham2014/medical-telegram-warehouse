/*
    Custom Test: Assert Positive Views

    Ensures all view counts are non-negative.
    This test should return 0 rows to pass.
*/

SELECT
    message_id,
    view_count
FROM {{ ref('stg_telegram_messages') }}
WHERE view_count < 0
