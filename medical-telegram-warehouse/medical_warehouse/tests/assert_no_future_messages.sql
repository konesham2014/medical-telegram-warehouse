/*
    Custom Test: Assert No Future Messages

    Ensures no messages have dates in the future.
    This test should return 0 rows to pass.
*/

SELECT
    message_id,
    message_date
FROM {{ ref('stg_telegram_messages') }}
WHERE message_date > CURRENT_TIMESTAMP
