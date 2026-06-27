/*
    Custom Test: Assert Valid Channel Types

    Ensures all channels have valid type classifications.
    This test should return 0 rows to pass.
*/

SELECT
    channel_key,
    channel_name,
    channel_type
FROM {{ ref('dim_channels') }}
WHERE channel_type NOT IN ('Pharmaceutical', 'Cosmetics', 'Medical')
