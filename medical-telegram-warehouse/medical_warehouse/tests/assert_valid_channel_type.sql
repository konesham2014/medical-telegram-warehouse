-- Custom Test: Ensure channel types are valid

SELECT channel_name, channel_type
FROM {{ ref('dim_channels') }}
WHERE channel_type NOT IN ('Pharmaceutical', 'Cosmetics', 'Medical')
