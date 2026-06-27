/*
    Task 2: Dimension Table - Channels

    This dimension table contains descriptive attributes about each Telegram channel.
    It provides a single source of truth for channel information and enables
    analysis of channel performance and characteristics.

    Surrogate key: channel_key (auto-incrementing integer)
*/

WITH channel_stats AS (
    SELECT
        channel_name,
        MIN(message_date) AS first_post_date,
        MAX(message_date) AS last_post_date,
        COUNT(*) AS total_posts,
        ROUND(AVG(view_count), 2) AS avg_views,
        SUM(CASE WHEN has_image THEN 1 ELSE 0 END) AS total_images
    FROM {{ ref('stg_telegram_messages') }}
    GROUP BY channel_name
),

channel_types AS (
    SELECT
        channel_name,
        CASE 
            WHEN channel_name LIKE '%pharma%' OR channel_name LIKE '%med%' OR channel_name LIKE '%health%' THEN 'Pharmaceutical'
            WHEN channel_name LIKE '%cosmetic%' OR channel_name LIKE '%beauty%' THEN 'Cosmetics'
            ELSE 'Medical'
        END AS channel_type
    FROM channel_stats
)

SELECT
    ROW_NUMBER() OVER (ORDER BY cs.channel_name) AS channel_key,
    cs.channel_name,
    ct.channel_type,
    cs.first_post_date,
    cs.last_post_date,
    cs.total_posts,
    cs.avg_views,
    cs.total_images
FROM channel_stats cs
LEFT JOIN channel_types ct ON cs.channel_name = ct.channel_name
