/*
    Task 2: Dimension Table - Dates

    This date dimension table provides a comprehensive calendar dimension
    that enables time-based analysis of message data. It covers all dates
    present in the staging data plus padding for analytical flexibility.
*/

WITH date_range AS (
    SELECT 
        MIN(DATE(message_date)) AS min_date,
        MAX(DATE(message_date)) AS max_date
    FROM {{ ref('stg_telegram_messages') }}
),

date_spine AS (
    SELECT 
        generate_series AS full_date
    FROM date_range,
    GENERATE_SERIES(
        (SELECT min_date FROM date_range),
        (SELECT max_date FROM date_range),
        INTERVAL '1 day'
    ) AS generate_series
)

SELECT
    -- Surrogate key: YYYYMMDD format
    CAST(TO_CHAR(full_date, 'YYYYMMDD') AS INTEGER) AS date_key,

    -- Full date
    CAST(full_date AS DATE) AS full_date,

    -- Day attributes
    EXTRACT(DOW FROM full_date) AS day_of_week,
    TO_CHAR(full_date, 'Day') AS day_name,
    EXTRACT(DAY FROM full_date) AS day_of_month,

    -- Week attributes
    EXTRACT(WEEK FROM full_date) AS week_of_year,

    -- Month attributes
    EXTRACT(MONTH FROM full_date) AS month,
    TO_CHAR(full_date, 'Month') AS month_name,

    -- Quarter and year
    EXTRACT(QUARTER FROM full_date) AS quarter,
    EXTRACT(YEAR FROM full_date) AS year,

    -- Boolean flags
    CASE WHEN EXTRACT(DOW FROM full_date) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    CASE WHEN EXTRACT(DOW FROM full_date) BETWEEN 1 AND 5 THEN TRUE ELSE FALSE END AS is_weekday

FROM date_spine
