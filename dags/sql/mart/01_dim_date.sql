CREATE TABLE IF NOT EXISTS mart.dim_date (
    date_sk INT PRIMARY KEY,
    full_date DATE,
    year INT,
    month INT,
    day INT,
    day_of_week INT,
    day_name TEXT,
    month_name TEXT,
    is_weekend BOOLEAN
);

INSERT INTO mart.dim_date
SELECT
    TO_CHAR(datum, 'yyyymmdd')::INT AS date_sk,
    datum AS full_date,
    EXTRACT(YEAR FROM datum) AS year,
    EXTRACT(MONTH FROM datum) AS month,
    EXTRACT(DAY FROM datum) AS day,
    EXTRACT(ISODOW FROM datum) AS day_of_week,
    TO_CHAR(datum, 'Day') AS day_name,
    TO_CHAR(datum, 'Month') AS month_name,
    CASE WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM (SELECT '2016-01-01'::DATE + SEQUENCE.DAY AS datum
      FROM GENERATE_SERIES(0, 365*5) AS SEQUENCE(DAY)
      GROUP BY SEQUENCE.DAY) DQ
ON CONFLICT (date_sk) DO NOTHING;
