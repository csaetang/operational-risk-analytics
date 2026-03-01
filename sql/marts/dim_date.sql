CREATE TABLE IF NOT EXISTS marts.dim_date (
    date_key DATE PRIMARY KEY,
    year INT,
    month INT,
    day INT,
    quarter INT,
    day_of_week INT
);

INSERT INTO marts.dim_date
SELECT DISTINCT
    DATE(incident_opened_at) AS date_key,
    EXTRACT(YEAR FROM incident_opened_at),
    EXTRACT(MONTH FROM incident_opened_at),
    EXTRACT(DAY FROM incident_opened_at),
    EXTRACT(QUARTER FROM incident_opened_at),
    EXTRACT(DOW FROM incident_opened_at)
FROM staging.stg_incidents
ON CONFLICT (date_key) DO NOTHING;