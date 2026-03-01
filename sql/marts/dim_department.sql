CREATE TABLE IF NOT EXISTS marts.dim_department (
    department_key SERIAL PRIMARY KEY,
    department_name VARCHAR(100) UNIQUE NOT NULL
);

INSERT INTO marts.dim_department (department_name)
SELECT DISTINCT department_name
FROM staging.stg_incidents
ON CONFLICT (department_name) DO NOTHING;