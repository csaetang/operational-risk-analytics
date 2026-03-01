DROP TABLE IF EXISTS marts.fact_incidents;

CREATE TABLE marts.fact_incidents AS
SELECT
    s.incident_id,
    d.department_key,
    DATE(s.incident_opened_at) AS date_key,
    s.severity_level,
    s.sla_breach_flag,
    s.downtime_minutes,
    s.estimated_cost
FROM staging.stg_incidents s
JOIN marts.dim_department d
    ON s.department_name = d.department_name;