CREATE TABLE staging.stg_incidents (
    incident_id SERIAL PRIMARY KEY,
    incident_opened_at TIMESTAMP NOT NULL,
    incident_resolved_at TIMESTAMP,
    department_name VARCHAR(100),
    severity_level VARCHAR(50),
    sla_breach_flag BOOLEAN,
    downtime_minutes INTEGER,
    estimated_cost NUMERIC(12,2)
);