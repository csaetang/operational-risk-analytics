import pandas as pd
import numpy as np
import psycopg2
from datetime import datetime, timedelta
import random
import os
from dotenv import load_dotenv

# ----------------------------
# Load environment variables
# ----------------------------
load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# ----------------------------
# Configuration
# ----------------------------
NUM_ROWS = 50000
START_DATE = datetime(2025, 1, 1)

departments = {
    "IT": 0.30,
    "Operations": 0.35,
    "Finance": 0.15,
    "HR": 0.10,
    "Compliance": 0.10
}

severity_weights = {
    "Low": 0.40,
    "Medium": 0.30,
    "High": 0.20,
    "Critical": 0.10
}

severity_cost_multiplier = {
    "Low": 40,
    "Medium": 80,
    "High": 200,
    "Critical": 500
}

# ----------------------------
# Helper Functions
# ----------------------------

def weighted_choice(weight_dict):
    items = list(weight_dict.keys())
    weights = list(weight_dict.values())
    return random.choices(items, weights=weights)[0]


def generate_timestamp():
    minutes_offset = random.randint(0, 365 * 24 * 60)
    base_time = START_DATE + timedelta(minutes=minutes_offset)

    # Business hour bias
    if 8 <= base_time.hour <= 18:
        return base_time
    else:
        if random.random() < 0.5:
            hour = random.randint(8, 18)
            return base_time.replace(hour=hour)
        return base_time


def generate_resolution_minutes(severity):
    if severity == "Low":
        return int(np.random.gamma(2, 30))
    elif severity == "Medium":
        return int(np.random.gamma(2, 60))
    elif severity == "High":
        return int(np.random.gamma(2, 120))
    else:  # Critical
        return int(np.random.gamma(2, 240))


# ----------------------------
# Data Generation
# ----------------------------

def generate_data(n):
    records = []

    for _ in range(n):
        department = weighted_choice(departments)
        severity = weighted_choice(severity_weights)

        opened_at = generate_timestamp()
        resolution_minutes = generate_resolution_minutes(severity)
        resolved_at = opened_at + timedelta(minutes=resolution_minutes)

        sla_breach = resolution_minutes > 180
        estimated_cost = resolution_minutes * severity_cost_multiplier[severity]

        records.append([
            opened_at,
            resolved_at,
            department,
            severity,
            sla_breach,
            resolution_minutes,
            estimated_cost
        ])

    df = pd.DataFrame(records, columns=[
        "incident_opened_at",
        "incident_resolved_at",
        "department_name",
        "severity_level",
        "sla_breach_flag",
        "downtime_minutes",
        "estimated_cost"
    ])

    return df


# ----------------------------
# Load to PostgreSQL
# ----------------------------

def load_to_postgres(df):
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

    cur = conn.cursor()

    # Clear existing data
    cur.execute("TRUNCATE staging.stg_incidents;")

    insert_query = """
        INSERT INTO staging.stg_incidents
        (incident_opened_at, incident_resolved_at, department_name,
         severity_level, sla_breach_flag, downtime_minutes, estimated_cost)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
    """

    for row in df.itertuples(index=False):
        cur.execute(insert_query, tuple(row))

    conn.commit()
    cur.close()
    conn.close()


# ----------------------------
# Main Execution
# ----------------------------

if __name__ == "__main__":
    print("Generating incident data...")
    df = generate_data(NUM_ROWS)

    print("Loading into PostgreSQL...")
    load_to_postgres(df)

    print("50,000 incidents generated and loaded successfully.")