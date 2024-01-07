from datetime import timedelta, datetime
import os
import json
from airflow.decorators import dag, task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator


TABLES_CREATION_QUERY = """CREATE TABLE IF NOT EXISTS job (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title VARCHAR(225),
    industry VARCHAR(225),
    description TEXT,
    employment_type VARCHAR(125),
    date_posted DATE
);

CREATE TABLE IF NOT EXISTS company (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    name VARCHAR(225),
    link TEXT,
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS education (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    required_credential VARCHAR(225),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS experience (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    months_of_experience INTEGER,
    seniority_level VARCHAR(25),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS salary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    currency VARCHAR(3),
    min_value NUMERIC,
    max_value NUMERIC,
    unit VARCHAR(12),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS location (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    country VARCHAR(60),
    locality VARCHAR(60),
    region VARCHAR(60),
    postal_code VARCHAR(25),
    street_address VARCHAR(225),
    latitude NUMERIC,
    longitude NUMERIC,
    FOREIGN KEY (job_id) REFERENCES job(id)
)
"""

extracted_folder = "/staging/extracted"

import pandas as pd
@task()
def extract():
    """Extract data from jobs.csv."""
    data = pd.read_csv('source/jobs.csv')

    return data

@task()
def transform(data: str):
    """Transform the extracted data from text files to json."""
    data = "staging/extracted"
    transformed = "staging/transformed"
    os.makedirs(transformed, exist_ok=True)

    for file_name in os.listdir(data):
        if file_name.endswith(".txt"):
            input_file_path = os.path.join(data, file_name)
            output_file_path = os.path.join(transformed, f"transformed_{file_name[:-4]}.json")

            with open(input_file_path, 'r') as input_file:
                job_description = input_file.read()

                # Replace the following line with your actual code for cleaning and transforming job description
                transformed_data = {
                    "job": {
                        "title": "Sample Title",
                        "industry": "Sample Industry",
                        "description": job_description,
                        "employment_type": "Full-Time",
                        "date_posted": str(datetime.now()),
                    },
                    "company": {
                        "name": "Sample Company",
                        "link": "https://www.samplecompany.com",
                    },
                    "education": {
                        "required_credential": "Sample Credential",
                    },
                    "experience": {
                        "months_of_experience": 12,
                        "seniority_level": "Mid-Level",
                    },
                    "salary": {
                        "currency": "USD",
                        "min_value": 50000,
                        "max_value": 80000,
                        "unit": "per year",
                    },
                    "location": {
                        "country": "Sample Country",
                        "locality": "Sample Locality",
                        "region": "Sample Region",
                        "postal_code": "12345",
                        "street_address": "123 Main St",
                        "latitude": 40.7128,
                        "longitude": -74.0060,
                    },
                }  # Placeholder for actual transformation logic

            with open(output_file_path, 'w') as output_file:
                json.dump(transformed, output_file, indent=2)

    return transformed



@task()
def load(transformed: str):
    """Load transformed data to SQLite database."""
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')

    for file_name in os.listdir(transformed):
        if file_name.endswith(".json"):
            file_path = os.path.join(transformed, file_name)

            with open(file_path, 'r') as file:
                transformed = json.load(file)

                # Replace the following line with your actual code for data loading
                connection = sqlite_hook.get_conn()
                cursor = connection.cursor()
                # Placeholder for actual data loading logic
                cursor.execute("""
                    INSERT INTO job (title, industry, description, employment_type, date_posted)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    transformed['job']['title'],
                    transformed['job']['industry'],
                    transformed['job']['description'],
                    transformed['job']['employment_type'],
                    transformed['job']['date_posted'],
                ))
                connection.commit()
                connection.close()


DAG_DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
}

@dag(
    dag_id="etl_dag",
    description="ETL LinkedIn job posts",
    tags=["etl"],
    schedule="@daily",
    start_date=datetime(2024, 1, 2),
    catchup=False,
    default_args=DAG_DEFAULT_ARGS
)
def etl_dag():
    """ETL pipeline"""

    create_tables = SqliteOperator(
        task_id="create_tables",
        sqlite_conn_id="sqlite_default",
        sql=TABLES_CREATION_QUERY
    )

    data = extract()
    transformed = transform(data)
    load(transformed)

    create_tables >> extract() >> transform() >> load()

etl_dag()
