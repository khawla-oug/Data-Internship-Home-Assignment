import os
import json
from unittest.mock import Mock, patch
import pandas as pd
import pytest
from dags import etl_dag, extract, transform, load

@pytest.fixture
def mock_read_csv(monkeypatch):
    def mock_data_frame(*args, **kwargs):
        return pd.DataFrame({
            "job_title": ["Title1", "Title2"],
            "job_industry": ["Industry1", "Industry2"],
            "job_description": ["Description1", "Description2"],
            # Add other columns as needed
        })

    monkeypatch.setattr(pd, 'read_csv', mock_data_frame)

def test_extract_job(mock_read_csv):
    extracted_folder = extract()

    assert os.path.exists(extracted_folder)
    assert os.path.isdir(extracted_folder)

    # Add more assertions as needed

def test_transform_job():
    data_folder = "/staging/extracted"
    # You may want to use sample data for testing
    sample_data = pd.DataFrame({
        "job_title": ["Title1", "Title2"],
        "job_industry": ["Industry1", "Industry2"],
        "job_description": ["Description1", "Description2"],
        # Add other columns as needed
    })

    with patch('your_script.json.dump') as mock_json_dump:
        transformed_folder = transform(data_folder, sample_data)

    assert os.path.exists(transformed_folder)
    assert os.path.isdir(transformed_folder)

    # Add more assertions as needed

def test_load():
    transformed_folder = "/staging/transformed"
    # You may want to use sample data for testing
    sample_data = {
    "job": {
        "title": "Sample Title",
        "industry": "Sample Industry",
        "description": "Sample Description",
        "employment_type": "Full-Time",
        "date_posted": "2024-01-01",  # Replace with an actual date
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
}


    with patch('etl.sqlite_hook.SqliteHook') as mock_sqlite_hook:
        load(transformed_folder, sample_data)

