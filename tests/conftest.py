"""
Pytest configuration and fixtures for sparkflow tests.
"""

import os
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def mock_env():
    """Mock environment variables for testing."""
    with patch.dict(
        os.environ,
        {
            "TEST_VAR": "test_value",
            "GCP_PROJECT": "test-project",
            "BIGQUERY_DATASET": "test_dataset",
        },
    ):
        yield


@pytest.fixture
def sample_dataframe():
    """Create a sample pandas DataFrame for testing."""
    import pandas as pd

    return pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "value": [10.5, 20.3, 30.1],
        }
    )


@pytest.fixture
def mock_bigquery():
    """Mock BigQuery client for testing."""
    with patch("pipelines.utils.utils.GoogleBigQuery") as mock_bq:
        yield mock_bq


@pytest.fixture
def mock_gspread():
    """Mock Google Sheets client for testing."""
    with patch("pipelines.utils.utils.gspread") as mock_gs:
        yield mock_gs
