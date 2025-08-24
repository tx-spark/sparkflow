"""
Unit tests for pipelines.utils.utils module.
"""

import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest

from pipelines.utils.utils import FtpConnection


class TestFtpConnection:
    """Test cases for FtpConnection class."""

    @patch("pipelines.utils.utils.FTP")
    def test_ftp_connection_init(self, mock_ftp_class):
        """Test FtpConnection initialization."""
        mock_ftp_instance = Mock()
        mock_ftp_class.return_value = mock_ftp_instance

        # Test initialization with all parameters
        ftp_conn = FtpConnection(
            host="test.example.com",
            username="testuser",
            password="testpass",
            timeout=60,
        )

        assert ftp_conn.host == "test.example.com"
        assert ftp_conn.username == "testuser"
        assert ftp_conn.password == "testpass"
        assert ftp_conn.timeout == 60

        # Verify FTP was called with correct parameters
        mock_ftp_class.assert_called_once_with(timeout=60)

    @patch("pipelines.utils.utils.FTP")
    def test_ftp_connection_without_credentials(self, mock_ftp_class):
        """Test FtpConnection initialization without username/password."""
        mock_ftp_instance = Mock()
        mock_ftp_class.return_value = mock_ftp_instance

        ftp_conn = FtpConnection(host="test.example.com")

        assert ftp_conn.host == "test.example.com"
        assert ftp_conn.username is None
        assert ftp_conn.password is None
        assert ftp_conn.timeout == 120  # Default timeout


# Example test for data processing functions
class TestDataProcessing:
    """Test cases for data processing utilities."""

    def test_dataframe_processing(self, sample_dataframe):
        """Test basic dataframe operations."""
        df = sample_dataframe

        # Test that dataframe has expected structure
        assert len(df) == 3
        assert list(df.columns) == ["id", "name", "value"]
        assert df["id"].dtype == "int64"
        assert df["value"].dtype == "float64"

    def test_dataframe_filtering(self, sample_dataframe):
        """Test dataframe filtering operations."""
        df = sample_dataframe

        # Filter by value
        filtered = df[df["value"] > 15]
        assert len(filtered) == 2
        assert list(filtered["name"]) == ["Bob", "Charlie"]


# Example test for environment and configuration
class TestEnvironmentConfig:
    """Test cases for environment configuration."""

    def test_env_variables(self, mock_env):
        """Test environment variable handling."""
        # Test that mocked environment variables are available
        assert os.getenv("TEST_VAR") == "test_value"
        assert os.getenv("GCP_PROJECT") == "test-project"
        assert os.getenv("BIGQUERY_DATASET") == "test_dataset"

    def test_missing_env_variable(self):
        """Test behavior when environment variable is missing."""
        # Test that non-existent env var returns None
        assert os.getenv("NON_EXISTENT_VAR") is None

        # Test with default value
        assert os.getenv("NON_EXISTENT_VAR", "default") == "default"


# Example test for file operations
class TestFileOperations:
    """Test cases for file operations."""

    def test_file_creation(self, temp_dir):
        """Test file creation in temporary directory."""
        test_file = temp_dir / "test.txt"
        test_content = "Hello, World!"

        # Create file
        test_file.write_text(test_content)

        # Verify file exists and has correct content
        assert test_file.exists()
        assert test_file.read_text() == test_content

    def test_csv_operations(self, temp_dir, sample_dataframe):
        """Test CSV file operations."""
        csv_file = temp_dir / "test.csv"
        df = sample_dataframe

        # Write DataFrame to CSV
        df.to_csv(csv_file, index=False)

        # Read CSV back
        df_read = pd.read_csv(csv_file)

        # Verify data integrity
        pd.testing.assert_frame_equal(df, df_read)


# Example integration test (would require more setup in real scenarios)
@pytest.mark.integration
class TestIntegration:
    """Integration tests that might require external services."""

    @pytest.mark.skip(reason="Requires actual BigQuery setup")
    def test_bigquery_integration(self):
        """Example integration test for BigQuery."""
        # This would test actual BigQuery operations
        # Marked as integration test and skipped by default
        pass

    @pytest.mark.skip(reason="Requires actual Google Sheets setup")
    def test_gsheets_integration(self):
        """Example integration test for Google Sheets."""
        # This would test actual Google Sheets operations
        # Marked as integration test and skipped by default
        pass
