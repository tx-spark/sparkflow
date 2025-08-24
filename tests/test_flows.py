"""
Unit tests for pipelines.flows modules.
"""

from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest
from prefect import flow, task
from prefect.testing.utilities import prefect_test_harness


class TestPrefectFlows:
    """Test cases for Prefect flows and tasks."""

    def test_prefect_harness_setup(self):
        """Test that Prefect test harness works correctly."""

        @task
        def sample_task(x: int) -> int:
            return x * 2

        @flow
        def sample_flow(input_val: int) -> int:
            return sample_task(input_val)

        with prefect_test_harness():
            # Test the flow
            result = sample_flow(5)
            assert result == 10

    def test_task_with_exception(self):
        """Test task behavior when exceptions occur."""

        @task
        def failing_task():
            raise ValueError("Something went wrong")

        @flow
        def flow_with_failing_task():
            try:
                return failing_task()
            except ValueError as e:
                return f"Caught error: {str(e)}"

        with prefect_test_harness():
            result = flow_with_failing_task()
            assert "Caught error: Something went wrong" in result

    @patch("pipelines.utils.utils.GoogleBigQuery")
    def test_bigquery_task_mock(self, mock_bq):
        """Test a task that uses BigQuery with mocking."""

        # Mock the BigQuery client
        mock_client = Mock()
        mock_bq.return_value = mock_client

        # Mock query result
        mock_client.query.return_value = pd.DataFrame(
            {"id": [1, 2, 3], "value": ["a", "b", "c"]}
        )

        @task
        def bigquery_task():
            from pipelines.utils.utils import GoogleBigQuery

            bq = GoogleBigQuery()
            return bq.query("SELECT * FROM test_table")

        @flow
        def bigquery_flow():
            return bigquery_task()

        with prefect_test_harness():
            result = bigquery_flow()

            # Verify the result
            assert len(result) == 3
            assert list(result.columns) == ["id", "value"]

            # Verify BigQuery was called
            mock_bq.assert_called_once()

    def test_data_processing_task(self):
        """Test a data processing task."""

        @task
        def process_data(df: pd.DataFrame) -> pd.DataFrame:
            # Simple data processing
            df = df.copy()
            df["processed"] = df["value"] * 2
            return df

        @flow
        def data_processing_flow():
            input_data = pd.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})
            return process_data(input_data)

        with prefect_test_harness():
            result = data_processing_flow()

            assert "processed" in result.columns
            assert list(result["processed"]) == [20, 40, 60]


class TestDataValidation:
    """Test cases for data validation in flows."""

    def test_dataframe_validation(self):
        """Test DataFrame validation logic."""

        def validate_dataframe(df: pd.DataFrame, required_columns: list) -> bool:
            """Validate that DataFrame has required columns."""
            return all(col in df.columns for col in required_columns)

        # Test with valid DataFrame
        valid_df = pd.DataFrame(
            {"id": [1, 2, 3], "name": ["A", "B", "C"], "value": [10, 20, 30]}
        )

        assert validate_dataframe(valid_df, ["id", "name"]) is True
        assert validate_dataframe(valid_df, ["id", "missing_col"]) is False

    def test_data_quality_checks(self):
        """Test data quality validation."""

        def check_no_nulls(df: pd.DataFrame, columns: list) -> bool:
            """Check that specified columns have no null values."""
            return not df[columns].isnull().any().any()

        def check_unique_values(df: pd.DataFrame, column: str) -> bool:
            """Check that column values are unique."""
            return df[column].nunique() == len(df)

        # Test DataFrame with no nulls
        clean_df = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})

        assert check_no_nulls(clean_df, ["id", "name"]) is True
        assert check_unique_values(clean_df, "id") is True

        # Test DataFrame with nulls
        dirty_df = pd.DataFrame(
            {"id": [1, 2, None], "name": ["A", "B", "B"]}  # Duplicate name
        )

        assert check_no_nulls(dirty_df, ["id"]) is False
        assert check_unique_values(dirty_df, "name") is False


# Example integration tests for flows
@pytest.mark.integration
class TestFlowIntegration:
    """Integration tests for complete flows."""

    @pytest.mark.skip(reason="Requires actual external services")
    def test_full_data_pipeline(self):
        """Test complete data pipeline flow."""
        # This would test a full end-to-end flow
        # Marked as integration test and skipped by default
        pass

    @pytest.mark.skip(reason="Requires actual BigQuery access")
    def test_bigquery_integration_flow(self):
        """Test flow with actual BigQuery integration."""
        # This would test actual BigQuery operations
        # Marked as integration test and skipped by default
        pass
