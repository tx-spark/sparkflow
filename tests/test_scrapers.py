"""
Unit tests for pipelines.scrapers.official_contact_info module.
"""

import os
import tempfile
from unittest.mock import MagicMock, Mock, patch

import pytest
import requests
from bs4 import BeautifulSoup

from pipelines.scrapers.official_contact_info import (
    HOUSE_URL,
    SENATE_URL,
    get_senate_pages,
)


class TestOfficialContactInfo:
    """Test cases for official contact info scraper."""

    @patch("pipelines.scrapers.official_contact_info.requests.get")
    @patch("pipelines.scrapers.official_contact_info.os.makedirs")
    @patch("builtins.open", create=True)
    def test_get_senate_pages(self, mock_open, mock_makedirs, mock_requests_get):
        """Test get_senate_pages function with mocked requests."""

        # Mock HTML content for senate page
        mock_html = """
        <html>
            <body>
                <div class="memlist">
                    <div class="mempicdiv">
                        <a href="/member?d=1">
                            <img src="/images/senator1.jpg" alt="Senator John Doe">
                        </a>
                    </div>
                    <div class="mempicdiv">
                        <a href="/member?d=2">
                            <img src="/images/senator2.jpg" alt="Senator Jane Smith">
                        </a>
                    </div>
                </div>
            </body>
        </html>
        """

        # Mock image content
        mock_image_content = b"fake_image_data"

        # Configure mock responses
        mock_page_response = Mock()
        mock_page_response.text = mock_html

        mock_img_response = Mock()
        mock_img_response.status_code = 200
        mock_img_response.content = mock_image_content

        # Configure requests.get to return different responses
        mock_requests_get.side_effect = [
            mock_page_response,  # First call for main page
            mock_img_response,  # First image
            mock_img_response,  # Second image
        ]

        # Configure file writing mock
        mock_file = MagicMock()
        mock_open.return_value.__enter__.return_value = mock_file

        # Call the function
        result = get_senate_pages()

        # Assertions
        assert len(result) == 2
        assert result[0]["name"] == "Senator John Doe"
        assert result[1]["name"] == "Senator Jane Smith"
        assert "senate.texas.gov/member?d=1" in result[0]["page_url"]
        assert "senate.texas.gov/member?d=2" in result[1]["page_url"]

        # Verify directory creation was called
        mock_makedirs.assert_called_once_with(
            "./data/temp_data/Contact Info/Official Images", exist_ok=True
        )

        # Verify files were written
        assert mock_file.write.call_count == 2
        mock_file.write.assert_called_with(mock_image_content)

    @patch("pipelines.scrapers.official_contact_info.requests.get")
    def test_get_senate_pages_network_error(self, mock_requests_get):
        """Test get_senate_pages when network request fails."""

        # Mock a network error
        mock_requests_get.side_effect = requests.exceptions.ConnectionError(
            "Network error"
        )

        # The function should raise an exception
        with pytest.raises(requests.exceptions.ConnectionError):
            get_senate_pages()

    @patch("pipelines.scrapers.official_contact_info.requests.get")
    @patch("pipelines.scrapers.official_contact_info.os.makedirs")
    def test_get_senate_pages_empty_response(self, mock_makedirs, mock_requests_get):
        """Test get_senate_pages with empty HTML response."""

        # Mock empty HTML content
        mock_html = "<html><body></body></html>"

        mock_response = Mock()
        mock_response.text = mock_html
        mock_requests_get.return_value = mock_response

        # This should raise an AttributeError when trying to find the memlist div
        with pytest.raises(AttributeError):
            get_senate_pages()

    def test_constants(self):
        """Test that URL constants are properly defined."""
        assert HOUSE_URL == "https://www.house.texas.gov/members"
        assert SENATE_URL == "https://senate.texas.gov/members.php"

    @patch("pipelines.scrapers.official_contact_info.requests.get")
    @patch("pipelines.scrapers.official_contact_info.os.makedirs")
    @patch("builtins.open", create=True)
    def test_get_senate_pages_image_download_failure(
        self, mock_open, mock_makedirs, mock_requests_get
    ):
        """Test behavior when image download fails."""

        # Mock HTML content
        mock_html = """
        <html>
            <body>
                <div class="memlist">
                    <div class="mempicdiv">
                        <a href="/member?d=1">
                            <img src="/images/senator1.jpg" alt="Senator John Doe">
                        </a>
                    </div>
                </div>
            </body>
        </html>
        """

        # Mock responses
        mock_page_response = Mock()
        mock_page_response.text = mock_html

        mock_img_response = Mock()
        mock_img_response.status_code = 404  # Image not found

        mock_requests_get.side_effect = [
            mock_page_response,  # Main page
            mock_img_response,  # Failed image
        ]

        # Call the function
        result = get_senate_pages()

        # Should still return senator info even if image download fails
        assert len(result) == 1
        assert result[0]["name"] == "Senator John Doe"

        # File should not be written for failed download
        mock_open.assert_not_called()


class TestBeautifulSoupParsing:
    """Test cases for HTML parsing logic."""

    def test_parse_senator_info_from_html(self):
        """Test parsing senator information from HTML."""
        html = """
        <div class="mempicdiv">
            <a href="/member?d=15">
                <img src="/images/senator15.jpg" alt="Senator Test Name">
            </a>
        </div>
        """

        soup = BeautifulSoup(html, "html.parser")
        member_div = soup.find("div", class_="mempicdiv")

        # Extract information like the function does
        member_link = member_div.find("a")["href"]
        img = member_div.find("img")
        name = img["alt"].strip()
        img_src = img["src"]

        assert member_link == "/member?d=15"
        assert name == "Senator Test Name"
        assert img_src == "/images/senator15.jpg"

    def test_parse_malformed_html(self):
        """Test parsing with malformed HTML."""
        html = """
        <div class="mempicdiv">
            <a>
                <img alt="Senator No Link">
            </a>
        </div>
        """

        soup = BeautifulSoup(html, "html.parser")
        member_div = soup.find("div", class_="mempicdiv")

        # Should raise KeyError when href is missing
        with pytest.raises(KeyError):
            member_link = member_div.find("a")["href"]
