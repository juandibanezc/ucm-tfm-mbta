"""Fixtures for landing pipeline tests"""
import pytest


@pytest.fixture
def mock_response():
    """Mock response for requests"""
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

        def raise_for_status(self):
            if self.status_code != 200:
                raise Exception("API Error")

    return MockResponse
