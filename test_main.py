import pytest
from main import get_current_weather


@pytest.fixture
def mock_requests(mocker):
    return mocker.patch("main.requests.get")


def test_get_current_weather_success(mock_requests):
    mock_get = mock_requests
    mock_response = mock_get.return_value
    mock_response.json.return_value = {"Hello": "World"}
    mock_response.status_code = 200

    result = get_current_weather("http://www.google.com")
    assert result == {"Hello": "World"}


def test_get_current_weather_failure(mock_requests):
    mock_get = mock_requests
    mock_get.return_value.status_code = 400
    result = get_current_weather("http://www.google.com")
    assert result == None
