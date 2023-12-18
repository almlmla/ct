import pytest
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from datetime import datetime
from ctetl.ct_helpers import get_request_parameters
from ctetl.ct_helpers import load_db_credentials
from ctetl.ct_helpers import isoformat_to_seconds


def test_get_request_parameters_with_api_key(monkeypatch):
    monkeypatch.setenv("CT_KEY", "your_api_key")

    headers, api_key = get_request_parameters()

    assert os.environ.get("CT_KEY") == "your_api_key"
    assert "User-Agent" in headers
    assert api_key == "your_api_key"


def test_get_request_parameters_with_empty_api_key(monkeypatch, capsys):
    # Unset the environment variable
    monkeypatch.delenv("CT_KEY", raising=False)

    # Call the function and catch SystemExit
    with pytest.raises(SystemExit) as exc_info:
        get_request_parameters()

    # Check the exit code
    assert exc_info.value.code == 1

    # Check if the error message is printed to stdout
    captured = capsys.readouterr()
    assert "CT_KEY is empty.  Is it defined in the environment?" in captured.out


def test_load_db_credentials(monkeypatch, capsys):
    # Set environment variables for testing
    monkeypatch.setenv("PGUSER", "test_user")
    monkeypatch.setenv("PGPASSWD", "test_password")
    monkeypatch.setenv("PGHOST", "test_host")
    monkeypatch.setenv("PGPORT", "5432")

    # Call the function
    result = load_db_credentials()

    # Check the return value
    assert result == ("test_user", "test_password", "test_host", "5432", "ct")

    # Check if any message is printed to stdout
    captured = capsys.readouterr()
    assert captured.out == ""

    # Reset environment variables
    monkeypatch.delenv("PGUSER", raising=False)
    monkeypatch.delenv("PGPASSWD", raising=False)
    monkeypatch.delenv("PGHOST", raising=False)
    monkeypatch.delenv("PGPORT", raising=False)


def test_load_db_credentials_with_empty_parameters(monkeypatch, capsys):
    # Unset the environment variable
    monkeypatch.delenv("PGPASSWD", raising=False)

    # Set environment variables with one being unset
    monkeypatch.setenv("PGUSER", "test_user")
    monkeypatch.setenv("PGHOST", "test_host")
    monkeypatch.setenv("PGPORT", "5432")

    # Call the function and catch SystemExit
    with pytest.raises(SystemExit) as exc_info:
        load_db_credentials()

    # Check the exit code
    assert exc_info.value.code == 1

    # Check if the error message is printed to stdout
    captured = capsys.readouterr()
    assert (
        "At least one of the PostgreSQL parameters is empty.  Are they defined in the environment?"
        in captured.out
    )

    # Reset environment variables
    monkeypatch.delenv("PGUSER", raising=False)
    monkeypatch.delenv("PGPASSWD", raising=False)
    monkeypatch.delenv("PGHOST", raising=False)
    monkeypatch.delenv("PGPORT", raising=False)


def test_isoformat_to_seconds_one_datetime_argument():  # Test isoformat_to_seconds
    dt_object_one = datetime.strptime("2023-12-10 05:00:00", "%Y-%m-%d %H:%M:%S")
    assert "2023-12-10T05:00:00" == (isoformat_to_seconds(dt_object_one))


def test_isoformat_to_seconds_two_datetime_arguments():  # Test isoformat_to_seconds
    dt_object_one = datetime.strptime("2023-12-10 05:00:00", "%Y-%m-%d %H:%M:%S")
    dt_object_two = datetime.strptime("2023-12-12 18:00:00", "%Y-%m-%d %H:%M:%S")

    assert "2023-12-10T05:00:00", "2023-12-12T18:00:00" == (
        isoformat_to_seconds(dt_object_one, dt_object_two)
    )
