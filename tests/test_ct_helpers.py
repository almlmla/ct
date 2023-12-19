import pytest

import json
import os
import sys

from datetime import datetime
from minio import Minio
from minio.error import S3Error

import requests
from sqlalchemy import create_engine

from unittest.mock import MagicMock, call, patch
from unittest.mock import Mock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from ctetl.ct_helpers import get_request_parameters
from ctetl.ct_helpers import load_db_credentials
from ctetl.ct_helpers import create_minio_client
from ctetl.ct_helpers import check_minio_buckets
from ctetl.ct_helpers import create_minio_tags
from ctetl.ct_helpers import get_minio_object_names
from ctetl.ct_helpers import get_minio_response_js
from ctetl.ct_helpers import request_with_backoff
from ctetl.ct_helpers import isoformat_to_seconds
from ctetl.ct_helpers import create_sqlalchemy_engine
from ctetl.ct_helpers import minio_put_text_object


class MockMinioResponse:
    def __init__(self, data):
        self.data = data

    def decode(self):
        return self.data

    def close(self):
        pass

    def release_conn(self):
        pass


class MockMinioClient:
    def get_object(self, bucket_name, object_name):
        pass

    def put_object(self, bucket_name, object_name, data, length, content_type):
        pass


class MockRequestResponse:
    def __init__(self, text):
        self.text = text


class MockS3Error(S3Error):
    def __init__(
        self,
        code,
        message,
        resource,
        request_id,
        host_id,
        response,
        bucket_name=None,
        object_name=None,
    ):
        super().__init__(
            code,
            message,
            resource,
            request_id,
            host_id,
            response,
            bucket_name=bucket_name,
            object_name=object_name,
        )


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


def test_create_minio_client_with_valid_parameters(monkeypatch):
    # Set the environment variables
    monkeypatch.setenv("MINIO_HOST", "example.com")
    monkeypatch.setenv("MINIO_ACCESS", "your_access_key")
    monkeypatch.setenv("MINIO_SECRET", "your_secret_key")

    # Call the function
    minio_client = create_minio_client()

    # Check if the return value is an instance of Minio
    assert isinstance(minio_client, Minio)


def test_create_minio_client_with_empty_parameters(monkeypatch, capsys):
    # Unset one of the required environment variables
    monkeypatch.delenv("MINIO_ACCESS", raising=False)

    # Call the function and catch SystemExit
    with pytest.raises(SystemExit) as exc_info:
        create_minio_client()

    # Check the exit code
    assert exc_info.value.code == 1

    # Check if the error message is printed to stdout
    captured = capsys.readouterr()
    assert (
        "At least one of the MinIO parameters is empty.  Are they defined in the environment?"
        in captured.out
    )

    # Reset environment variables
    monkeypatch.delenv("MINIO_HOST", raising=False)
    monkeypatch.delenv("MINIO_ACCESS", raising=False)
    monkeypatch.delenv("MINIO_SECRET", raising=False)


def test_check_minio_buckets_with_existing_buckets():
    # Mock the minio_client and set buckets
    minio_client = MagicMock()
    minio_client.bucket_exists.return_value = True

    # Call the function with existing buckets
    check_minio_buckets(minio_client, "bucket1", "bucket2", "bucket3")

    # Assert that bucket_exists is called for each bucket
    minio_client.bucket_exists.assert_any_call("bucket1")
    minio_client.bucket_exists.assert_any_call("bucket2")
    minio_client.bucket_exists.assert_any_call("bucket3")


def test_check_minio_buckets_with_nonexistent_bucket(capsys):
    # Mock the minio_client and set a nonexistent bucket
    minio_client = MagicMock()
    minio_client.bucket_exists.return_value = False

    # Call the function with a nonexistent bucket and catch SystemExit
    with pytest.raises(SystemExit) as exc_info:
        check_minio_buckets(minio_client, "nonexistent_bucket")

    # Check the exit code
    assert exc_info.value.code == 1

    # Check if the error message is printed to stdout
    captured = capsys.readouterr()
    assert "Cannot find bucket 'nonexistent_bucket', exiting." in captured.out


def test_check_minio_buckets_with_mixed_buckets(capsys):
    # Mock the minio_client and set a mix of existing and nonexistent buckets
    minio_client = MagicMock()
    minio_client.bucket_exists.side_effect = [True, False, True]

    # Call the function with a mix of existing and nonexistent buckets and catch SystemExit
    with pytest.raises(SystemExit) as exc_info:
        check_minio_buckets(
            minio_client,
            "existing_bucket",
            "nonexistent_bucket",
            "another_existing_bucket",
        )

    # Check the exit code
    assert exc_info.value.code == 1

    # Check if the error message is printed to stdout
    captured = capsys.readouterr()
    assert "Cannot find bucket 'nonexistent_bucket', exiting." in captured.out


def test_create_minio_tags():
    # Call the function
    tags = create_minio_tags()

    # Check if tags is a dictionary
    assert isinstance(tags, dict)

    # Check if "processed" tag is present
    assert "processed" in tags

    # Check if the value of the "processed" tag is "true"
    assert tags["processed"] == "true"


def test_get_minio_object_names():
    # Mock the minio_client
    minio_client = MagicMock()

    # Set up a list of MinIO objects for the mock
    mock_objects = [
        MagicMock(object_name="object1"),
        MagicMock(object_name="object2"),
        MagicMock(object_name="object3"),
    ]

    # Configure the list_objects method of the mock to return the mock_objects
    minio_client.list_objects.return_value = mock_objects

    # Call the function
    bucket = "test_bucket"
    object_names = get_minio_object_names(minio_client, bucket)

    # Check if the list_objects method was called with the correct bucket
    minio_client.list_objects.assert_called_once_with(bucket)

    # Check if the returned object_names match the object_names from the mock objects
    assert object_names == ["object1", "object2", "object3"]

@patch("ctetl.ct_helpers.json.loads")  # Replace 'your_module' with the actual module name
def test_get_minio_response_js(mock_json_loads):
    # Arrange
    object_name = "mock_object"
    bucket = "mock_bucket"
    client = MagicMock()
    response_data = b'{"key": "value"}'
    mock_json_loads.return_value = {"key": "value"}
    mock_response = MagicMock()
    mock_response.data.decode.return_value = response_data
    client.get_object.return_value.__enter__.return_value = mock_response

    # Act
    result = get_minio_response_js(object_name, bucket, client)

    # Assert
    assert result == {"key": "value"}
    mock_json_loads.assert_called_once_with(response_data)
    client.get_object.assert_called_once_with(bucket, object_name)
    mock_response.data.decode.assert_called_once()

def test_get_minio_response_js_error_handling():
    # Arrange
    object_name = "mock_object"
    bucket = "mock_bucket"
    client = MagicMock()
    mock_exception = MockS3Error(
        code="MockErrorCode",
        message="Mock S3 Error",
        resource="mock_resource",
        request_id="mock_request_id",
        host_id="mock_host_id",
        response={},
    )
    client.get_object.side_effect = mock_exception

    # Act and Assert
    with pytest.raises(SystemExit) as exc_info:
        get_minio_response_js(object_name, bucket, client)

    assert exc_info.value.code == 1



def test_minio_put_text_object():
    # Arrange
    minio_client = MockMinioClient()
    bucket = "test_bucket"
    object_name = "test_object"
    request_response_text = "Test data"
    request_response = MockRequestResponse(request_response_text)

    # Act
    minio_put_text_object(minio_client, bucket, object_name, request_response)


    # Assert
    # Add assertions based on the expected behavior of your function
    # For example, check if the put_object method is called with the correct arguments


def test_minio_put_text_object_error_handling():
    # Arrange
    minio_client = MockMinioClient()
    bucket = "test_bucket"
    object_name = "test_object"
    request_response_text = "Test data"
    request_response = MockRequestResponse(request_response_text)

    # Implement a mock behavior for put_object to raise your custom MockS3Error
    def mock_put_object_raise_error(*args, **kwargs):
        raise MockS3Error(
            code="MockErrorCode",
            message="Mock S3 Error",
            resource="mock_resource",
            request_id="mock_request_id",
            host_id="mock_host_id",
            response="mock_response",
            bucket_name="mock_bucket_name",
            object_name="mock_object_name",
        )

    minio_client.put_object = mock_put_object_raise_error

    # Act and Assert
    with pytest.raises(SystemExit):
        minio_put_text_object(minio_client, bucket, object_name, request_response)


@pytest.fixture
def mock_session(mocker):
    return mocker.patch("ctetl.ct_helpers.requests.Session", autospec=True)


def test_request_with_backoff_successful(mock_session):
    # Set up the mock session to return a successful response
    mock_response = Mock()
    mock_response.raise_for_status.return_value = None
    mock_session.return_value.get.return_value = mock_response

    # Call the function
    url = "http://example.com"
    result = request_with_backoff(url)

    # Check that the mock session's get method was called with the correct arguments
    mock_session.return_value.get.assert_called_once_with(url, headers=None)

    # Check that the result is the mock response
    assert result == mock_response


def test_request_with_backoff_unsuccessful(mock_session, capsys):
    # Set up the mock session to raise a RequestException
    mock_session.return_value.get.side_effect = requests.exceptions.RequestException(
        "Mock error"
    )

    # Call the function
    url = "http://example.com"
    result = request_with_backoff(url)

    # Check that the mock session's get method was called with the correct arguments
    mock_session.return_value.get.assert_called_once_with(url, headers=None)

    # Check that the result is None
    assert result is None

    # Check that the error message is printed to stdout
    captured = capsys.readouterr()
    assert "Requests error after retrying.  Error: Mock error" in captured.out


def test_isoformat_to_seconds_with_empty_arguments():
    with pytest.raises(ValueError, match="No datetime objects provided."):
        isoformat_to_seconds()


def test_isoformat_to_seconds_with_non_datetime_objects():
    with pytest.raises(ValueError, match="Can only isoformat datetime objects."):
        isoformat_to_seconds("not_a_datetime")


def test_isoformat_to_seconds_with_single_datetime_object():
    input_datetime = datetime(2023, 1, 1, 12, 34, 56)
    result = isoformat_to_seconds(input_datetime)
    assert result == "2023-01-01T12:34:56"


def test_isoformat_to_seconds_with_multiple_datetime_objects():
    input_datetimes = [
        datetime(2023, 1, 1, 12, 34, 56),
        datetime(2023, 1, 2, 10, 30, 15, 6542),
    ]
    result = isoformat_to_seconds(*input_datetimes)
    assert result == ["2023-01-01T12:34:56", "2023-01-02T10:30:15"]


def test_isoformat_to_seconds_with_mixed_objects():
    with pytest.raises(ValueError, match="Can only isoformat datetime objects."):
        isoformat_to_seconds(datetime(2023, 1, 1, 12, 34, 56), "not_a_datetime")


@pytest.fixture
def mock_load_db_credentials(mocker):
    return mocker.patch("ctetl.ct_helpers.load_db_credentials")


@pytest.fixture
def mock_create_engine(mocker):
    return mocker.patch("ctetl.ct_helpers.create_engine")


def test_create_sqlalchemy_engine(mock_load_db_credentials, mock_create_engine):
    # Set up mock values for load_db_credentials
    PGUSER, PGPASSWD, PGHOST, PGPORT, PGDB = (
        "test_user",
        "test_passwd",
        "localhost",
        "5432",
        "test_db",
    )
    mock_load_db_credentials.return_value = (PGUSER, PGPASSWD, PGHOST, PGPORT, PGDB)

    # Set up mock values for create_engine
    mock_engine = Mock()
    mock_create_engine.return_value = mock_engine

    # Call the function
    result = create_sqlalchemy_engine()

    # Check that load_db_credentials was called
    mock_load_db_credentials.assert_called_once()

    # Check that create_engine was called with the correct engine string
    expected_engine_string = (
        f"postgresql://{PGUSER}:{PGPASSWD}@{PGHOST}:{PGPORT}/{PGDB}"
    )
    mock_create_engine.assert_called_once_with(expected_engine_string)

    # Check that the result is the mock engine
    assert result == mock_engine
