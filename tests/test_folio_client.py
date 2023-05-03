import pytest  # noqa

import requests

from libsys_airflow.plugins.folio.folio_client import FolioClient


@pytest.fixture
def client(requests_mock):
    requests_mock.post(
        "https://okapi-test.stanford.edu/authn/login",
        request_headers={"x-okapi-tenant": "sul"},
        status_code=201,
        headers={"x-okapi-token": "abc123"},
    )
    return FolioClient(
        "https://okapi-test.stanford.edu",
        "sul",
        "admin",
        "sekret",
    )


def test_login(client):
    assert client.okapi_token == "abc123"


def test_get(client, requests_mock):
    requests_mock.get(
        "https://okapi-test.stanford.edu/foo",
        request_headers={"x-okapi-tenant": "sul", "x-okapi-token": "abc123"},
        status_code=200,
        json={"foo": "bar"},
    )
    assert client.get("/foo") == {"foo": "bar"}


def test_get_error(client, requests_mock):
    requests_mock.get(
        "https://okapi-test.stanford.edu/foo",
        request_headers={"x-okapi-tenant": "sul", "x-okapi-token": "abc123"},
        status_code=500,
    )
    with pytest.raises(requests.exceptions.HTTPError):
        client.get("/foo")


def test_post(client, requests_mock):
    requests_mock.post(
        "https://okapi-test.stanford.edu/foo",
        request_headers={"x-okapi-tenant": "sul", "x-okapi-token": "abc123"},
        status_code=201,
        json={"foo": "bar"},
        additional_matcher=match_request_json,
    )
    assert client.post("/foo", payload={"req_foo": "req_bar"}) == {"foo": "bar"}


def test_post_error(client, requests_mock):
    requests_mock.post(
        "https://okapi-test.stanford.edu/foo",
        request_headers={"x-okapi-tenant": "sul", "x-okapi-token": "abc123"},
        status_code=400,
        additional_matcher=match_request_json,
    )
    with pytest.raises(requests.exceptions.HTTPError):
        client.post("/foo", payload={"req_foo": "req_bar"})


def test_put(client, requests_mock):
    requests_mock.put(
        "https://okapi-test.stanford.edu/foo",
        request_headers={"x-okapi-tenant": "sul", "x-okapi-token": "abc123"},
        status_code=201,
        json={"foo": "bar"},
        additional_matcher=match_request_json,
    )
    assert client.put("/foo", payload={"req_foo": "req_bar"}) == {"foo": "bar"}


def test_put_error(client, requests_mock):
    requests_mock.put(
        "https://okapi-test.stanford.edu/foo",
        request_headers={"x-okapi-tenant": "sul", "x-okapi-token": "abc123"},
        status_code=403,
        additional_matcher=match_request_json,
    )
    with pytest.raises(requests.exceptions.HTTPError):
        client.put("/foo", payload={"req_foo": "req_bar"})


def match_request_json(request):
    return request.json() == {"req_foo": "req_bar"}
