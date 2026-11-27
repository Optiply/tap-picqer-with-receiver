"""Tests standard tap features using the built-in SDK tests library."""

import requests
from hotglue_singer_sdk.testing import get_standard_tap_tests

from tap_picqer.tap import Tappicqer

SAMPLE_CONFIG = {"api_key": "synthetic-api-key", "org": "example"}


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests(monkeypatch):
    """Run standard tap tests against empty mocked API pages, never live APIs."""

    def send(session, request, **kwargs):
        assert request.method == "GET"
        assert request.url.startswith("https://example.picqer.com/api/v1/")
        response = requests.Response()
        response.status_code = 200
        response._content = b"[]"
        response.request = request
        response.url = request.url
        return response

    monkeypatch.setattr(requests.Session, "send", send)
    tests = get_standard_tap_tests(Tappicqer, config=SAMPLE_CONFIG)
    for test in tests:
        test()
