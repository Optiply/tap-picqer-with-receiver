"""Concurrent product fetching preserves ordered output and safe failure handling."""

import json
import threading
from collections import Counter
from email.utils import formatdate
from urllib.parse import parse_qs, urlparse

import pytest
import requests
import singer
from singer_sdk.exceptions import ConfigValidationError, FatalAPIError, RetriableAPIError

import tap_picqer.streams as streams
from tap_picqer.tap import Tappicqer


def response(request, data=None, status=200, headers=None, text=None):
    result = requests.Response()
    result.status_code = status
    result._content = (text if text is not None else json.dumps(data or [])).encode()
    result.request = request
    result.url = request.url
    result.headers.update(headers or {})
    return result


def offset(request):
    return int(parse_qs(urlparse(request.url).query).get("offset", ["0"])[0])


def products_tap(workers):
    tap = Tappicqer(config={"api_key": "synthetic-token", "org": "example", "mt_workers": workers})
    for stream in tap.streams.values():
        stream.mask[()] = stream.name == "products"
    return tap


def product_ids(capsys):
    messages = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    return [
        message["record"]["idproduct"]
        for message in messages
        if message["type"] == "RECORD" and message["stream"] == "products"
    ]


@pytest.fixture(autouse=True)
def immediate_sdk_backoff(monkeypatch):
    monkeypatch.setattr(streams.ProductsStream, "backoff_wait_generator", lambda self: iter([0] * 5))
    monkeypatch.setattr(streams.ProductsStream, "backoff_handler", lambda self, details: None)


@pytest.mark.parametrize("workers", [1, 2, 5, 10, 15])
def test_pages_overlap_but_emit_in_order_with_bounded_prefetch(monkeypatch, capsys, workers):
    effective = min(workers, 10)
    initial_wave = threading.Barrier(effective)
    later_completed = threading.Event()
    lock = threading.Lock()
    active = 0
    peak = 0
    requested = []
    completed = []
    main_thread = threading.get_ident()
    write_message = singer.write_message

    def write(message):
        assert threading.get_ident() == main_thread
        write_message(message)

    def send(session, request, **kwargs):
        nonlocal active, peak
        page = offset(request)
        with lock:
            requested.append(page)
            active += 1
            peak = max(peak, active)
        if page < effective * 100:
            initial_wave.wait(timeout=5)
        if page == 0 and effective > 1:
            assert later_completed.wait(timeout=5)
        if page == 0:
            ids = range(100)
        elif page == 100:
            ids = range(100, 150)  # A short page is not the existing API's terminator.
        elif page == 200:
            ids = []
        else:
            ids = [999]  # Speculative responses beyond the first empty page are discarded.
        with lock:
            completed.append(page)
            active -= 1
        if page != 0:
            later_completed.set()
        return response(request, [{"idproduct": value, "type": "normal"} for value in ids])

    monkeypatch.setattr(requests.Session, "send", send)
    monkeypatch.setattr(singer, "write_message", write)
    products_tap(workers).sync_all()

    assert product_ids(capsys) == list(range(150))
    assert peak == effective
    assert max(requested) <= 200 + (effective - 1) * 100
    if effective > 1:
        assert completed[0] != 0


@pytest.mark.parametrize("status,text", [(200, None), (404, "not found")])
def test_empty_first_page_discards_later_products(monkeypatch, capsys, status, text):
    def send(session, request, **kwargs):
        if offset(request) == 0:
            return response(request, status=status, text=text)
        return response(request, [{"idproduct": 999, "type": "normal"}])

    monkeypatch.setattr(requests.Session, "send", send)
    products_tap(3).sync_all()
    assert product_ids(capsys) == []


@pytest.mark.parametrize("failure", [400, 401, 403, 500, "invalid_json", "timeout"])
def test_failed_page_never_emits_later_pages(monkeypatch, capsys, failure):
    attempts = Counter()

    def send(session, request, **kwargs):
        page = offset(request)
        attempts[page] += 1
        if page == 100:
            if failure == "timeout":
                raise requests.exceptions.ReadTimeout("synthetic timeout")
            if failure == "invalid_json":
                return response(request, text="not JSON")
            return response(request, status=failure)
        return response(request, [{"idproduct": page + 1, "type": "normal"}])

    monkeypatch.setattr(requests.Session, "send", send)
    expected = requests.exceptions.ReadTimeout if failure == "timeout" else (
        RetriableAPIError if failure == 500 else FatalAPIError
    )
    with pytest.raises(expected):
        products_tap(2).sync_all()
    assert product_ids(capsys) == [1]
    assert attempts[100] == (5 if failure in [500, "timeout"] else 1)


@pytest.mark.parametrize("failure", [500, "timeout"])
def test_transient_failure_retries_same_offset_without_duplicate_products(monkeypatch, capsys, failure):
    attempts = Counter()

    def send(session, request, **kwargs):
        page = offset(request)
        attempts[page] += 1
        if page == 0 and attempts[page] == 1:
            if failure == "timeout":
                raise requests.exceptions.ReadTimeout("synthetic timeout")
            return response(request, status=500)
        return response(request, [{"idproduct": 1, "type": "normal"}] if page == 0 else [])

    monkeypatch.setattr(requests.Session, "send", send)
    products_tap(2).sync_all()
    assert product_ids(capsys) == [1]
    assert attempts[0] == 2


@pytest.mark.parametrize(
    "retry_after,delay",
    [("10", 10), (formatdate(1700000010, usegmt=True), 10), (None, 2), ("invalid", 2), ("10000", 60)],
)
def test_429_cooldown_also_pauses_other_workers(monkeypatch, retry_after, delay):
    first_wave = threading.Barrier(2)
    rate_limit_published = threading.Event()
    second_worker_retried = threading.Event()
    worker = threading.local()
    lock = threading.Lock()
    now = 0.0
    sent = []
    waits = []
    attempts = Counter()
    original_validate = streams.ProductsStream.validate_response

    def clock():
        with lock:
            return now

    class ClockEvent(threading.Event):
        def wait(self, timeout=None):
            nonlocal now
            if not self.is_set():
                with lock:
                    waits.append((worker.page, timeout))
                    now += timeout
                if worker.page == 100:
                    second_worker_retried.set()
            return self.is_set()

    def validate(self, result):
        if result.status_code == 429:
            rate_limit_published.set()
        original_validate(self, result)

    def backoff(self, details):
        if worker.page == 0:
            assert second_worker_retried.wait(timeout=5)

    def send(session, request, **kwargs):
        page = worker.page = offset(request)
        with lock:
            attempts[page] += 1
            attempt = attempts[page]
            sent.append((page, attempt, now))
        if page in [0, 100] and attempt == 1:
            first_wave.wait(timeout=5)
            if page == 0:
                headers = {"Retry-After": retry_after} if retry_after is not None else {}
                return response(request, status=429, headers=headers)
            assert rate_limit_published.wait(timeout=5)
            return response(request, status=500)
        if page == 100:
            second_worker_retried.set()
        data = [{"idproduct": page + 1, "type": "normal"}] if page < 200 else []
        return response(request, data)

    monkeypatch.setattr(streams, "Event", ClockEvent)
    monkeypatch.setattr(streams, "monotonic", clock)
    monkeypatch.setattr(streams, "time", lambda: 1700000000)
    monkeypatch.setattr(streams, "uniform", lambda low, high: 0)
    monkeypatch.setattr(streams.ProductsStream, "validate_response", validate)
    monkeypatch.setattr(streams.ProductsStream, "backoff_handler", backoff)
    monkeypatch.setattr(requests.Session, "send", send)

    records = list(products_tap(2).streams["products"].get_records(None))
    assert [record["idproduct"] for record in records] == [1, 101]
    assert (100, delay) in waits
    assert all(start >= delay for page, attempt, start in sent if attempt > 1 or page >= 200)


def test_exhausted_429_is_not_an_empty_page(monkeypatch):
    attempts = Counter()

    def send(session, request, **kwargs):
        page = offset(request)
        attempts[page] += 1
        return response(request, status=429, headers={"Retry-After": "0"})

    monkeypatch.setattr(requests.Session, "send", send)
    with pytest.raises(RetriableAPIError):
        list(products_tap(2).streams["products"].get_records(None))
    assert attempts[0] == 5


@pytest.mark.parametrize("workers", [0, -1, 1.5, "3", None, True])
def test_invalid_worker_config_is_rejected(workers):
    with pytest.raises(ConfigValidationError):
        products_tap(workers)
