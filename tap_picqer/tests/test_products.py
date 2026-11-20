"""Regression coverage for product child extraction and state growth."""

import json
from urllib.parse import parse_qs, urlparse

import pytest
import requests

from tap_picqer.tap import Tappicqer


@pytest.mark.parametrize("mt_workers", [1, 3])
@pytest.mark.parametrize(
    "product_types,parts_selected,stock_selected,expected_parts",
    [
        (["normal"] * 4, True, False, []),
        (
            ["normal", "virtual_composition", "composition_with_stock", "other"],
            True,
            True,
            [2, 3],
        ),
        (["virtual_composition", "composition_with_stock"], False, True, []),
    ],
)
def test_product_children_preserve_eligibility_and_parent_context(
    monkeypatch, capsys, product_types, parts_selected, stock_selected, expected_parts,
    mt_workers,
):
    products = [
        {"idproduct": index, "type": product_type}
        for index, product_type in enumerate(product_types, start=1)
    ]
    part_requests = []
    stock_requests = []

    def send(session, request, **kwargs):
        parsed_url = urlparse(request.url)
        path = parsed_url.path
        if path == "/api/v1/products":
            data = [] if "offset" in parse_qs(parsed_url.query) else products
        elif path.endswith("/parts"):
            product_id = int(path.split("/")[-2])
            part_requests.append(product_id)
            data = [{"idproduct_part": 100 + product_id, "idproduct": 99, "amount": 2}]
        elif path.endswith("/stock"):
            product_id = int(path.split("/")[-2])
            stock_requests.append(product_id)
            data = [{"idproduct": product_id, "idwarehouse": 10, "stock": 5}]
        else:
            raise AssertionError(f"Unexpected endpoint: {path}")
        response = requests.Response()
        response.status_code = 200
        response._content = json.dumps(data).encode()
        response.request = request
        response.url = request.url
        return response

    monkeypatch.setattr(requests.Session, "send", send)
    tap = Tappicqer(
        config={"api_key": "synthetic-token", "org": "example", "mt_workers": mt_workers}
    )
    selected = {"products"}
    if parts_selected:
        selected.add("part_products")
    if stock_selected:
        selected.add("stock_products")
    for stream in tap.streams.values():
        stream.mask[()] = stream.name in selected

    tap.sync_all()

    messages = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    records = [message for message in messages if message["type"] == "RECORD"]
    assert [
        message["record"]["idproduct"]
        for message in records
        if message["stream"] == "products"
    ] == [product["idproduct"] for product in products]
    assert part_requests == expected_parts
    assert [
        message["record"]["idproduct_main"]
        for message in records
        if message["stream"] == "part_products"
    ] == expected_parts

    expected_stock = [product["idproduct"] for product in products] if stock_selected else []
    assert stock_requests == expected_stock
    assert [
        message["record"]["idproduct"]
        for message in records
        if message["stream"] == "stock_products"
    ] == expected_stock

    # Skipped products must never accumulate partitions in emitted parts state.
    for message in messages:
        if message["type"] == "STATE":
            parts_state = message["value"].get("bookmarks", {}).get("part_products", {})
            assert {
                partition["context"]["idproduct"]
                for partition in parts_state.get("partitions", [])
            } <= set(expected_parts)
