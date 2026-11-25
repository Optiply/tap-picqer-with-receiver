"""Exercise real CLI state handoff and structured failures without live services."""

import json
import os
import subprocess
import sys
import textwrap

import pytest


@pytest.fixture
def offline_cli(tmp_path):
    """Run the installed CLI with synthetic config and mocked service transports."""
    config = tmp_path / "config.json"
    config.write_text(
        json.dumps(
            {
                "api_key": "synthetic-api-key",
                "org": "example",
                "firestore_extension": {
                    "enabled": True,
                    "tenant_uuid": "synthetic-tenant",
                    "project_id": "synthetic-project",
                    "private_key_id": "synthetic-key-id",
                    "private_key": "synthetic-not-a-private-key",
                    "client_email": "synthetic@example.com",
                    "tap_streams": {"products": "products"},
                },
            }
        )
    )
    script = tmp_path / "tap-picqer-offline.py"
    script.write_text(
        textwrap.dedent(
            """\
        import json
        import sys
        from pathlib import Path
        from types import SimpleNamespace
        from unittest.mock import Mock, patch
        from urllib.parse import urlparse

        import requests
        import tap_picqer.tap as tap_module
        from tap_picqer.streams import ProductsStream, SuppliersStream

        status = int(sys.argv.pop(1))
        tap_module.STREAM_TYPES = [ProductsStream, SuppliersStream]
        query = Mock()
        query.where.return_value = query
        query.order_by.return_value = query
        query.limit.return_value = query
        query.stream.return_value = iter(())
        query.document.return_value.get.return_value = SimpleNamespace(
            exists=True, to_dict=lambda: {"status": "active"}
        )
        db = Mock()
        db.collection.return_value = query

        def send(session, request, **kwargs):
            parsed = urlparse(request.url)
            assert parsed.hostname == "example.picqer.com"
            assert parsed.path in ("/api/v1/products", "/api/v1/suppliers")
            print("SYNTHETIC_REQUEST " + parsed.path, file=sys.stderr)
            response = requests.Response()
            response.status_code = status if parsed.path.endswith("/suppliers") else 200
            response._content = b"[]"
            response.request = request
            response.url = request.url
            return response

        with patch("requests.Session.send", send), patch(
            "tap_firestore.extension.get_firestore_client", return_value=db
        ):
            config_path = Path(sys.argv[sys.argv.index("--config") + 1])
            catalog_path = Path(sys.argv[sys.argv.index("--catalog") + 1])
            if not catalog_path.exists():
                tap = tap_module.Tappicqer(config=json.loads(config_path.read_text()))
                catalog = tap.catalog.to_dict()
                for stream in catalog["streams"]:
                    for metadata in stream["metadata"]:
                        if metadata["breadcrumb"] == []:
                            metadata["metadata"]["selected"] = True
                catalog_path.write_text(json.dumps(catalog))
            tap_module.Tappicqer.cli()
    """
        )
    )

    def run(status, state):
        run_dir = tmp_path / ("run-" + str(len(list(tmp_path.glob("run-*")))))
        run_dir.mkdir()
        state_path = run_dir / "state.json"
        state_path.write_text(json.dumps(state))
        result = subprocess.run(
            [
                sys.executable,
                str(script),
                str(status),
                "--config",
                str(config),
                "--catalog",
                str(tmp_path / "catalog.json"),
                "--state",
                str(state_path),
            ],
            cwd=run_dir,
            env={"PATH": os.environ["PATH"]},
            text=True,
            capture_output=True,
            timeout=30,
        )
        messages = [json.loads(line) for line in result.stdout.splitlines()]
        states = [
            message["value"] for message in messages if message["type"] == "STATE"
        ]
        exceptions = [
            json.loads(path.read_text())
            for path in run_dir.glob("hg-tap-exception-*.json")
        ]
        return result, states, exceptions

    return run


def test_receiver_checkpoint_does_not_hide_supplier_403(offline_cli):
    """Switch a cold API sync to receiver mode, then retain its state on failure."""
    cold, states, exceptions = offline_cli(200, {})
    assert cold.returncode == 0, cold.stderr
    assert exceptions == []
    assert "SYNTHETIC_REQUEST /api/v1/products" in cold.stderr
    state = states[-1]
    assert state["bookmarks"]["receiver_products"] == {
        "replication_key": "received_at",
        "replication_key_value": "",
    }

    warm, states, exceptions = offline_cli(403, state)
    assert warm.returncode != 0
    assert "SYNTHETIC_REQUEST /api/v1/products" not in warm.stderr
    assert warm.stderr.count("SYNTHETIC_REQUEST /api/v1/suppliers") == 1
    assert states[-1]["bookmarks"]["receiver_products"]["replication_key_value"] == ""
    assert len(exceptions) == 1
    assert exceptions[0]["exception_name"] == "FatalAPIError"
    assert "403" in exceptions[0]["exception_message"]
    assert "/suppliers" in exceptions[0]["exception_message"]
    assert "bookmarks" not in exceptions[0]["exception_message"]


def test_failed_full_sync_does_not_enable_receiver_handoff(offline_cli):
    """Do not skip a failed full API sync on the next run."""
    result, states, exceptions = offline_cli(403, {})
    assert result.returncode != 0
    assert "SYNTHETIC_REQUEST /api/v1/products" in result.stderr
    assert all(
        "replication_key_value"
        not in state.get("bookmarks", {}).get("receiver_products", {})
        for state in states
    )
    assert len(exceptions) == 1
    assert "403" in exceptions[0]["exception_message"]


def test_about_is_credential_free_and_does_not_read_local_config(tmp_path):
    """Keep metadata commands offline under the new SDK."""
    marker = "synthetic-local-config-must-not-be-logged"
    (tmp_path / "config.json").write_text(json.dumps({"apiCredentials": marker}))
    result = subprocess.run(
        ["tap-picqer", "--about", "--format=json"],
        cwd=tmp_path,
        env={"PATH": os.environ["PATH"]},
        text=True,
        capture_output=True,
        timeout=30,
        check=True,
    )
    metadata = json.loads(result.stdout)
    assert "hotglue-exceptions-classes" in metadata["capabilities"]
    assert metadata["alerting_level"] == "warning"
    assert marker not in result.stdout + result.stderr
