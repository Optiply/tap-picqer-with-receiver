"""Tests for tap-picqer Firestore extension integration."""

from tap_picqer.tap import Tappicqer


def test_picqer_tap_preserves_force_full_sync_state():
    tap = Tappicqer(config={"api_key": "token", "org": "example"})

    tap.load_state({"bookmarks": {}, "force_full_sync": ["orders"]})

    assert tap.state["force_full_sync"] == ["orders"]


def test_picqer_tap_uses_firestore_extension(monkeypatch):
    calls = []

    def fake_initialize(self):
        calls.append("initialize")
        return self

    def fake_filter(self, main_streams):
        calls.append(("filter", [stream.name for stream in main_streams]))
        return main_streams

    def fake_discover(self, main_streams):
        calls.append(("discover", [stream.name for stream in main_streams]))
        return []

    monkeypatch.setattr("tap_firestore.extension.FirestoreExtension.initialize", fake_initialize)
    monkeypatch.setattr("tap_firestore.extension.FirestoreExtension.filter_main_streams", fake_filter)
    monkeypatch.setattr("tap_firestore.extension.FirestoreExtension.discover_streams", fake_discover)

    tap = Tappicqer(
        config={
            "api_key": "token",
            "org": "example",
            "firestore_extension": {
                "enabled": True,
                "tenant_id": "tenant-1",
                "project_id": "project-id",
                "private_key_id": "key-id",
                "private_key": "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----\n",
                "client_email": "test@example.com",
                "collection_name": "picqer_changes",
                "tap_streams": {"orders": "orders"},
                "receiver_only": {},
            },
        }
    )

    discovered = tap.discover_streams()
    discovered_names = [stream.name for stream in discovered]
    assert "orders" in discovered_names
    assert "products" in discovered_names
    assert calls[0] == "initialize"
    assert calls[1][0] == "filter"
    assert "orders" in calls[1][1]
    assert "products" in calls[1][1]
    assert calls[2][0] == "discover"
    assert "orders" in calls[2][1]
