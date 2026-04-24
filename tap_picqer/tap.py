"""picqer tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers
from tap_picqer.streams import (
    ProductsStream,
    ProductsInativeStream,
    StockHistoryStream,
    WareHouseProductsStream,
    StockProductsStream,
    ImageProductsStream,
    LocationProductsStream,
    PartProductsStream,
    SuppliersStream,
    WarehousesStream,
    ProductFieldsStream,
    OrdersStream,
    OrderFieldsStream,
    BackOrdersStream,
    PurchaseOrdersStream,
    PicklistsStream,
    PicklistsClosedStream,
    ReceiptsStream,
)
from tap_firestore.extension import FirestoreExtension

STREAM_TYPES = [
    ProductsStream,
    ProductsInativeStream,
    StockHistoryStream,
    WareHouseProductsStream,
    StockProductsStream,
    ImageProductsStream,
    LocationProductsStream,
    PartProductsStream,
    SuppliersStream,
    WarehousesStream,
    ProductFieldsStream,
    OrdersStream,
    OrderFieldsStream,
    BackOrdersStream,
    PurchaseOrdersStream,
    PicklistsStream,
    PicklistsClosedStream,
    ReceiptsStream,
]


class Tappicqer(Tap):
    """picqer tap class."""
    name = "tap-picqer"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            required=True,
            description="The token to authenticate against the API service"
        ),
        th.Property(
            "org",
            th.StringType,
            required=True,
            description="Picqer organization subdomain.",
        ),
        th.Property(
            "firestore_extension",
            th.CustomType(
                {
                    "type": "object",
                    "properties": {
                        "enabled": {"type": "boolean"},
                        "tenant_id": {"type": "string"},
                        "project_id": {"type": "string"},
                        "private_key_id": {"type": "string"},
                        "private_key": {"type": "string"},
                        "client_email": {"type": "string"},
                        "token_uri": {"type": "string"},
                        "collection_name": {"type": "string"},
                        "start_date": {"type": "string", "format": "date-time"},
                        "tap_streams": {
                            "type": "object",
                            "additionalProperties": {
                                "oneOf": [
                                    {"type": "string"},
                                    {
                                        "type": "object",
                                        "properties": {
                                            "entity_type": {"type": "string"},
                                            "schema_mode": {
                                                "type": "string",
                                                "enum": ["inherit", "file"],
                                            },
                                            "schema_file": {"type": "string"},
                                            "primary_keys": {
                                                "type": "array",
                                                "items": {"type": "string"},
                                            },
                                        },
                                        "additionalProperties": False,
                                    },
                                ]
                            },
                        },
                        "receiver_only": {
                            "type": "object",
                            "additionalProperties": {
                                "oneOf": [
                                    {"type": "string"},
                                    {
                                        "type": "object",
                                        "properties": {
                                            "entity_type": {"type": "string"},
                                            "schema_mode": {
                                                "type": "string",
                                                "enum": ["minimal", "file"],
                                            },
                                            "schema_file": {"type": "string"},
                                            "primary_keys": {
                                                "type": "array",
                                                "items": {"type": "string"},
                                            },
                                            "extra_properties": {"type": "object"},
                                        },
                                        "additionalProperties": False,
                                    },
                                ]
                            },
                        },
                    },
                    "additionalProperties": False,
                }
            ),
            required=False,
        ),
    ).to_dict()

    def load_state(self, state):
        """Preserve receiver extension state flags in addition to bookmarks."""
        super().load_state(state)
        if "force_full_sync" in state:
            self.state["force_full_sync"] = list(state.get("force_full_sync", []))

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        main_streams = [stream_class(tap=self) for stream_class in STREAM_TYPES]
        ext_config = self.config.get("firestore_extension")
        if not ext_config or not ext_config.get("enabled", False):
            return main_streams

        extension = FirestoreExtension(tap=self, config=ext_config).initialize()
        filtered_main = extension.filter_main_streams(main_streams)
        firestore_streams = extension.discover_streams(main_streams)
        return [*filtered_main, *firestore_streams]

    def sync_all(self) -> None:
        """Apply receiver/host runtime selection before syncing."""
        ext_config = self.config.get("firestore_extension")
        if ext_config and ext_config.get("enabled", False):
            extension = FirestoreExtension(tap=self, config=ext_config)
            extension.apply_runtime_selection(self.streams)
        super().sync_all()

if __name__ == "__main__":
    Tappicqer.cli()