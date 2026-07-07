# tap-picqer-with-receiver

`tap-picqer` is a Singer tap for the [Picqer API](https://picqer.com/en/api) built with the Meltano Singer SDK.

This version also supports the Firestore receiver extension from `tap-firestore`, so selected Picqer streams can switch from full Picqer API pulls to Firestore-backed incremental receiver streams after an initial sync.

## Installation

From GitHub:

```bash
pipx install git+https://github.com/Optiply/tap-picqer-with-receiver.git
```

For local development:

```bash
pipx install poetry
poetry install
poetry run tap-picqer --help
```

`tap-firestore` is installed as a dependency from:

```toml
tap-firestore = {git = "https://github.com/Optiply/tap-firestore", branch = "main"}
```

## Configuration

### Picqer settings

| Setting | Required | Default | Description |
| --- | --- | --- | --- |
| `api_key` | Yes | unset | Picqer API key. Used as HTTP Basic Auth username with an empty password. |
| `org` | Yes | unset | Picqer organization subdomain. Requests go to `https://{org}.picqer.com/api/v1`. |
| `picqer_fulfilment` | No | `false` | Skip Picqer endpoints unavailable for fulfilment accounts. |
| `firestore_extension` | No | unset | Optional Firestore receiver configuration. If present, the extension is enabled unless `enabled` is explicitly `false`. |

Minimal config:

```json
{
  "api_key": "<picqer-api-key>",
  "org": "my-picqer-org"
}
```

## Streams

The tap currently discovers these Picqer API streams:

| Stream | Endpoint | Notes |
| --- | --- | --- |
| `products` | `/products` | Product catalog; includes stock array in the schema. |
| `products_inactive` | `/products?inactive=true` | Inactive products; incremental on `updated`. |
| `stock_history` | `/stockhistory` | Incremental on `changed_at`. |
| `warehouse_products` | `/products/{idproduct}/warehouses` | Child stream of products. |
| `stock_products` | `/products/{idproduct}/stock` | Child stream of products. |
| `image_products` | `/products/{idproduct}/images` | Child stream of products. |
| `location_products` | `/products/{idproduct}/locations` | Child stream of products. |
| `part_products` | `/products/{idproduct}/parts` | Child stream of products. |
| `suppliers` | `/suppliers` | Supplier master data. |
| `warehouses` | `/warehouses` | Warehouse master data. |
| `productfields` | `/productfields` | Product field metadata. |
| `orders` | `/orders` | Sell orders. |
| `orderfields` | `/orderfields` | Order field metadata. |
| `backorders` | `/backorders` | Incremental on `created_at`. |
| `purchaseorders` | `/purchaseorders` | Incremental on `updated`. |
| `picklists` | `/picklists` | Incremental on `created`. |
| `picklists_closed` | `/picklists?status=closed` | Incremental on `created`. |
| `receipts` | `/receipts` | Incremental on `updated`. |

Pagination uses Picqer's offset style: the tap requests offsets in steps of 100 while responses contain records.

When a stream has a replication key, the tap sends Picqer date filters:

- `updated_after` for replication key `updated`
- `sincedate` for `created`, `created_at`, and `changed_at`

## Firestore receiver extension

If `firestore_extension` is configured, this tap imports `FirestoreExtension` from `tap-firestore`.

The integration works as follows:

1. `discover_streams()` builds the normal Picqer API streams.
2. If `firestore_extension` is absent or has `enabled: false`, only the normal Picqer streams are returned.
3. If enabled, the tap appends Firestore receiver streams for configured `tap_streams` and `receiver_only` streams.
4. At sync time, `apply_runtime_selection()` decides per stream whether to run the Picqer API stream or the Firestore receiver stream.
5. After a full Picqer API sync, `write_post_full_sync_bookmarks()` writes receiver bookmarks so later runs can switch to Firestore incremental sync.

### Firestore config shape

```json
{
  "api_key": "<picqer-api-key>",
  "org": "my-picqer-org",
  "firestore_extension": {
    "enabled": true,
    "tenant_uuid": "tenant-1",
    "firestore_project_id": "my-firebase-project",
    "firestore_private_key_id": "key-id",
    "firestore_private_key": "<service-account-private-key>",
    "firestore_client_email": "firebase-adminsdk@example.iam.gserviceaccount.com",
    "collection_name": "picqer_changes",
    "tap_streams": {
      "products": {"entity_type": "receiver_products", "schema_mode": "inherit"},
      "orders": {"entity_type": "receiver_orders", "schema_mode": "inherit"},
      "purchaseorders": {"entity_type": "receiver_purchaseorders", "schema_mode": "inherit"},
      "receipts": {"entity_type": "receiver_receipts", "schema_mode": "inherit"}
    },
    "receiver_only": {
      "stock": {
        "entity_type": "receiver_stock",
        "schema_mode": "minimal"
      }
    }
  }
}
```

Supported Firestore credential keys inside `firestore_extension`:

- `project_id`, `private_key_id`, `private_key`, `client_email`
- or aliases `firestore_project_id`, `firestore_private_key_id`, `firestore_private_key`, `firestore_client_email`

Supported receiver settings:

| Setting | Description |
| --- | --- |
| `enabled` | Optional. Defaults to `true` when `firestore_extension` is present. Set to `false` to disable receiver behavior. |
| `tenant_uuid` | Firestore tenant document ID. |
| `collection_name` | Firestore collection containing receiver change documents, for example `picqer_changes`. |
| `start_date` | Initial lower bound for receiver `received_at` when no receiver state exists. |
| `tap_streams` | Host Picqer streams that can be mirrored by Firestore receiver streams. |
| `receiver_only` | Extra Firestore-only streams with no matching Picqer API stream. |

### State and full sync control

The tap preserves `force_full_sync` in Singer state. Use it to force specific streams back to the Picqer API for a run:

```json
{
  "bookmarks": {},
  "force_full_sync": ["orders", "products"]
}
```

Rules:

- If no receiver bookmark exists for a `tap_streams` stream, the Picqer API stream runs.
- If a receiver bookmark exists, the Firestore receiver stream runs.
- If the stream is listed in `force_full_sync`, the Picqer API stream runs even if receiver state exists.

## Usage

Discover catalog:

```bash
tap-picqer --config config.json --discover > catalog.json
```

Run sync:

```bash
tap-picqer --config config.json --catalog catalog.json > output.singer
```

Useful commands:

```bash
tap-picqer --version
tap-picqer --help
tap-picqer --about
```

## Development

```bash
poetry install
poetry run pytest
```

The CLI entrypoint is:

```toml
[tool.poetry.scripts]
tap-picqer = 'tap_picqer.tap:Tappicqer.cli'
```
