# tap-tilroy Project Overview

## What is this?

A **Meltano Singer tap** for extracting data from the Tilroy retail/POS API. Built with the Singer SDK.

## Architecture

```
tap_tilroy/
├── __init__.py
├── __main__.py          # Entry point
├── auth.py              # Authentication (not heavily used)
├── client.py            # Base stream classes + shared logic
├── tap.py               # Main TapTilroy class + stream registration
└── streams/             # Individual stream modules
    ├── __init__.py      # Exports all streams
    ├── shops.py         # ShopsStream (non-paginated)
    ├── products.py      # ProductsStream, SuppliersStream
    ├── sales.py         # SalesStream
    ├── stock.py         # StockStream, StockChangesStream
    ├── prices.py        # PricesStream
    ├── purchase.py      # PurchaseOrdersStream
    └── transfers.py     # TransfersStream
```

## Base Classes (in client.py)

| Class | Purpose |
|-------|---------|
| `TilroyStream` | Base REST stream with auth headers, pagination, type coercion |
| `DateFilteredStream` | Adds `dateFrom` parameter for incremental sync |
| `LastIdPaginatedStream` | Cursor-based pagination using `lastId` |
| `DynamicRoutingStream` | Switches between bulk/incremental endpoints |

## Config Properties

```json
{
  "tilroy_api_key": "required - Tilroy API key",
  "x_api_key": "required - AWS API Gateway key", 
  "api_url": "required - Base URL (https://api.tilroy.com)",
  "start_date": "optional - ISO date for initial sync",
  "prices_shop_number": "optional - Shop filter for prices"
}
```

## Stream Dependencies

- `ProductsStream` must run first (collects SKU IDs)
- `StockStream` and `PricesStream` depend on products for SKU list
- See `TapTilroy.sync_all()` for ordering logic

## Key Files Outside tap_tilroy/

- `testconfig.json` - Local test credentials (gitignored)
- `test_state.json` - Test state file for incremental syncs
- `tilroy_docs/` - Downloaded API specs (may exist)
