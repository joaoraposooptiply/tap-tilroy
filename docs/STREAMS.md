# Stream Documentation

## All Streams (10 total)

| Stream | Endpoint | Base Class | Replication Key | Notes |
|--------|----------|------------|-----------------|-------|
| `products` | `/product-bulk/production/products` or `/product/production/export/skus` | `DynamicRoutingStream` | `dateModified` | Switches bulk/incremental |
| `shops` | `/shop/production/shops` | `TilroyStream` | - (full table) | Non-paginated, single request |
| `purchase_orders` | `/purchaseorder/production/purchaseorders` or `/purchaseorder/production/export/purchaseorders` | `DynamicRoutingStream` | `orderDate` | Switches bulk/incremental |
| `stock_changes` | `/stockapi/production/stockchanges` | `DateFilteredStream` | `dateUpdated` | Page-based pagination, requires dateFrom+dateTo |
| `stock_deltas` | `/stockapi/production/export/stockdeltas` | `DateFilteredStream` | `dateExported` | Transfer/correction events with qtyDelta fields |
| `sales` | `/saleapi/production/export/sales` | `DateFilteredStream` | `saleDate` | Uses /export endpoint |
| `suppliers` | `/supplier/production/suppliers` | `TilroyStream` | - (full table) | Simple full sync |
| `prices` | `/price/production/price/rules` | `LastIdPaginatedStream` | `date_modified` | Cursor pagination |
| `stock` | `/stock/production/skus/{sku_id}/stock` | `TilroyStream` | - | Depends on products for SKU IDs |
| `transfers` | `/transferapi/production/export/transfers` | `DateFilteredStream` | `dateExported` | Inter-store transfers |

## Stream Details

### ShopsStream
- **Special**: Non-paginated endpoint, returns all shops in single response
- **Override**: `request_records()` to bypass SDK pagination
- **Override**: `get_url_params()` returns empty dict (no page/count)

### ProductsStream
- **Special**: Collects SKU IDs for downstream StockStream
- **Method**: `clear_collected_sku_ids()`, `get_collected_sku_ids()`
- **Dynamic**: Uses bulk endpoint for full sync, export for incremental

### SalesStream
- **Important**: Uses `/export/sales` NOT `/sales`
- **Reason**: `/sales` requires customer filter, `/export/sales` doesn't

### PricesStream
- **Pagination**: Uses `lastId` cursor, NOT page numbers
- **Start**: Must send `lastId=""` (empty string) for first page
- **Flattens**: Price rules into individual records per SKU

### StockDeltasStream
- **Endpoint**: `/export/stockdeltas` - Returns actual stock change events
- **Purpose**: Captures transfer movements, corrections, and other delta events
- **Key fields**: `qtyDelta`, `qtyTransferredDelta`, `qtyReservedDelta`, `modificationType`
- **Param**: Uses `dateExportedSince` (ISO datetime format)
- **Use case**: Track when transfers affect stock levels

### StockStream
- **Dependency**: Requires ProductsStream to run first
- **Batch**: Fetches stock per-SKU in batches

### TransfersStream
- **Required param**: `dateFrom` or `dateExportedSince` (handled by DateFilteredStream)
- **Status codes**: N=New, Q=Request, T=Transferred, R=Received, C=Cancelled

## Type Coercion (in TilroyStream.post_process)

The base class handles:
1. Converting `None` and `"NA"` to proper nulls
2. Converting string numbers to floats
3. **Important**: Converting ID fields to integers (not floats) to avoid `.0` suffix
   - Fields: `tilroyId`, `sourceId`, `number`, `tilroy_id`, `source_id`, `legalEntityId`, `tenantId`, `shopNumber`, `shop_number`
4. Stringifying nested objects/arrays for CSV compatibility
