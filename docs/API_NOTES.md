# Tilroy API Notes & Gotchas

## Authentication

Two headers required on ALL requests:
```
Tilroy-Api-Key: <tilroy_api_key>
x-api-key: <x_api_key>
```

## Base URL

Production: `https://api.tilroy.com`

## API Specs (OpenAPI)

Downloaded specs available at:
- `/workspace/tilroy_docs/` (if folder exists)
- Online: `https://tilroy-apidocs-prd.s3.eu-west-1.amazonaws.com/{api}.html`

Available APIs:
- `shopapi` - Shops
- `productapi` - Products
- `saleapi` - Sales
- `stockapi` - Stock
- `priceapi` - Prices
- `purchaseorderapi` - Purchase orders
- `supplierapi` - Suppliers
- `transferapi` - Transfers

## Pagination Patterns

### Page-based (most /export endpoints)
```
?page=1&count=100
```
Headers return: `X-Paging-PageCount`, `X-Paging-CurrentPage`, `X-Paging-ItemCount`

### Cursor-based (prices)
```
?lastId=&count=100        # First page (empty lastId)
?lastId=12345&count=100   # Next page
```

## Known Gotchas

### 1. Sales endpoint requires customer filter
- ❌ `/saleapi/production/sales` - Requires `customerSourceId` or `customerTilroyId`
- ✅ `/saleapi/production/export/sales` - Works with just `dateFrom`

### 2. Shops endpoint is non-paginated
- Does NOT support `page` or `count` parameters
- Returns all shops in single response
- SDK's default pagination will break - must override `request_records()`

### 3. Transfer API requires date filter
- Must include `dateFrom` OR `dateExportedSince`
- Without it: `400 Bad Request`

### 4. ID fields come as floats
- API returns numbers like `2131.0` for IDs
- Must convert to int in post_process to avoid `.0` suffix in output

### 5. Bulk vs Export endpoints
- Bulk endpoints: Full dataset, for initial sync
- Export endpoints: Incremental, for ongoing sync
- Some streams (products, purchase_orders) switch dynamically based on state

### 6. Empty responses
- API returns `[]` (empty array) when no data, not an error
- Pagination should stop when empty response received

## Custom Endpoints

The tenant may have custom endpoints built specifically for them.
Check with the user before removing any endpoints that seem non-standard.

## Rate Limits

Not documented, but be cautious with:
- Large batch sizes
- Parallel requests
- Stock endpoint (one request per SKU)
