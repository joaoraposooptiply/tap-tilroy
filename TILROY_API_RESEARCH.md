# Tilroy API Research Documentation

This document provides a comprehensive analysis of all Tilroy APIs and their endpoints, extracted from the official API documentation.

---

## API Overview

| API | Version | Base URL | Endpoints | Primary Use |
|-----|---------|----------|-----------|-------------|
| **activityapi** | 1.1.5 | `https://api.tilroy.com/activityapi/production` | 4 | Workshop & activity management |
| **addonapi** | 2.1.4 | Internal | 3 | Product addons |
| **assetapi** | 1.2.4 | Internal | 3 | Asset management (bikes/velopass) |
| **authenticationapi** | 3.1.2 | `https://api.tilroy.com/authenticationapi/production` | 6 | Authentication & tokens |
| **basketapi** | 2.43.0 | `https://api.tilroy.com/basketapi2` | 29 | Shopping basket management |
| **customerapi** | 2.33.1 | `https://api.tilroy.com/customerapi/production` | 23 | Customer management |
| **deliveryapi** | 1.0.0 | `https://api.tilroy.com/deliveryapi/production` | 4 | Delivery services |
| **deliverynoteapi** | 2.3.0 | `https://api.tilroy.com/deliverynoteapi/production` | 1 | Delivery notes export |
| **featureapi** | 2.10.0 | `https://api.tilroy.com/featureapi/production` | 5 | Product features |
| **orderapi** | 3.30.0 | `https://api.tilroy.com/orderapi/production` | 34 | Order management |
| **paymentrequestapi** | 2.1.3 | `http://api.tilroy.com/paymentreqestapi/production/` | 3 | Payment requests |
| **priceapi** | 2.11.0 | `https://api.tilroy.com/priceapi/production` | 9 | Price management |
| **processlogapi** | 1.1.4 | Internal | 3 | Process logging |
| **productapi** | 2.39.0 | `https://api.tilroy.com/product-bulk/production` | 9 | Product management |
| **promotionapi** | 2.5.0 | `https://api.tilroy.com/promotionapi/production` | 8 | Promotions & discounts |
| **purchaseapi** | 3.4.1 | `https://api.tilroy.com/purchaseapi/production` | 8 | Purchase orders |
| **quotationapi** | 1.8.0 | `https://api.tilroy.com/quotationapi/production` | 9 | Quotations |
| **saleapi** | 2.12.1 | `https://api.tilroy.com/saleapi/production` | 8 | Sales transactions |
| **shipmentapi** | 1.7.3 | `https://api.tilroy.com/shipmentapi/production` | 2 | Shipments (PostNL) |
| **shopapi** | 2.5.0 | `https://api.tilroy.com/shopapi/production` | 11 | Shop management |
| **stockapi** | 2.8.4 | `https://api.tilroy.com/stockapi/production` | 7 | Stock management |
| **transferapi** | 2.7.0 | `https://api.tilroy.com/transferapi/production` | 3 | Stock transfers |
| **userapi** | 1.0.3 | `https://api.tilroy.com/userapi/production` | 3 | User management |
| **voucherapi** | 2.8.0 | `https://api.tilroy.com/voucherapi/production` | 11 | Vouchers & loyalty |

---

## Authentication

All APIs require two headers:
- `Tilroy-Api-Key` - Tenant identification key
- `x-api-key` - AWS API Gateway key

---

## Key APIs for Data Extraction (tap-tilroy)

### 1. Product API (`productapi`)

**Base URL:** `https://api.tilroy.com/product-bulk/production`

| Method | Endpoint | Description | Pagination |
|--------|----------|-------------|------------|
| GET | `/products` | Search products | `page`, `count` (max 1000) |
| GET | `/products/{sourceId}` | Get product by sourceId | - |
| GET | `/v2/products/{tilroyId}` | Get product by tilroyId | - |
| GET | `/suppliers` | Get all suppliers | - |
| GET | `/export/products` | Export products (incremental) | `page`, `count`, `dateExportedSince` |

**Key Parameters:**
- `count` - max 1000 per page
- `page` - page number
- `fields` - comma-separated list of fields to return
- `dateExportedSince` - filter by export date (ISO 8601)
- `skuSourceId`, `code`, `barcodes` - search filters

**Response Headers:**
- `X-Paging-CurrentPage`
- `X-Paging-ItemCount`
- `X-Paging-ItemsPerPage`
- `X-Paging-PageCount`

---

### 2. Sale API (`saleapi`)

**Base URL:** `https://api.tilroy.com/saleapi/production`

| Method | Endpoint | Description | Pagination |
|--------|----------|-------------|------------|
| GET | `/export/sales` | Export sales | `page`, `count`, `dateExportedSince` |
| GET | `/export/saleshistory` | Sales history | `page`, `count`, `dateFrom`, `dateTo` |
| GET | `/export/suppliersales` | Supplier sales | `page`, `count`, `dateFrom` |
| GET | `/sales` | Get sales | `page` OR `lastId`, `count` |
| GET | `/sales/{saleId}` | Get sale by ID | - |

**Key Parameters:**
- `count` - max 100 per page
- `page` - page-based pagination (requires customer filter)
- `lastId` - cursor-based pagination (customer filter optional)
- `dateFrom`, `dateTo` - date range filter
- `dateExportedSince` - filter by export date
- `customerSourceId`, `customerTilroyId` - customer filters
- `excludeInvoiceLinks` - improve response times

**Important Notes:**
- When using `page` parameter, customer filter is **required**
- When using `lastId` parameter, customer filter is **optional**
- Cannot use `page` and `lastId` together

---

### 3. Stock API (`stockapi`)

**Base URL:** `https://api.tilroy.com/stockapi/production`

| Method | Endpoint | Description | Pagination |
|--------|----------|-------------|------------|
| GET | `/stock` | Get stock levels | `page`, `count` |
| GET | `/stock/{stockTilroyId}` | Get stock by ID | - |
| GET | `/stockchanges` | Get stock changes | `page`, `count`, `dateFrom`, `dateTo` |
| GET | `/export/stockdeltas` | Export stock deltas | `page`, `count`, `dateExportedSince` |

**Key Parameters:**
- `count` - max 100 per page (default 10)
- `page` - page number
- `skuTilroyId` - comma-separated list of SKU IDs
- `skuSourceId` - single SKU source ID
- `shopNumber`, `shopTilroyId` - shop filters
- `dateFrom`, `dateTo` - **required** for `/stockchanges`
- `dateExportedSince` - **required** for `/export/stockdeltas`
- `withQtyOrdered` - include ordered quantity

**Stock Response Schema:**
```json
{
  "tilroyId": "sku_10926674_shop_43",
  "sku": { "tilroyId": "10926674", "sourceId": "..." },
  "shop": { "tilroyId": "43", "number": 101 },
  "location1": "AA01",
  "location2": "AA02",
  "qty": {
    "available": 5,
    "ideal": 5,
    "max": 10,
    "requested": 2,
    "transfered": 2,
    "ordered": 4
  },
  "refill": 2,
  "dateUpdated": "2022-07-22T13:44:23.164Z"
}
```

---

### 4. Price API (`priceapi`)

**Base URL:** `https://api.tilroy.com/priceapi/production`

| Method | Endpoint | Description | Pagination |
|--------|----------|-------------|------------|
| GET | `/prices` | Get current prices for multiple SKUs | - |
| GET | `/prices/{skuId}` | Get current price for single SKU | - |
| GET | `/price/rules` | Get all price rules | `page` OR `lastId`, `count` |
| GET | `/price/rules/{skuId}` | Get price rules for SKU | `page`, `count` |

**Key Parameters:**
- `count` - items per page
- `page` - page-based pagination
- `lastId` - cursor-based pagination (cannot use with `page`)
- `dateModified` - filter by modification date (ISO 8601)
- `skuIds`, `skuSourceIds` - comma-separated SKU identifiers (max 100)
- `shopId`, `shopNumber` - shop filter
- `countryCode` - country filter (ISO 2-letter)
- `priceRange` - "retail", "wholesale", or custom

**Price Rule Response Schema:**
```json
{
  "tilroyId": "sku32725560_store7570",
  "shop": { "tilroyId": "7570", "number": "101" },
  "country": { "tilroyId": "1", "code": "BE" },
  "sku": { "tilroyId": "32725560", "sourceId": "..." },
  "tenantId": "...",
  "prices": [
    {
      "tilroyId": "...",
      "price": 19.99,
      "type": "standard",
      "quantity": 1,
      "startDate": "2024-01-01T00:00:00Z",
      "endDate": null,
      "dateCreated": "2024-01-01T00:00:00Z",
      "dateModified": "2024-06-01T12:00:00Z"
    }
  ],
  "dateModified": "2024-06-01T12:00:00Z"
}
```

---

### 5. Shop API (`shopapi`)

**Base URL:** `https://api.tilroy.com/shopapi/production`

| Method | Endpoint | Description | Pagination |
|--------|----------|-------------|------------|
| GET | `/shops` | Get all shops | - |
| GET | `/export/cashups` | Export cash-ups | `page`, `count`, `dateFrom` |
| GET | `/export/pettycashes` | Export petty cashes | `page`, `count`, `dateFrom` |
| GET | `/export/moneytransfers` | Export money transfers | `page`, `count`, `dateFrom` |
| GET | `/moneylocations` | Get money locations | - |

---

### 6. Purchase API (`purchaseapi`)

**Base URL:** `https://api.tilroy.com/purchaseapi/production`

| Method | Endpoint | Description | Pagination |
|--------|----------|-------------|------------|
| GET | `/export/{type}` | Export purchase data | `page`, `count`, `dateFrom` |
| GET | `/purchaseorders` | Get purchase orders | `page`, `count`, `orderDateFrom` |
| GET | `/purchaseorders/{id}` | Get single purchase order | - |

**Export Types:**
- `orders` - Purchase orders
- `receipts` - Purchase receipts

---

### 7. Order API (`orderapi`)

**Base URL:** `https://api.tilroy.com/orderapi/production`

| Method | Endpoint | Description | Pagination |
|--------|----------|-------------|------------|
| GET | `/orders` | Get orders | `page`, `count` |
| GET | `/v2/orders` | Get orders v2 | `page`, `count` |
| GET | `/v2/orders/{orderId}` | Get single order | - |
| GET | `/export/orders` | Export orders | `page`, `count`, `dateFrom` |

---

### 8. Customer API (`customerapi`)

**Base URL:** `https://api.tilroy.com/customerapi/production`

| Method | Endpoint | Description | Pagination |
|--------|----------|-------------|------------|
| GET | `/customers` | Search customers | `page`, `count` |
| GET | `/customers/{customerId}` | Get customer by ID | - |
| GET | `/customers/{customerId}/addresses` | Get customer addresses | - |
| GET | `/customers/{customerId}/cards` | Get customer cards | - |

---

### 9. Transfer API (`transferapi`)

**Base URL:** `https://api.tilroy.com/transferapi/production`

| Method | Endpoint | Description | Pagination |
|--------|----------|-------------|------------|
| GET | `/export/transfers` | Export stock transfers | `page`, `count`, `dateFrom` |

---

### 10. Voucher API (`voucherapi`)

**Base URL:** `https://api.tilroy.com/voucherapi/production`

| Method | Endpoint | Description | Pagination |
|--------|----------|-------------|------------|
| GET | `/v2/vouchers` | Get vouchers | various filters |
| GET | `/v2/vouchers/{voucherId}` | Get voucher by ID | - |
| GET | `/budget/{customerSourceId}/balance` | Get customer budget balance | - |

---

## Pagination Patterns

### 1. Page-Based Pagination
```
GET /endpoint?page=1&count=100
```
**Response Headers:**
- `X-Paging-CurrentPage` - Current page number
- `X-Paging-ItemCount` - Total items across all pages
- `X-Paging-ItemsPerPage` - Items per page
- `X-Paging-PageCount` - Total pages

### 2. Cursor-Based Pagination (lastId)
```
GET /endpoint?lastId=xxx&count=100
```
- Uses `lastId` from last record of previous page
- Cannot be combined with `page` parameter
- More efficient for large datasets

### 3. Date-Based Filtering
```
GET /endpoint?dateFrom=2024-01-01&dateTo=2024-12-31
GET /endpoint?dateExportedSince=2024-01-01T00:00:00Z
GET /endpoint?dateModified=2024-01-01T00:00:00Z
```

---

## Common Response Patterns

### Success Response
```json
[
  { "tilroyId": "123", ... },
  { "tilroyId": "456", ... }
]
```

### Error Response
```json
{
  "code": "ERROR_CODE",
  "message": "Human-readable error description"
}
```

---

## Currently Implemented Streams in tap-tilroy

| Stream | API | Endpoint | Status |
|--------|-----|----------|--------|
| `shops` | shopapi | `/shops` | ✅ Implemented |
| `products` | productapi | `/products` + `/export/products` | ✅ Implemented (Dynamic routing) |
| `suppliers` | productapi | `/suppliers` | ✅ Implemented |
| `purchase_orders` | purchaseapi | `/purchaseorders` + `/export/orders` | ✅ Implemented (Dynamic routing) |
| `stock_changes` | stockapi | `/stockchanges` | ✅ Implemented |
| `sales` | saleapi | `/sales` | ✅ Implemented (lastId pagination) |
| `prices` | priceapi | `/price/rules` | ✅ Implemented (lastId pagination) |
| `stock` | stockapi | `/stock` | ✅ Implemented (batched SKU IDs) |

---

## Potential Additional Streams

Based on the API research, these endpoints could be added:

| Stream | API | Endpoint | Use Case |
|--------|-----|----------|----------|
| `orders` | orderapi | `/export/orders` | Order tracking |
| `customers` | customerapi | `/customers` | Customer data |
| `transfers` | transferapi | `/export/transfers` | Stock transfers |
| `vouchers` | voucherapi | `/v2/vouchers` | Voucher/loyalty data |
| `delivery_notes` | deliverynoteapi | `/export/deliverynotes` | Delivery tracking |
| `quotations` | quotationapi | `/quotations` | Quote management |
| `promotions` | promotionapi | `/v1/discountcodes` | Promotion data |
| `cashups` | shopapi | `/export/cashups` | Cash reconciliation |

---

## API Documentation Files

All extracted OpenAPI specifications are saved in `/workspace/tilroy_docs/` as JSON files:
- `activityapi_spec.json`
- `customerapi_spec.json`
- `orderapi_spec.json`
- `priceapi_spec.json`
- `productapi_spec.json`
- `saleapi_spec.json`
- `shopapi_spec.json`
- `stockapi_spec.json`
- ... and more

---

*Last Updated: January 2026*
*Generated from official Tilroy API documentation*
