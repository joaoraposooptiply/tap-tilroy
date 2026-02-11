"""Product and Supplier streams for Tilroy API."""

from __future__ import annotations

import typing as t
from datetime import datetime, timezone

from singer_sdk import typing as th

from tap_tilroy.client import DynamicRoutingStream, TilroyStream

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


# Full product detail (including sku.code) only from single-product endpoint
PRODUCT_V2_PATH = "/product-bulk/production/v2/products"


class ProductsStream(DynamicRoutingStream):
    """Lightweight product stream that paginates the list endpoint directly.

    Emits records from the list response (no individual v2 fetches).
    Primary purpose: collect SKU IDs for use by PricesStream and StockStream.

    - First sync (no state): GET /product-bulk/production/products
    - Incremental: GET /product-bulk/production/export/products?dateFrom=X
    """

    name = "products"
    historical_path = "/product-bulk/production/products"
    incremental_path = "/product-bulk/production/export/products"
    primary_keys: t.ClassVar[list[str]] = ["tilroyId"]
    replication_key = "extraction_timestamp"  # Synthetic replication key
    replication_method = "INCREMENTAL"
    records_jsonpath = "$[*]"
    default_count = 1000  # Product API allows up to 1000 per page

    # Class-level storage for SKU tilroyIds (shared across instances).
    # These are colours[].skus[].tilroyId (SKU-level), NOT product tilroyId. Used by prices and stock.
    _collected_sku_ids: t.ClassVar[set[str]] = set()

    schema = th.PropertiesList(
        th.Property("tilroyId", th.CustomType({"type": ["string", "integer"]})),
        th.Property("sourceId", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("code", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property(
            "descriptions",
            th.ArrayType(
                th.ObjectType(
                    th.Property("languageCode", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property("standard", th.CustomType({"type": ["string", "number", "null"]})),
                )
            ),
        ),
        th.Property("supplier", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("brand", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property(
            "colours",
            th.ArrayType(
                th.ObjectType(
                    th.Property("tilroyId", th.CustomType({"type": ["string", "integer"]})),
                    th.Property("sourceId", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property("code", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property(
                        "skus",
                        th.ArrayType(
                            th.ObjectType(
                                th.Property("tilroyId", th.CustomType({"type": ["string", "integer"]})),
                                th.Property("sourceId", th.CustomType({"type": ["string", "number", "null"]})),
                                th.Property("code", th.CustomType({"type": ["string", "number", "null"]})),
                                th.Property("costPrice", th.NumberType),
                                th.Property(
                                    "barcodes",
                                    th.ArrayType(
                                        th.ObjectType(
                                            th.Property("code", th.CustomType({"type": ["string", "number", "null"]})),
                                            th.Property("quantity", th.NumberType),
                                            th.Property("isInternal", th.BooleanType),
                                        )
                                    ),
                                ),
                                th.Property(
                                    "size",
                                    th.ObjectType(
                                        th.Property("code", th.CustomType({"type": ["string", "number", "null"]})),
                                    ),
                                ),
                                th.Property("rrp", th.CustomType({"type": ["array", "object", "string", "null"]})),
                            )
                        ),
                    ),
                    th.Property("pictures", th.ArrayType(th.ObjectType())),
                )
            ),
        ),
        th.Property("isUsed", th.BooleanType),
        th.Property("suppliers", th.CustomType({"type": ["array", "object", "string", "null"]})),
        th.Property("extraction_timestamp", th.DateTimeType),
    ).to_dict()

    def request_records(self, context: Context | None) -> t.Iterable[dict]:
        """Paginate list endpoint directly. No individual v2 fetches."""
        is_incremental = self._has_existing_state()
        list_path = self.incremental_path if is_incremental else self.historical_path

        self.logger.info(
            f"[{self.name}] Using {'INCREMENTAL' if is_incremental else 'HISTORICAL'} path: {list_path}"
        )

        page = 1
        total_yielded = 0

        while True:
            params: dict[str, t.Any] = {"count": self.default_count, "page": page}

            # Export endpoint requires dateFrom
            if is_incremental:
                start_date = self._get_start_date(context)
                params["dateFrom"] = start_date.strftime("%Y-%m-%d")

            list_url = f"{self.url_base.rstrip('/')}{list_path}"
            prepared = self.build_prepared_request(
                method="GET",
                url=list_url,
                params=params,
                headers=self.http_headers,
            )

            try:
                response = self._request(prepared, context)
            except Exception as e:
                self.logger.error(f"[{self.name}] Request failed: {e}")
                break
            if response.status_code != 200:
                self.logger.error(
                    f"[{self.name}] API error {response.status_code}: {response.text[:500]}"
                )
                break

            records = list(self.parse_response(response))
            page_count = len(records)

            for record in records:
                processed = self.post_process(record, context)
                if processed:
                    total_yielded += 1
                    yield processed

            # Check pagination headers
            total_pages = None
            try:
                current = int(response.headers.get("X-Paging-CurrentPage", 1))
                total_pages = int(response.headers.get("X-Paging-PageCount", 1))
            except (ValueError, TypeError):
                current = page

            page_info = f"page {page}/{total_pages}" if total_pages else f"page {page}"
            self.logger.info(
                f"[{self.name}] {page_info}: {page_count} products (total: {total_yielded})"
            )

            if page_count < self.default_count:
                break
            if total_pages and current >= total_pages:
                break

            page += 1

        self.logger.info(f"[{self.name}] Done: {total_yielded} products")

    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        """Add extraction timestamp and collect SKU IDs."""
        if not row:
            return None

        row["extraction_timestamp"] = datetime.now(timezone.utc).isoformat()
        self._collect_sku_ids(row)

        return row

    def _collect_sku_ids(self, product: dict) -> None:
        """Extract and store SKU IDs from product record."""
        colours = product.get("colours", [])
        if not isinstance(colours, list):
            return

        for colour in colours:
            skus = colour.get("skus", [])
            if not isinstance(skus, list):
                continue

            for sku in skus:
                sku_id = sku.get("tilroyId")
                if sku_id:
                    sku_id_str = str(sku_id)
                    if sku_id_str not in self._collected_sku_ids:
                        self._collected_sku_ids.add(sku_id_str)

                        if len(self._collected_sku_ids) % 500 == 0:
                            self.logger.info(
                                f"[{self.name}] Collected {len(self._collected_sku_ids)} SKU IDs..."
                            )

    @classmethod
    def get_collected_sku_ids(cls) -> list[str]:
        """Get the collected SKU IDs for use by other streams."""
        return list(cls._collected_sku_ids)

    @classmethod
    def clear_collected_sku_ids(cls) -> None:
        """Clear the collected SKU IDs."""
        cls._collected_sku_ids.clear()

    def finalize_child_contexts(self) -> None:
        """Log final SKU collection statistics."""
        self.logger.info(
            f"[{self.name}] Final: {len(self._collected_sku_ids)} unique SKU IDs collected"
        )


class ProductDetailsStream(DynamicRoutingStream):
    """Full product detail stream using individual v2/products/{id} fetches.

    Lists products from the bulk/export endpoint, then fetches each product
    individually via GET v2/products/{id} for the complete schema (sku.code,
    size.sizeOrder/skuCode, pictures, web descriptions, etc.).

    - First sync (no state): list from /products, then GET v2/products/{id}
    - Incremental: list from /export/products?dateFrom=X, then GET v2/products/{id}
    """

    name = "product_details"
    historical_path = "/product-bulk/production/products"
    incremental_path = "/product-bulk/production/export/products"
    primary_keys: t.ClassVar[list[str]] = ["tilroyId"]
    replication_key = "extraction_timestamp"
    replication_method = "INCREMENTAL"
    records_jsonpath = "$[*]"
    default_count = 1000

    # Full singular-response schema so no fields are dropped by schema validation
    schema = th.PropertiesList(
        th.Property("tilroyId", th.CustomType({"type": ["string", "integer"]})),
        th.Property("sourceId", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("code", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("codeAlt", th.StringType),
        th.Property("serialNumberSale", th.BooleanType),
        th.Property("supplierReference", th.StringType),
        th.Property("supplierDescription", th.StringType),
        th.Property("visibleOnline", th.BooleanType),
        th.Property("priceInfo", th.StringType),
        th.Property("nextSaleDiscounts", th.ObjectType()),
        th.Property(
            "descriptions",
            th.ArrayType(
                th.ObjectType(
                    th.Property("languageCode", th.StringType),
                    th.Property("standard", th.StringType),
                    th.Property("web", th.StringType),
                    th.Property("webName", th.StringType),
                    th.Property("seoTitle", th.StringType),
                    th.Property("seoDescription", th.StringType),
                    th.Property("ticket", th.StringType),
                    th.Property("webShort", th.StringType),
                    th.Property("priceLabelDescription", th.StringType),
                    th.Property("canonicalUrl", th.StringType),
                    th.Property("videoPath", th.StringType),
                    th.Property("freeTexts", th.ArrayType(th.StringType)),
                )
            ),
        ),
        th.Property("supplier", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("brand", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("uom", th.ObjectType(
            th.Property("code", th.StringType),
            th.Property("showOnline", th.BooleanType),
        )),
        th.Property("sizeRange", th.CustomType({"type": ["object", "null"]})),
        th.Property("countryVats", th.ArrayType(th.ObjectType())),
        th.Property("gender", th.CustomType({"type": ["object", "null"]})),
        th.Property("groupLevels", th.ArrayType(th.ObjectType())),
        th.Property(
            "colours",
            th.ArrayType(
                th.ObjectType(
                    th.Property("tilroyId", th.CustomType({"type": ["string", "integer", "null"]})),
                    th.Property("sourceId", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property("code", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property("supplierReference", th.StringType),
                    th.Property("baseColour", th.CustomType({"type": ["object", "null"]})),
                    th.Property("descriptions", th.ArrayType(th.ObjectType())),
                    th.Property("productColourDescriptions", th.ArrayType(th.ObjectType())),
                    th.Property("season", th.CustomType({"type": ["object", "null"]})),
                    th.Property(
                        "skus",
                        th.ArrayType(
                            th.ObjectType(
                                th.Property("tilroyId", th.CustomType({"type": ["string", "integer"]})),
                                th.Property("sourceId", th.CustomType({"type": ["string", "number", "null"]})),
                                th.Property("code", th.CustomType({"type": ["string", "number", "null"]})),
                                th.Property("consumerPrice", th.NumberType),
                                th.Property("costPrice", th.NumberType),
                                th.Property("maxDiscount", th.NumberType),
                                th.Property("lifeStatus", th.StringType),
                                th.Property("wholeSalePrice", th.NumberType),
                                th.Property("origin", th.StringType),
                                th.Property("length", th.NumberType),
                                th.Property("width", th.NumberType),
                                th.Property("height", th.NumberType),
                                th.Property("content", th.NumberType),
                                th.Property("MOQ", th.NumberType),
                                th.Property("IOQ", th.NumberType),
                                th.Property(
                                    "barcodes",
                                    th.ArrayType(
                                        th.ObjectType(
                                            th.Property("code", th.CustomType({"type": ["string", "number", "null"]})),
                                            th.Property("quantity", th.NumberType),
                                            th.Property("isInternal", th.BooleanType),
                                        )
                                    ),
                                ),
                                th.Property(
                                    "size",
                                    th.ObjectType(
                                        th.Property("code", th.CustomType({"type": ["string", "number", "null"]})),
                                        th.Property("sizeOrder", th.NumberType),
                                        th.Property("skuCode", th.StringType),
                                    ),
                                ),
                                th.Property("weight", th.CustomType({"type": ["object", "null"]})),
                            )
                        ),
                    ),
                    th.Property(
                        "pictures",
                        th.ArrayType(
                            th.ObjectType(
                                th.Property("name", th.StringType),
                                th.Property("isDefault", th.BooleanType),
                                th.Property("sortOrder", th.NumberType),
                                th.Property("swatch", th.BooleanType),
                                th.Property("showOnline", th.BooleanType),
                            )
                        ),
                    ),
                )
            ),
        ),
        th.Property("used", th.CustomType({"type": ["object", "null"]})),
        th.Property("configurator", th.CustomType({"type": ["object", "null"]})),
        th.Property("readOnly", th.BooleanType),
        th.Property("deliverynoteAllowed", th.BooleanType),
        th.Property("extraction_timestamp", th.DateTimeType),
    ).to_dict()

    def request_records(self, context: Context | None) -> t.Iterable[dict]:
        """List products, then fetch each via v2/products/{id} for full schema."""
        is_incremental = self._has_existing_state()
        list_path = self.incremental_path if is_incremental else self.historical_path

        self.logger.info(
            f"[{self.name}] Using {'INCREMENTAL' if is_incremental else 'HISTORICAL'} path: {list_path}"
        )

        page = 1
        total_yielded = 0

        while True:
            params: dict[str, t.Any] = {"count": self.default_count, "page": page}

            if is_incremental:
                start_date = self._get_start_date(context)
                params["dateFrom"] = start_date.strftime("%Y-%m-%d")

            list_url = f"{self.url_base.rstrip('/')}{list_path}"
            prepared = self.build_prepared_request(
                method="GET",
                url=list_url,
                params=params,
                headers=self.http_headers,
            )

            self.logger.info(f"[{self.name}] List page {page}: GET {list_url}")

            try:
                response = self._request(prepared, context)
            except Exception as e:
                self.logger.error(f"[{self.name}] List request failed: {e}")
                break
            if response.status_code != 200:
                self.logger.error(
                    f"[{self.name}] List API error {response.status_code}: {response.text[:500]}"
                )
                break

            records = list(self.parse_response(response))
            page_count = len(records)

            for product in records:
                tilroy_id = product.get("tilroyId")
                if tilroy_id is None:
                    continue
                single_url = f"{self.url_base.rstrip('/')}{PRODUCT_V2_PATH}/{tilroy_id}"
                single_prepared = self.build_prepared_request(
                    method="GET",
                    url=single_url,
                    params={},
                    headers=self.http_headers,
                )
                try:
                    single_response = self._request(single_prepared, context)
                except Exception as e:
                    self.logger.warning(
                        f"[{self.name}] Single product {tilroy_id} failed: {e}"
                    )
                    continue
                if single_response.status_code != 200:
                    self.logger.warning(
                        f"[{self.name}] Single product {tilroy_id}: {single_response.status_code}"
                    )
                    continue
                try:
                    full_product = single_response.json()
                except Exception:
                    continue
                if isinstance(full_product, dict) and full_product.get("tilroyId") is not None:
                    processed = self.post_process(full_product, context)
                    if processed:
                        total_yielded += 1
                        yield processed

            # Check pagination headers
            total_pages = None
            try:
                current = int(response.headers.get("X-Paging-CurrentPage", 1))
                total_pages = int(response.headers.get("X-Paging-PageCount", 1))
            except (ValueError, TypeError):
                current = page

            page_info = f"page {page}/{total_pages}" if total_pages else f"page {page}"
            self.logger.info(
                f"[{self.name}] {page_info}: {page_count} listed -> {total_yielded} fetched"
            )

            if page_count < self.default_count:
                break
            if total_pages and current >= total_pages:
                break

            page += 1

        self.logger.info(
            f"[{self.name}] Done: {total_yielded} products (full detail from v2/products/{{id}})"
        )

    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        """Add extraction_timestamp only; pass through full singular response."""
        if not row:
            return None
        row["extraction_timestamp"] = datetime.now(timezone.utc).isoformat()
        return row


class SuppliersStream(TilroyStream):
    """Stream for Tilroy suppliers.

    Fetches supplier master data. This is typically a small dataset.
    """

    name = "suppliers"
    path = "/product-bulk/production/suppliers"
    primary_keys: t.ClassVar[list[str]] = ["tilroyId"]
    replication_key = None
    replication_method = "FULL_TABLE"
    records_jsonpath = "$[*]"
    default_count = 10000  # Suppliers are typically few, fetch all at once

    schema = th.PropertiesList(
        th.Property("tilroyId", th.IntegerType),
        th.Property("code", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("name", th.CustomType({"type": ["string", "number", "null"]})),
    ).to_dict()
