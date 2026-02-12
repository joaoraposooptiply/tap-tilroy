"""Stock streams for Tilroy API."""

from __future__ import annotations

import time
import typing as t
from datetime import datetime, timedelta

from singer_sdk import typing as th

from tap_tilroy.client import DateFilteredStream, TilroyStream

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class StockDeltasStream(DateFilteredStream):
    """Stream for Tilroy stock deltas (export endpoint).

    Uses /export/stockdeltas endpoint which returns actual inventory change events
    with delta values (qtyDelta, qtyTransferredDelta, etc.) for tracking movements
    like transfers, sales, corrections, etc.

    This is the endpoint that captures transfer-related stock movements.
    Requires dateExportedSince parameter for filtering.
    """

    name = "stock_deltas"
    path = "/stockapi/production/export/stockdeltas"
    primary_keys: t.ClassVar[list[str]] = ["tilroyId"]
    replication_key = "dateExported"
    replication_method = "INCREMENTAL"
    records_jsonpath = "$[*]"
    default_count = 100

    schema = th.PropertiesList(
        th.Property("tilroyId", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("timestamp", th.DateTimeType),
        th.Property("sourceId", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("modificationType", th.CustomType({"type": ["string", "null"]})),
        th.Property("reason", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("shop", th.CustomType({"type": ["object", "string", "null"]})),
        # Flattened shop fields for easier filtering
        th.Property("shop_tilroy_id", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("shop_number", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("shop_name", th.CustomType({"type": ["string", "null"]})),
        th.Property("shop_source_id", th.CustomType({"type": ["string", "null"]})),
        th.Property("product", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("colour", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("size", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("sku", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("qtyDelta", th.IntegerType),
        th.Property("qtyTransferredDelta", th.IntegerType),
        th.Property("qtyReservedDelta", th.IntegerType),
        th.Property("cause", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("dateExported", th.DateTimeType),
    ).to_dict()

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: int | None,
    ) -> dict[str, t.Any]:
        """Return URL parameters with dateExportedSince for the export endpoint.

        The /export/stockdeltas endpoint requires dateExportedSince in ISO datetime format.
        """
        params = {
            "count": self.default_count,
            "page": next_page_token or 1,
        }

        # Get start date from bookmark or config
        start_date = self._get_start_date(context)

        # Format as ISO datetime for this endpoint
        params["dateExportedSince"] = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")

        self.logger.info(
            f"[{self.name}] Fetching stock deltas since: {params['dateExportedSince']}"
        )

        return params

    def _get_start_date(self, context: Context | None) -> datetime:
        """Determine the start date for filtering."""
        # Try to get from bookmark
        bookmark_date = self.get_starting_timestamp(context)

        if bookmark_date:
            # Go back 1 day to avoid missing records at boundary
            if hasattr(bookmark_date, "date"):
                date_only = bookmark_date.date()
            else:
                date_only = bookmark_date
            return datetime.combine(date_only - timedelta(days=1), datetime.min.time())

        # Fall back to config start_date
        config_start = self.config.get("start_date", "2010-01-01T00:00:00Z")
        try:
            return datetime.fromisoformat(config_start.replace("Z", "+00:00")).replace(tzinfo=None)
        except ValueError:
            date_part = config_start.split("T")[0]
            return datetime.strptime(date_part, "%Y-%m-%d")

    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        """Post-process stock delta record."""
        row = super().post_process(row, context)
        if row:
            row = self._flatten_shop(row, "shop", "shop")
            row = self._stringify_nested_objects(row)
        return row


class StockChangesStream(TilroyStream):
    """Stream for Tilroy stock changes.

    Tracks inventory changes over time with date-based filtering.
    Iterates through shop numbers internally to avoid per-partition state bloat.
    """

    name = "stock_changes"
    path = "/stockapi/production/stockchanges"
    primary_keys: t.ClassVar[list[str]] = ["tilroyId"]
    replication_key = "dateUpdated"
    replication_method = "INCREMENTAL"
    records_jsonpath = "$[*]"
    default_count = 100

    # Schema matches /stockchanges API response
    schema = th.PropertiesList(
        th.Property("tilroyId", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("sku", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("shop", th.CustomType({"type": ["object", "string", "null"]})),
        # Flattened shop fields for easier filtering
        th.Property("shop_tilroy_id", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("shop_number", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("shop_name", th.CustomType({"type": ["string", "null"]})),
        th.Property("shop_source_id", th.CustomType({"type": ["string", "null"]})),
        th.Property("qty", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("refill", th.IntegerType),
        th.Property("dateCreated", th.DateTimeType),
        th.Property("dateUpdated", th.DateTimeType),
        th.Property("modificationType", th.CustomType({"type": ["string", "null"]})),
        th.Property("location1", th.CustomType({"type": ["string", "null"]})),
        th.Property("location2", th.CustomType({"type": ["string", "null"]})),
        th.Property("pickupLocation", th.CustomType({"type": ["string", "null"]})),
        th.Property("inAssortment", th.BooleanType),
        th.Property("lastExternalDelivery", th.CustomType({"type": ["object", "string", "null"]})),
    ).to_dict()

    def _get_start_date(self) -> datetime:
        """Determine the start date for filtering from global bookmark."""
        bookmark_date = self.get_starting_timestamp(None)

        if bookmark_date:
            self.logger.info(
                f"[{self.name}] Found bookmark: {bookmark_date} (type: {type(bookmark_date).__name__})"
            )
            if hasattr(bookmark_date, "date"):
                date_only = bookmark_date.date()
            else:
                date_only = bookmark_date
            result = datetime.combine(date_only - timedelta(days=1), datetime.min.time())
            self.logger.info(f"[{self.name}] Using dateFrom: {result.date()} (bookmark -1 day)")
            return result

        config_start = self.config.get("start_date", "2010-01-01T00:00:00Z")
        self.logger.info(f"[{self.name}] No bookmark found, using config start_date: {config_start}")
        date_part = config_start.split("T")[0]
        return datetime.strptime(date_part, "%Y-%m-%d")

    def _fetch_page(
        self,
        shop_number: int | None,
        page: int,
        start_date: datetime,
    ) -> tuple[list[dict], bool]:
        """Fetch a single page of stock changes.
        
        Args:
            shop_number: Shop number filter (optional).
            page: Page number.
            start_date: Start date for dateFrom.
            
        Returns:
            Tuple of (records, has_more_pages).
        """
        params = {
            "count": self.default_count,
            "page": page,
            "dateFrom": start_date.strftime("%Y-%m-%d"),
            "dateTo": datetime.now().strftime("%Y-%m-%d"),
        }

        if shop_number:
            params["shopNumber"] = shop_number

        prepared = self.build_prepared_request(
            method="GET",
            url=self.get_url(None),
            params=params,
            headers=self.http_headers,
        )
        response = self._request_with_backoff(prepared, None)

        current_page = int(response.headers.get("X-Paging-CurrentPage", 1))
        total_pages = int(response.headers.get("X-Paging-PageCount", 1))
        total_records_header = response.headers.get("X-Paging-TotalRecordCount", "?")
        has_more = current_page < total_pages

        records = list(self.parse_response(response))

        self.logger.info(
            f"[{self.name}] API response: page {current_page}/{total_pages}, "
            f"{len(records)} records (total: {total_records_header}), "
            f"shop={shop_number}, dateFrom={start_date.date()}"
        )

        return records, has_more

    def _fetch_all_for_shop(
        self,
        shop_number: int | None,
        start_date: datetime,
    ) -> t.Iterable[dict]:
        """Fetch all pages for a shop number.
        
        Args:
            shop_number: Shop number filter.
            start_date: Start date for filtering.
            
        Yields:
            Stock change records.
        """
        page = 1
        while True:
            records, has_more = self._fetch_page(shop_number, page, start_date)
            
            # Break if no records returned (avoid infinite loop)
            if not records:
                break
            
            for record in records:
                yield record
            
            if not has_more:
                break
            page += 1

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Fetch stock changes, iterating through shops internally.

        This avoids partition-based state. Single global bookmark is maintained.
        """
        start_date = self._get_start_date()
        shop_numbers = getattr(self._tap, "_resolved_shop_numbers", [])
        start_time = time.time()
        total_records = 0

        if not shop_numbers:
            # No shop filter - fetch all
            self.logger.info(
                f"=== [{self.name}] STARTING INCREMENTAL SYNC === "
                f"(all shops from {start_date.date()})"
            )
            for record in self._fetch_all_for_shop(None, start_date):
                total_records += 1
                yield record
        else:
            # Iterate through each shop
            self.logger.info(
                f"=== [{self.name}] STARTING INCREMENTAL SYNC === "
                f"({len(shop_numbers)} shops from {start_date.date()})"
            )
            for i, shop_num in enumerate(shop_numbers, 1):
                shop_start = time.time()
                shop_records = 0
                for record in self._fetch_all_for_shop(shop_num, start_date):
                    shop_records += 1
                    total_records += 1
                    yield record

                self.logger.info(
                    f"[{self.name}] Shop {shop_num} ({i}/{len(shop_numbers)}): "
                    f"+{shop_records:,} records in {time.time()-shop_start:.1f}s"
                )

        elapsed = time.time() - start_time
        self.logger.info(
            f"=== [{self.name}] INCREMENTAL SYNC COMPLETE === "
            f"{total_records:,} records in {elapsed:.0f}s"
        )

    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        """Post-process stock change record."""
        row = super().post_process(row, context)
        if row:
            row = self._flatten_shop(row, "shop", "shop")
            row = self._stringify_nested_objects(row)
        return row


class StockStream(TilroyStream):
    """Stream for Tilroy current stock levels.

    The /stock API requires skuTilroyId, so we batch SKU IDs collected by
    ProductsStream (which runs first). This gives complete stock for all products.
    """

    name = "stock"
    path = "/stockapi/production/stock"
    primary_keys: t.ClassVar[list[str]] = ["tilroyId"]
    replication_key = None
    replication_method = "FULL_TABLE"
    records_jsonpath = "$[*]"

    # Batch configuration
    _batch_size: int = 150  # SKUs per API request (comma-separated in URL)

    schema = th.PropertiesList(
        th.Property("tilroyId", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("sku", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("location1", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("location2", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("qty", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("refill", th.IntegerType),
        th.Property("dateUpdated", th.DateTimeType),
        th.Property("shop", th.CustomType({"type": ["object", "string", "null"]})),
        # Flattened shop fields for easier filtering
        th.Property("shop_tilroy_id", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("shop_number", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("shop_name", th.CustomType({"type": ["string", "null"]})),
        th.Property("shop_source_id", th.CustomType({"type": ["string", "null"]})),
    ).to_dict()

    def request_records(self, context: Context | None) -> t.Iterable[dict]:
        """Fetch stock for all SKUs in batches.

        The /stock API requires skuTilroyId — it returns 400 without it.
        SKU IDs come from ProductsStream which runs first.
        """
        from tap_tilroy.streams.products import ProductsStream

        sku_ids = ProductsStream.get_collected_sku_ids()

        if not sku_ids:
            self.logger.warning(
                f"[{self.name}] No SKU IDs found. Ensure ProductsStream runs first."
            )
            return

        total_records = 0
        total_batches = (len(sku_ids) + self._batch_size - 1) // self._batch_size
        start_time = time.time()
        last_log_time = start_time

        self.logger.info(
            f"=== [{self.name}] STARTING FULL SYNC === "
            f"({len(sku_ids):,} SKUs in {total_batches} batches)"
        )

        for batch_num, start_idx in enumerate(range(0, len(sku_ids), self._batch_size), 1):
            batch = sku_ids[start_idx:start_idx + self._batch_size]
            batch_records = 0

            page = 1
            sku_param = ",".join(batch)

            while True:
                params = {"skuTilroyId": sku_param, "page": page}
                prepared = self.build_prepared_request(
                    method="GET",
                    url=self.get_url(context),
                    params=params,
                    headers=self.http_headers,
                )

                try:
                    response = self._request_with_backoff(prepared, context)
                except Exception as e:
                    self.logger.error(f"[{self.name}] Batch {batch_num} failed: {e}")
                    break

                if response.status_code != 200:
                    self.logger.error(
                        f"[{self.name}] Batch {batch_num} API error {response.status_code}"
                    )
                    break

                total_pages = int(response.headers.get("X-Paging-PageCount", 1))
                records = list(self.parse_response(response))

                for record in records:
                    processed = self.post_process(record, context)
                    if processed:
                        batch_records += 1
                        total_records += 1
                        yield processed

                if not records or page >= total_pages:
                    break
                page += 1

            # Log every 10 seconds or every 20 batches
            now = time.time()
            if now - last_log_time >= 10 or batch_num % 20 == 0:
                elapsed = now - start_time
                rate = batch_num / elapsed if elapsed > 0 else 0
                remaining = total_batches - batch_num
                eta_secs = remaining / rate if rate > 0 else 0
                eta_min = int(eta_secs // 60)
                eta_sec = int(eta_secs % 60)

                self.logger.info(
                    f"[{self.name}] Progress: batch {batch_num}/{total_batches} ({100*batch_num/total_batches:.1f}%) | "
                    f"{total_records:,} records | {rate:.1f} batches/s | ETA: {eta_min}m {eta_sec}s"
                )
                last_log_time = now

        elapsed = time.time() - start_time
        self.logger.info(
            f"=== [{self.name}] FULL SYNC COMPLETE === "
            f"{total_records:,} stock records in {elapsed:.0f}s"
        )

    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        """Post-process stock record."""
        row = super().post_process(row, context)
        if row:
            row = self._flatten_shop(row, "shop", "shop")
            row = self._stringify_nested_objects(row)
        return row
