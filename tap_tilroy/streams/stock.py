"""Stock streams for Tilroy API."""

from __future__ import annotations

import typing as t
from datetime import datetime, timedelta

import requests
from singer_sdk import typing as th
from singer_sdk.helpers.jsonpath import extract_jsonpath

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
        th.Property("lastExternalDelivery", th.CustomType({"type": ["string", "null"]})),
    ).to_dict()

    def _get_start_date(self) -> datetime:
        """Determine the start date for filtering from global bookmark."""
        bookmark_date = self.get_starting_timestamp(None)

        if bookmark_date:
            if hasattr(bookmark_date, "date"):
                date_only = bookmark_date.date()
            else:
                date_only = bookmark_date
            return datetime.combine(date_only - timedelta(days=1), datetime.min.time())

        config_start = self.config.get("start_date", "2010-01-01T00:00:00Z")
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
        
        url = f"{self.url_base}{self.path}"
        
        response = requests.get(
            url,
            params=params,
            headers=self.http_headers,
            timeout=60,
        )
        response.raise_for_status()
        
        current_page = int(response.headers.get("X-Paging-CurrentPage", 1))
        total_pages = int(response.headers.get("X-Paging-PageCount", 1))
        has_more = current_page < total_pages
        
        data = response.json()
        records = list(extract_jsonpath(self.records_jsonpath, input=data))
        
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
        
        if not shop_numbers:
            # No shop filter - fetch all
            self.logger.info(f"[{self.name}] Fetching all stock changes from {start_date.date()}")
            yield from self._fetch_all_for_shop(None, start_date)
        else:
            # Iterate through each shop
            self.logger.info(
                f"[{self.name}] Fetching stock changes for shops {shop_numbers} from {start_date.date()}"
            )
            for shop_num in shop_numbers:
                self.logger.debug(f"[{self.name}] Fetching shop_number={shop_num}")
                yield from self._fetch_all_for_shop(shop_num, start_date)

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

    Fetches stock data for SKUs collected from ProductsStream.
    Processes SKUs in batches due to API limitations.
    """

    name = "stock"
    path = "/stockapi/production/stock"
    primary_keys: t.ClassVar[list[str]] = ["tilroyId"]
    replication_key = None
    replication_method = "FULL_TABLE"
    records_jsonpath = "$[*]"

    # Batch configuration
    _batch_size: int = 150  # SKUs per API request
    _sku_ids: t.ClassVar[list[str]] = []

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

    def _get_sku_ids_from_products(self) -> list[str]:
        """Retrieve SKU IDs collected by ProductsStream.

        Returns:
            List of SKU IDs to fetch stock for.
        """
        # Import here to avoid circular import
        from tap_tilroy.streams.products import ProductsStream

        sku_ids = ProductsStream.get_collected_sku_ids()

        if not sku_ids:
            self.logger.warning(
                f"[{self.name}] No SKU IDs found. Ensure ProductsStream runs first."
            )

        return sku_ids

    def request_records(self, context: Context | None) -> t.Iterable[dict]:
        """Request stock records in batches by SKU ID.

        The Stock API accepts a comma-separated list of SKU IDs.
        We batch these to avoid URL length limits.

        Args:
            context: Stream partition context.

        Yields:
            Stock records from the API.
        """
        sku_ids = self._get_sku_ids_from_products()

        if not sku_ids:
            self.logger.info(f"[{self.name}] No SKU IDs to process, skipping stream")
            return

        self.logger.info(
            f"[{self.name}] Processing {len(sku_ids)} SKU IDs in batches of {self._batch_size}"
        )

        # Process in batches
        for batch_num, start_idx in enumerate(range(0, len(sku_ids), self._batch_size), 1):
            end_idx = min(start_idx + self._batch_size, len(sku_ids))
            batch_sku_ids = sku_ids[start_idx:end_idx]

            self.logger.info(
                f"[{self.name}] Batch {batch_num}: Processing {len(batch_sku_ids)} SKUs "
                f"(indices {start_idx}-{end_idx-1})"
            )

            # Fetch all pages for this batch
            yield from self._fetch_batch_with_pagination(batch_sku_ids, context)

        self.logger.info(f"[{self.name}] Completed processing all SKU batches")

    def _fetch_batch_with_pagination(
        self,
        sku_ids: list[str],
        context: Context | None,
    ) -> t.Iterable[dict]:
        """Fetch all pages of stock data for a batch of SKU IDs.

        Args:
            sku_ids: List of SKU IDs for this batch.
            context: Stream partition context.

        Yields:
            Stock records from the API.
        """
        page = 1
        sku_param = ",".join(sku_ids)

        while True:
            params = {"skuTilroyId": sku_param, "page": page}

            prepared_request = self.build_prepared_request(
                method="GET",
                url=self.get_url(context),
                params=params,
                headers=self.http_headers,
            )

            response = self._request(prepared_request, context)

            if response.status_code != 200:
                self.logger.error(
                    f"[{self.name}] API error {response.status_code}: {response.text[:500]}"
                )
                break

            # Parse pagination headers
            total_pages = int(response.headers.get("X-Paging-PageCount", 1))
            records = list(self.parse_response(response))

            self.logger.info(
                f"[{self.name}] Page {page}/{total_pages}: Got {len(records)} records"
            )

            if not records:
                break

            for record in records:
                processed = self.post_process(record, context)
                if processed:
                    yield processed

            # Check if more pages
            if page >= total_pages:
                break

            page += 1

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
