"""Stock streams for Tilroy API."""

from __future__ import annotations

import typing as t
from datetime import datetime

import requests
from singer_sdk import typing as th
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_tilroy.client import DateFilteredStream, TilroyStream

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class StockChangesStream(DateFilteredStream):
    """Stream for Tilroy stock changes.

    Tracks inventory changes over time with date-based filtering.
    Requires both dateFrom and dateTo parameters.
    """

    name = "stock_changes"
    path = "/stockapi/production/stockchanges"
    primary_keys: t.ClassVar[list[str]] = ["tilroyId"]
    replication_key = "dateUpdated"
    replication_method = "INCREMENTAL"
    records_jsonpath = "$[*]"
    default_count = 100

    schema = th.PropertiesList(
        th.Property("tilroyId", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("timestamp", th.DateTimeType),
        th.Property("sourceId", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("reason", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("shop", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("product", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("qty", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("size", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("refill", th.IntegerType),
        th.Property("sku", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("qtyDelta", th.IntegerType),
        th.Property("qtyTransferredDelta", th.IntegerType),
        th.Property("qtyReservedDelta", th.IntegerType),
        th.Property("qtyRequestedDelta", th.IntegerType),
        th.Property("cause", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("dateCreated", th.DateTimeType),
        th.Property("dateUpdated", th.DateTimeType),
    ).to_dict()

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: int | None,
    ) -> dict[str, t.Any]:
        """Return URL parameters including dateTo (required by API)."""
        params = super().get_url_params(context, next_page_token)

        # Stock changes API requires dateTo parameter
        params["dateTo"] = datetime.now().strftime("%Y-%m-%d")

        return params

    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        """Post-process stock change record."""
        row = super().post_process(row, context)
        if row:
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
            row = self._stringify_nested_objects(row)
        return row
