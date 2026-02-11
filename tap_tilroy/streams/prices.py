"""Prices stream for Tilroy API."""

from __future__ import annotations

import time
import typing as t
from datetime import datetime, timezone

from singer_sdk import typing as th

from tap_tilroy.client import TilroyStream

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class PricesStream(TilroyStream):
    """Stream for Tilroy price rules.

    Uses a conditional strategy based on sync state:
    - First sync (no bookmark): Fetches price rules individually per SKU
      using GET /price/rules/{skuTilroyId}. SKU IDs come from ProductsStream.
    - Incremental sync (has bookmark): Paginates the bulk endpoint
      GET /price/rules with dateModified filter.

    One record per price rule; the nested "prices" array is kept as-is.
    Incremental via bookmark on dateModified (rule-level).
    """

    name = "prices"
    path = "/priceapi/production/price/rules"
    primary_keys: t.ClassVar[list[str]] = ["tilroyId"]
    # Use date_modified (snake_case) so state/bookmarks and SDK increment_state match existing state
    replication_key = "date_modified"
    replication_method = "INCREMENTAL"
    records_jsonpath = "$[*]"
    default_count = 100

    max_retries_504 = 5
    retry_504_backoff_seconds = 30

    # Schema matches API: one record = one price rule. Nested "prices" array passed through; filter in ETL.
    schema = th.PropertiesList(
        th.Property("tilroyId", th.StringType),
        th.Property("sku", th.ObjectType(
            th.Property("tilroyId", th.CustomType({"type": ["string", "number", "null"]})),
            th.Property("sourceId", th.CustomType({"type": ["string", "number", "null"]})),
        )),
        th.Property("shop", th.ObjectType(
            th.Property("tilroyId", th.CustomType({"type": ["string", "number", "null"]})),
            th.Property("sourceId", th.CustomType({"type": ["string", "number", "null"]})),
            th.Property("number", th.CustomType({"type": ["string", "number", "null"]})),
        )),
        th.Property("country", th.ObjectType(
            th.Property("tilroyId", th.CustomType({"type": ["string", "number", "null"]})),
            th.Property("code", th.CustomType({"type": ["string", "number", "null"]})),
        )),
        th.Property("tenantId", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property(
            "prices",
            th.ArrayType(
                th.ObjectType(
                    th.Property("tilroyId", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property("sourceId", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property("price", th.NumberType),
                    th.Property("type", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property("quantity", th.IntegerType),
                    th.Property("run", th.CustomType({"type": ["object", "string", "number", "null"]})),
                    th.Property("labelType", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property("endDate", th.DateTimeType),
                    th.Property("startDate", th.DateTimeType),
                    th.Property("dateCreated", th.DateTimeType),
                    th.Property("dateModified", th.DateTimeType),
                )
            ),
        ),
        th.Property("dateModified", th.DateTimeType),
        th.Property("date_modified", th.DateTimeType),
    ).to_dict()

    def _has_existing_state(self) -> bool:
        """Check if the stream has existing state/bookmarks."""
        if not self.replication_key:
            return False

        state = self.tap_state or {}

        # Handle both Singer state formats
        if "value" in state:
            bookmarks = state.get("value", {}).get("bookmarks", {})
        else:
            bookmarks = state.get("bookmarks", {})

        stream_state = bookmarks.get(self.name, {})
        return bool(stream_state.get("replication_key_value"))

    def request_records(self, context: Context | None) -> t.Iterable[dict]:
        """Fetch price rules using conditional strategy.

        - No bookmark: fetch individually per SKU ID (first sync)
        - Has bookmark: paginate bulk endpoint with dateModified (incremental)
        """
        if self._has_existing_state():
            yield from self._request_incremental(context)
        else:
            yield from self._request_full_sync(context)

    def _request_full_sync(self, context: Context | None) -> t.Iterable[dict]:
        """Fetch price rules individually per SKU using collected SKU IDs.

        Uses GET /price/rules/{skuTilroyId} for each SKU collected by ProductsStream.
        This avoids the timeout-prone bulk endpoint for the initial full sync.
        """
        from tap_tilroy.streams.products import ProductsStream

        sku_ids = ProductsStream.get_collected_sku_ids()

        if not sku_ids:
            self.logger.warning(
                f"[{self.name}] No SKU IDs found. Ensure ProductsStream runs first."
            )
            return

        total_records = 0
        total_skus = len(sku_ids)
        self.logger.info(
            f"[{self.name}] Full sync: fetching prices for {total_skus} SKUs individually"
        )

        for i, sku_id in enumerate(sku_ids, 1):
            url = f"{self.url_base.rstrip('/')}{self.path}/{sku_id}"
            prepared = self.build_prepared_request(
                method="GET",
                url=url,
                params={},
                headers=self.http_headers,
            )

            response = self._request_with_retry(prepared, context)
            if response is None:
                continue
            if response.status_code == 404:
                continue  # SKU has no price rules
            if response.status_code != 200:
                self.logger.warning(
                    f"[{self.name}] SKU {sku_id}: HTTP {response.status_code}, skipping"
                )
                continue

            rules = list(self._parse_price_rules_response(response))
            for rule in rules:
                processed = self.post_process(rule, None)
                if processed:
                    total_records += 1
                    yield processed

            if i % 100 == 0:
                self.logger.info(
                    f"[{self.name}] Full sync progress: {i}/{total_skus} SKUs, "
                    f"{total_records} records"
                )

        self.logger.info(
            f"[{self.name}] Full sync done: {total_skus} SKUs -> {total_records} records"
        )

    def _request_incremental(self, context: Context | None) -> t.Iterable[dict]:
        """Paginate bulk endpoint with dateModified filter for incremental sync."""
        bookmark_date = self._parse_bookmark(context)
        if bookmark_date:
            self.logger.info(
                f"[{self.name}] Incremental sync with dateModified={bookmark_date.isoformat()}"
            )
        else:
            self.logger.info(f"[{self.name}] Incremental sync (no date filter)")

        page = 1
        total_records = 0
        total_pages = None

        while True:
            params = self.get_url_params(context, page)
            if bookmark_date:
                params["dateModified"] = bookmark_date.strftime("%Y-%m-%dT%H:%M:%SZ")

            prepared = self.build_prepared_request(
                method="GET",
                url=self.get_url(context),
                params=params,
                headers=self.http_headers,
            )

            response = self._request_with_retry(prepared, context)
            if response is None:
                return
            if response.status_code != 200:
                self.logger.error(
                    f"[{self.name}] API error {response.status_code}: {response.text[:500]}"
                )
                return

            price_rules = list(self._parse_price_rules_response(response))
            page_count = len(price_rules)
            for rule in price_rules:
                processed = self.post_process(rule, None)
                if processed:
                    total_records += 1
                    yield processed

            try:
                total_pages = int(response.headers.get("X-Paging-PageCount", total_pages or 1))
            except (ValueError, TypeError):
                pass

            page_info = f"page {page}/{total_pages}" if total_pages else f"page {page}"
            self.logger.info(
                f"[{self.name}] {page_info}: {page_count} rules -> {page_count} records (total: {total_records})"
            )

            if page_count < self.default_count:
                break
            try:
                current = int(response.headers.get("X-Paging-CurrentPage", 1))
                if total_pages and current >= total_pages:
                    self.logger.info(f"[{self.name}] Finished all {total_pages} pages")
                    break
            except (ValueError, TypeError):
                pass
            page += 1

        self.logger.info(f"[{self.name}] Incremental done: {total_records} records")

    def _request_with_retry(self, prepared, context: Context | None):
        """Execute request with 504 retry logic.

        Returns the response, or None if all retries failed.
        """
        for attempt in range(self.max_retries_504 + 1):
            try:
                response = self._request(prepared, context)
            except Exception as e:
                if attempt < self.max_retries_504:
                    self.logger.warning(
                        f"[{self.name}] Request failed (attempt {attempt + 1}): {e}; "
                        f"retrying in {self.retry_504_backoff_seconds}s..."
                    )
                    time.sleep(self.retry_504_backoff_seconds)
                    continue
                self.logger.error(
                    f"[{self.name}] Request failed after {self.max_retries_504 + 1} attempts: {e}"
                )
                return None
            if response.status_code == 504:
                if attempt < self.max_retries_504:
                    self.logger.warning(
                        f"[{self.name}] 504 Gateway Timeout (attempt {attempt + 1}); "
                        f"retrying in {self.retry_504_backoff_seconds}s..."
                    )
                    time.sleep(self.retry_504_backoff_seconds)
                    continue
                self.logger.error(
                    f"[{self.name}] 504 after {self.max_retries_504 + 1} attempts, giving up"
                )
                return None
            return response
        return None

    def _parse_bookmark(self, context: Context | None) -> datetime | None:
        """Bookmark from state, or config start_date for initial sync."""
        raw = self.get_starting_timestamp(context)
        if not raw:
            config_start = self.config.get("start_date")
            if config_start:
                if isinstance(config_start, str):
                    try:
                        return datetime.fromisoformat(config_start.replace("Z", "+00:00"))
                    except (ValueError, TypeError):
                        pass
                elif hasattr(config_start, "isoformat"):
                    return config_start
            return None
        if isinstance(raw, str):
            try:
                return datetime.fromisoformat(raw.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                return None
        return raw

    def _parse_price_rules_response(self, response) -> t.Iterable[dict]:
        """Parse rules from response: raw array, or wrapped in data/items/results/rules."""
        parsed = list(super().parse_response(response))
        if parsed:
            return parsed
        try:
            data = response.json()
        except Exception:
            return []
        if isinstance(data, dict):
            for key in ("data", "items", "results", "rules"):
                arr = data.get(key)
                if isinstance(arr, list):
                    return arr
            if data.get("prices") is not None or data.get("sku") is not None:
                return [data]
        return []

    def post_process(self, row: dict, context: Context | None = None) -> dict | None:
        if not row:
            return None
        # API uses dateModified (camelCase); state/SDK use date_modified (snake_case). Set both.
        if not row.get("dateModified"):
            row["dateModified"] = datetime.now(timezone.utc).isoformat()
        row["date_modified"] = row.get("dateModified") or row.get("date_modified") or datetime.now(timezone.utc).isoformat()
        return row
