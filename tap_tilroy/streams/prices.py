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

    Each price rule is flattened: one record per price in the prices array.
    Incremental via bookmark on date_modified.
    """

    name = "prices"
    path = "/priceapi/production/price/rules"
    # Composite key: one row per price within a SKU
    primary_keys: t.ClassVar[list[str]] = ["sku_tilroy_id", "price_tilroy_id", "type"]
    replication_key = "date_modified"
    replication_method = "INCREMENTAL"
    records_jsonpath = "$[*]"
    default_count = 100

    # Track the maximum dateModified seen during sync for accurate bookmarking
    _max_date_modified: datetime | None = None

    # Flattened schema: one record per price (exploded from prices array)
    schema = th.PropertiesList(
        th.Property("sku_tilroy_id", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("sku_source_id", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("price_tilroy_id", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("price_source_id", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("price", th.NumberType),
        th.Property("type", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("quantity", th.IntegerType),
        th.Property("run", th.CustomType({"type": ["object", "string", "number", "null"]})),
        th.Property("label_type", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("end_date", th.DateTimeType),
        th.Property("start_date", th.DateTimeType),
        th.Property("date_created", th.DateTimeType),
        th.Property("date_modified", th.DateTimeType),
        th.Property("tenant_id", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("shop_tilroy_id", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("shop_number", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("country_tilroy_id", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("country_code", th.CustomType({"type": ["string", "number", "null"]})),
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
        self._max_date_modified = None

        if self._has_existing_state():
            yield from self._request_incremental(context)
        else:
            yield from self._request_full_sync(context)

            if self._max_date_modified:
            self.logger.info(
                f"[{self.name}] Max dateModified seen: {self._max_date_modified.isoformat()}"
            )

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
        start_time = time.time()
        last_log_time = start_time

        self.logger.info(
            f"=== [{self.name}] STARTING FULL SYNC === "
            f"({total_skus:,} SKUs to fetch)"
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
                # Flatten each rule into individual price records
                for record in self._flatten_price_rule(rule, requested_sku_id=sku_id):
                    total_records += 1
                    yield record

            now = time.time()
            if now - last_log_time >= 10 or i % 500 == 0:
                elapsed = now - start_time
                rate = i / elapsed if elapsed > 0 else 0
                remaining = total_skus - i
                eta_secs = remaining / rate if rate > 0 else 0
                eta_min = int(eta_secs // 60)
                eta_sec = int(eta_secs % 60)

                self.logger.info(
                    f"[{self.name}] Progress: {i:,}/{total_skus:,} SKUs ({100*i/total_skus:.1f}%) | "
                    f"{total_records:,} records | {rate:.1f} SKUs/s | ETA: {eta_min}m {eta_sec}s"
                )
                last_log_time = now

        elapsed = time.time() - start_time
        rate = total_skus / elapsed if elapsed > 0 else 0
        self.logger.info(
            f"=== [{self.name}] FULL SYNC COMPLETE === "
            f"{total_skus:,} SKUs -> {total_records:,} records in {elapsed:.0f}s ({rate:.1f} SKUs/s)"
        )

    def _request_incremental(self, context: Context | None) -> t.Iterable[dict]:
        """Paginate bulk endpoint with dateModified filter for incremental sync."""
        bookmark_date = self._parse_bookmark(context)
        start_time = time.time()

        if bookmark_date:
            self.logger.info(
                f"=== [{self.name}] STARTING INCREMENTAL SYNC === "
                f"(dateModified >= {bookmark_date.isoformat()})"
            )
        else:
            self.logger.info(f"=== [{self.name}] STARTING INCREMENTAL SYNC === (no date filter)")

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
            page_count = 0
            for rule in price_rules:
                # Flatten each rule into individual price records
                for record in self._flatten_price_rule(rule):
                    total_records += 1
                    page_count += 1
                    yield record

            try:
                total_pages = int(response.headers.get("X-Paging-PageCount", total_pages or 1))
            except (ValueError, TypeError):
                pass

            pct = (100 * page / total_pages) if total_pages else 0
            self.logger.info(
                f"[{self.name}] Page {page}/{total_pages or '?'} ({pct:.0f}%) | "
                f"+{page_count} records | Total: {total_records:,}"
            )

            if len(price_rules) < self.default_count:
                break
            try:
                current = int(response.headers.get("X-Paging-CurrentPage", 1))
                if total_pages and current >= total_pages:
                    break
            except (ValueError, TypeError):
                pass
            page += 1

        elapsed = time.time() - start_time
        self.logger.info(
            f"=== [{self.name}] INCREMENTAL SYNC COMPLETE === "
            f"{total_records:,} records in {elapsed:.0f}s"
        )

    def _flatten_price_rule(
        self, price_rule: dict, requested_sku_id: str | None = None
    ) -> t.Iterable[dict]:
        """Flatten a price rule into individual price records.

        Args:
            price_rule: The price rule containing SKU, shop, country, and prices.
            requested_sku_id: The SKU tilroyId we requested (per-SKU endpoint).

        Yields:
            Flattened price records (one per item in the prices array).
        """
        if not price_rule:
            return

        sku = price_rule.get("sku") or {}
        shop = price_rule.get("shop") or {}
        country = price_rule.get("country") or {}
        tenant_id = price_rule.get("tenantId")
        prices = price_rule.get("prices", [])

        if not prices:
            return

        # Use requested SKU ID if provided, fallback to payload
        payload_sku_id = sku.get("tilroyId")
        if payload_sku_id is not None:
            payload_sku_id = str(payload_sku_id)
        sku_tilroy_id = requested_sku_id if requested_sku_id else payload_sku_id

        # Rule-level dateModified (used as replication key)
        rule_date_modified = price_rule.get("dateModified")

        for price in prices:
            price_date_created = price.get("dateCreated")
            price_date_modified = price.get("dateModified")

            # Use rule's dateModified as replication key; fallback to price dates
            date_modified = rule_date_modified or price_date_modified or price_date_created
            if not date_modified:
                date_modified = datetime.now(timezone.utc).isoformat()

            # Track the max dateModified for bookmark advancement
            self._update_max_date_modified(date_modified)

            record = {
                "sku_tilroy_id": sku_tilroy_id,
                "sku_source_id": sku.get("sourceId"),
                "price_tilroy_id": str(price.get("tilroyId", "")) if price.get("tilroyId") else None,
                "price_source_id": price.get("sourceId"),
                "price": price.get("price"),
                "type": price.get("type"),
                "quantity": price.get("quantity", 1),
                "run": price.get("run"),
                "label_type": price.get("labelType"),
                "end_date": price.get("endDate"),
                "start_date": price.get("startDate"),
                "date_created": price_date_created,
                "date_modified": date_modified,
                "tenant_id": tenant_id,
                "shop_tilroy_id": shop.get("tilroyId"),
                "shop_number": shop.get("number"),
                "country_tilroy_id": country.get("tilroyId"),
                "country_code": country.get("code"),
            }

            yield record

    def _request_with_retry(self, prepared, context: Context | None):
        """Execute request with SDK backoff. Returns response or None on failure."""
        try:
            return self._request_with_backoff(prepared, context)
        except Exception as e:
            self.logger.error(f"[{self.name}] Request failed after retries: {e}")
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

    def _update_max_date_modified(self, date_str: str | None) -> None:
        """Track the maximum dateModified value seen during sync.

        Args:
            date_str: ISO datetime string from API response.
        """
        if not date_str:
            return

        try:
            if isinstance(date_str, str):
                parsed = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            elif hasattr(date_str, "isoformat"):
                parsed = date_str
            else:
                return

            if self._max_date_modified is None or parsed > self._max_date_modified:
                self._max_date_modified = parsed
        except (ValueError, TypeError):
            pass

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
