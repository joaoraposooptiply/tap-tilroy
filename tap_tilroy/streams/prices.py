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

    GET /price/rules with count and page. Paginate until no more records or
    API says no more pages. Incremental via bookmark on date_modified.
    """

    name = "prices"
    path = "/priceapi/production/price/rules"
    primary_keys: t.ClassVar[list[str]] = ["sku_tilroy_id", "price_tilroy_id", "type"]
    replication_key = "date_modified"
    replication_method = "INCREMENTAL"
    records_jsonpath = "$[*]"
    default_count = 100

    # Retry 504 with backoff (gateway timeout)
    max_retries_504 = 5
    retry_504_backoff_seconds = 30

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

    def request_records(self, context: Context | None) -> t.Iterable[dict]:
        """GET /price/rules with count and page; paginate until no records or no more pages."""
        bookmark_date = self._parse_bookmark(context)
        if bookmark_date:
            self.logger.info(f"[{self.name}] Bookmark: {bookmark_date}, only emitting records >= bookmark")
        else:
            self.logger.info(f"[{self.name}] No bookmark - full sync")

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

            response = None
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
                    else:
                        self.logger.error(f"[{self.name}] Request failed after {self.max_retries_504 + 1} attempts: {e}")
                        return
                    continue
                if response.status_code == 504:
                    if attempt < self.max_retries_504:
                        self.logger.warning(
                            f"[{self.name}] 504 Gateway Timeout on page {page} (attempt {attempt + 1}); "
                            f"retrying in {self.retry_504_backoff_seconds}s..."
                        )
                        time.sleep(self.retry_504_backoff_seconds)
                    else:
                        self.logger.error(f"[{self.name}] 504 after {self.max_retries_504 + 1} attempts, stopping")
                        return
                    continue
                break
            if response.status_code != 200:
                self.logger.error(
                    f"[{self.name}] API error {response.status_code}: {response.text[:500]}"
                )
                return

            price_rules = list(self._parse_price_rules_response(response))
            page_count = len(price_rules)
            emitted = 0
            for price_rule in price_rules:
                for record in self._flatten_price_rule(price_rule, bookmark_date):
                    if self._should_include_record(record, bookmark_date):
                        total_records += 1
                        emitted += 1
                        yield record

            # Update total pages from response headers
            try:
                total_pages = int(response.headers.get("X-Paging-PageCount", total_pages or 1))
            except (ValueError, TypeError):
                pass

            page_info = f"page {page}/{total_pages}" if total_pages else f"page {page}"
            self.logger.info(
                f"[{self.name}] {page_info}: {page_count} rules -> {emitted} records (total: {total_records})"
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

        self.logger.info(f"[{self.name}] Done: {total_records} records")

    def _parse_bookmark(self, context: Context | None) -> datetime | None:
        raw = self.get_starting_timestamp(context)
        if not raw:
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

    def _should_include_record(self, record: dict, bookmark_date: datetime | None) -> bool:
        if not bookmark_date:
            return True
        record_date = record.get(self.replication_key)
        if not record_date:
            return False
        if isinstance(record_date, str):
            try:
                record_date = datetime.fromisoformat(record_date.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                return False
        if hasattr(record_date, "tzinfo") and record_date.tzinfo:
            record_date = record_date.replace(tzinfo=None)
        if hasattr(bookmark_date, "tzinfo") and bookmark_date.tzinfo:
            bookmark_date = bookmark_date.replace(tzinfo=None)
        return record_date >= bookmark_date

    def _flatten_price_rule(
        self, price_rule: dict, bookmark_date: datetime | None = None,
    ) -> t.Iterable[dict]:
        if not price_rule:
            return
        sku = price_rule.get("sku") or {}
        shop = price_rule.get("shop") or {}
        country = price_rule.get("country") or {}
        tenant_id = price_rule.get("tenantId")
        prices = price_rule.get("prices", [])
        if not prices:
            return
        sku_tilroy_id = sku.get("tilroyId")
        rule_date_modified = price_rule.get("dateModified")
        now_iso = datetime.now(timezone.utc).isoformat()
        for price in prices:
            # Determine date_modified: prefer explicit values from the API,
            # fall back to dateCreated. During incremental syncs (bookmark
            # exists), the API's server-side dateModified filter already
            # confirmed recency, so use now() when no explicit date exists.
            date_modified = (
                rule_date_modified
                or price.get("dateModified")
                or price.get("dateCreated")
            )
            if not date_modified and bookmark_date:
                date_modified = now_iso

            record = {
                "sku_tilroy_id": sku_tilroy_id,
                "sku_source_id": sku.get("sourceId"),
                "price_tilroy_id": str(price.get("tilroyId", "")),
                "price_source_id": price.get("sourceId"),
                "price": price.get("price"),
                "type": price.get("type"),
                "quantity": price.get("quantity", 1),
                "run": price.get("run"),
                "label_type": price.get("labelType"),
                "end_date": price.get("endDate"),
                "start_date": price.get("startDate"),
                "date_created": price.get("dateCreated"),
                "date_modified": date_modified,
                "tenant_id": tenant_id,
                "shop_tilroy_id": shop.get("tilroyId"),
                "shop_number": shop.get("number"),
                "country_tilroy_id": country.get("tilroyId"),
                "country_code": country.get("code"),
            }
            if not record["date_modified"]:
                record["date_modified"] = now_iso
            processed = self.post_process(record, None)
            if processed:
                yield processed

    def post_process(self, row: dict, context: Context | None = None) -> dict | None:
        if not row:
            return None
        if not row.get("date_modified"):
            row["date_modified"] = datetime.now(timezone.utc).isoformat()
        return row
