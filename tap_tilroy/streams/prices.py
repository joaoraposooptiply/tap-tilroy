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

    GET /price/rules with count and page. One record per price rule (API
    top-level object); the nested "prices" array is kept as-is. So we emit
    exactly 100 records per page when the API returns 100 rules.
    Paginate until no more records or API says no more pages.
    Incremental via bookmark on dateModified (rule-level).
    """

    name = "prices"
    path = "/priceapi/production/price/rules"
    primary_keys: t.ClassVar[list[str]] = ["tilroyId"]
    replication_key = "dateModified"
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
    ).to_dict()

    def request_records(self, context: Context | None) -> t.Iterable[dict]:
        """GET /price/rules with count and page; emit one record per rule (100 rules = 100 records). No client-side filtering."""
        bookmark_date = self._parse_bookmark(context)
        if bookmark_date:
            self.logger.info(f"[{self.name}] Sending dateModified to API for incremental; emitting all rules returned (no client filter)")
        else:
            self.logger.info(f"[{self.name}] Full sync; emitting all rules returned")

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

        self.logger.info(f"[{self.name}] Done: {total_records} records")

    def _parse_bookmark(self, context: Context | None) -> datetime | None:
        """Bookmark from state, or config start_date for initial sync (e.g. everything since Jan 1)."""
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
        if not row.get("dateModified"):
            row["dateModified"] = datetime.now(timezone.utc).isoformat()
        return row
