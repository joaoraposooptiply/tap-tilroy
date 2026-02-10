"""Prices stream for Tilroy API."""

from __future__ import annotations

import typing as t
from datetime import datetime, timedelta, timezone

from singer_sdk import typing as th

from tap_tilroy.client import DateWindowedStream

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class PricesStream(DateWindowedStream):
    """Stream for Tilroy price rules.

    Uses the list endpoint GET /price/rules with date windowing and page pagination.
    Each response is a list of price rules; we flatten each rule into records and filter
    by replication key (date_modified) client-side for incremental sync.
    """

    name = "prices"
    path = "/priceapi/production/price/rules"
    primary_keys: t.ClassVar[list[str]] = ["sku_tilroy_id", "price_tilroy_id", "type"]
    replication_key = "date_modified"
    replication_method = "INCREMENTAL"
    records_jsonpath = "$[*]"
    default_count = 100

    date_window_days = 7
    use_date_to = False
    date_param_name = "dateModified"
    use_last_id_pagination = False
    last_id_field = "tilroyId"
    last_id_param = "lastId"
    _debug_count = 0

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
        """Request records from list endpoint GET /price/rules with date windowing and page pagination."""
        start_date = self._get_start_date(context)
        end_date = datetime.now()
        bookmark_date = self._parse_bookmark(context)

        self.logger.info(
            f"[{self.name}] Starting date-windowed sync from {start_date.date()} to {end_date.date()} "
            f"with {self.date_window_days}-day windows, page pagination"
        )
        if bookmark_date:
            self.logger.info(f"[{self.name}] Bookmark: {bookmark_date}, filtering records >= bookmark")
        else:
            self.logger.info(f"[{self.name}] No bookmark - full sync, all records included")

        total_records = 0
        window_start = start_date

        while window_start < end_date:
            window_end = min(
                window_start + timedelta(days=self.date_window_days),
                end_date,
            )
            self.logger.info(
                f"[{self.name}] Window {window_start.date()} to {window_end.date()}"
            )
            page = 1
            window_records = 0

            while True:
                params = self._get_window_params(window_start, window_end, page, None)
                prepared = self.build_prepared_request(
                    method="GET",
                    url=self.get_url(context),
                    params=params,
                    headers=self.http_headers,
                )
                self.logger.info(
                    f"[{self.name}] GET {self.get_url(context)} page={page} count={params.get('count')} "
                    f"dateModified={params.get(self.date_param_name, '')[:10]}"
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

                price_rules = list(self._parse_price_rules_response(response))
                page_rules = len(price_rules)
                page_emitted = 0
                for price_rule in price_rules:
                    for record in self._flatten_price_rule(price_rule):
                        if self._should_include_record(record, bookmark_date):
                            total_records += 1
                            window_records += 1
                            page_emitted += 1
                            yield record

                self.logger.info(
                    f"[{self.name}] Window {window_start.date()}-{window_end.date()} page {page}: "
                    f"{page_rules} rules -> {page_emitted} records (total so far: {total_records})"
                )
                if page_rules < self.default_count:
                    break
                page += 1

            self.logger.info(
                f"[{self.name}] Window {window_start.date()}-{window_end.date()} complete: {window_records} records"
            )
            window_start = window_end

        self.logger.info(f"[{self.name}] Complete: {total_records} total records")

    def _parse_bookmark(self, context: Context | None) -> datetime | None:
        """Parse bookmark from context/state into datetime or None."""
        raw = self.get_starting_timestamp(context)
        if not raw:
            return None
        if isinstance(raw, str):
            try:
                return datetime.fromisoformat(raw.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                self.logger.warning(f"[{self.name}] Could not parse bookmark: {raw}")
                return None
        return raw

    def _parse_price_rules_response(self, response) -> t.Iterable[dict]:
        """Parse price rules from response; handle raw array, wrapped {data: [...]}, or single rule object."""
        parsed = list(super(DateWindowedStream, self).parse_response(response))
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
                    self.logger.info(
                        f"[{self.name}] Response was wrapped in key {key!r}, using {len(arr)} items"
                    )
                    return arr
            # Per-SKU endpoint may return a single rule object at top level
            if data.get("prices") is not None or data.get("sku") is not None:
                self.logger.debug(f"[{self.name}] Response was single rule object, wrapping in list")
                return [data]
        return []

    def _should_include_record(self, record: dict, bookmark_date: datetime | str | None) -> bool:
        """Check if record should be included based on replication key.
        
        Args:
            record: The flattened price record.
            bookmark_date: The bookmark date from state.
            
        Returns:
            True if record should be included (newer than bookmark or no bookmark).
        """
        if not bookmark_date:
            return True  # No bookmark, include all
        
        record_date = record.get(self.replication_key)  # date_modified
        if not record_date:
            # If no date_modified, exclude it (shouldn't happen, but be safe)
            self.logger.debug(f"[{self.name}] Record missing {self.replication_key}, excluding")
            return False
        
        # Parse bookmark date if it's a string
        if isinstance(bookmark_date, str):
            try:
                bookmark_date = datetime.fromisoformat(bookmark_date.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                self.logger.warning(f"[{self.name}] Could not parse bookmark date: {bookmark_date}")
                return True  # Include if we can't parse bookmark
        
        # Remove timezone from bookmark for comparison (make naive)
        if hasattr(bookmark_date, "tzinfo") and bookmark_date.tzinfo:
            bookmark_date_naive = bookmark_date.replace(tzinfo=None)
        else:
            bookmark_date_naive = bookmark_date
        
        # Parse record date if it's a string
        if isinstance(record_date, str):
            try:
                # Try ISO format first
                if "T" in record_date or "+" in record_date or "Z" in record_date:
                    record_date_parsed = datetime.fromisoformat(record_date.replace("Z", "+00:00"))
                else:
                    # Try date-only format
                    record_date_parsed = datetime.strptime(record_date, "%Y-%m-%d")
            except (ValueError, TypeError):
                self.logger.debug(f"[{self.name}] Could not parse record date: {record_date}, excluding")
                return False
        else:
            record_date_parsed = record_date
        
        # Remove timezone from record date for comparison (make naive)
        if hasattr(record_date_parsed, "tzinfo") and record_date_parsed.tzinfo:
            record_date_naive = record_date_parsed.replace(tzinfo=None)
        else:
            record_date_naive = record_date_parsed
        
        # Include if record date is >= bookmark date (strict comparison)
        should_include = record_date_naive >= bookmark_date_naive
        
        # Log first few comparisons for debugging
        self._debug_count += 1
        
        if self._debug_count <= 5:
            self.logger.info(
                f"[{self.name}] Record comparison #{self._debug_count}: "
                f"record_date={record_date_naive}, bookmark={bookmark_date_naive}, "
                f"include={should_include}"
            )
        
        if not should_include:
            self.logger.debug(
                f"[{self.name}] Excluding record: {record_date_naive} < {bookmark_date_naive}"
            )
        
        return should_include

    def _get_window_params(
        self,
        window_start: datetime,
        window_end: datetime,
        page: int,
        last_id: str | None = None,
    ) -> dict[str, t.Any]:
        """Get URL parameters for a specific date window and page.
        
        Overrides DateWindowedStream._get_window_params to use dateModified
        instead of dateFrom, and handle lastId pagination.
        
        Note: The API's dateModified filter filters by rule-level dateModified,
        but many rules have dateModified=None. We still use it for the API query,
        but rely on client-side filtering of individual prices by their dateCreated.
        
        Args:
            window_start: Start of the date window.
            window_end: End of the date window (not used for price API).
            page: Current page number (not used for lastId pagination).
            last_id: Last record ID (used for lastId pagination).
            
        Returns:
            Dictionary of URL parameters.
        """
        params: dict[str, t.Any] = {
            "count": self.default_count,
            self.date_param_name: window_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "page": page,
        }
        if self.use_last_id_pagination and last_id:
            params[self.last_id_param] = last_id
        return params

    def _flatten_price_rule(
        self, price_rule: dict, requested_sku_id: str | None = None
    ) -> t.Iterable[dict]:
        """Flatten a price rule into individual price records.

        Args:
            price_rule: The price rule containing SKU and prices.
            requested_sku_id: The SKU tilroyId we requested (per-SKU endpoint). Used to pin
                sku_tilroy_id when payload is ambiguous; if payload sku.tilroyId differs, we log.

        Yields:
            Flattened price records.
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

        # Prefer the SKU id we requested (per-SKU URL); fall back to payload sku.tilroyId
        payload_sku_id = sku.get("tilroyId")
        if payload_sku_id is not None:
            payload_sku_id = str(payload_sku_id)
        if requested_sku_id and payload_sku_id and payload_sku_id != requested_sku_id:
            self.logger.warning(
                f"[{self.name}] Payload sku.tilroyId ({payload_sku_id}) != requested skuTilroyId "
                f"({requested_sku_id}); using requested id for record"
            )
        sku_tilroy_id = requested_sku_id if requested_sku_id else payload_sku_id

        # Get rule-level dateModified (if available)
        rule_date_modified = price_rule.get("dateModified")

        for price in prices:
            price_date_created = price.get("dateCreated")
            price_date_modified = price.get("dateModified")

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
                "date_created": price_date_created,
                # Use rule's dateModified as date_modified for replication key
                # This ensures the bookmark advances when price rules are modified
                # Fall back to price's dateModified, then dateCreated if rule has no dateModified
                "date_modified": rule_date_modified or price_date_modified or price_date_created,
                "tenant_id": tenant_id,
                "shop_tilroy_id": shop.get("tilroyId"),
                "shop_number": shop.get("number"),
                "country_tilroy_id": country.get("tilroyId"),
                "country_code": country.get("code"),
            }

            # Ensure date_modified is set (required for replication)
            # This should only happen if both dateModified and dateCreated are missing
            if not record["date_modified"]:
                # If no date at all, exclude this record (shouldn't happen in real data)
                self.logger.warning(
                    f"[{self.name}] Price {record['price_tilroy_id']} has no date_created or date_modified, "
                    "setting to current time (will be included)"
                )
                record["date_modified"] = datetime.now(timezone.utc).isoformat()

            processed = self.post_process(record, None)
            if processed:
                yield processed

    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        """Validate price record has required fields."""
        if not row:
            return None

        # Ensure replication key exists
        if not row.get("date_modified"):
            row["date_modified"] = datetime.now(timezone.utc).isoformat()

        return row
