"""Prices stream for Tilroy API."""

from __future__ import annotations

import typing as t
from datetime import datetime

from singer_sdk import typing as th

from tap_tilroy.client import DateWindowedStream

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class PricesStream(DateWindowedStream):
    """Stream for Tilroy price rules.

    Uses /price/rules endpoint with lastId pagination and date windowing.
    Flattens nested price structures into individual records.
    Iterates through shop_ids internally to avoid per-partition state bloat.
    
    Uses date windowing to prevent API timeouts on large date ranges.
    """

    name = "prices"
    path = "/priceapi/production/price/rules"
    primary_keys: t.ClassVar[list[str]] = ["sku_tilroy_id", "price_tilroy_id", "type"]
    replication_key = "date_modified"
    replication_method = "INCREMENTAL"
    records_jsonpath = "$[*]"
    default_count = 100

    # Date windowing configuration - 7 days at a time to avoid timeouts
    date_window_days = 7
    use_date_to = False  # Price API doesn't support dateTo, only dateModified
    date_param_name = "dateModified"  # Price API uses dateModified instead of dateFrom
    
    # Use lastId pagination for /price/rules endpoint
    use_last_id_pagination = True
    last_id_field = "tilroyId"  # Top-level rule ID
    last_id_param = "lastId"

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
        """Request records using date windowing with shop iteration.
        
        Overrides DateWindowedStream.request_records to handle shop filtering,
        proper replication key filtering on flattened records, and correct
        lastId pagination (which needs to come from price rules, not flattened records).
        
        Args:
            context: Stream partition context.
            
        Yields:
            Records from the API that meet replication key criteria.
        """
        from datetime import timedelta
        
        # Get shop IDs to iterate through
        shop_ids = getattr(self._tap, "_resolved_shop_ids", [])
        if not shop_ids:
            shop_ids = [None]  # None means fetch all shops
        
        # Get bookmark for replication key filtering
        bookmark_date = self.get_starting_timestamp(context)
        start_date = self._get_start_date(context)
        end_date = datetime.now()
        
        self.logger.info(
            f"[{self.name}] Starting date-windowed sync from {start_date.date()} "
            f"to {end_date.date()} with {self.date_window_days}-day windows"
        )
        
        # Iterate through shops
        for shop_id in shop_ids:
            if shop_id:
                self.logger.info(f"[{self.name}] Processing shop_id={shop_id}")
            else:
                self.logger.info(f"[{self.name}] Processing all shops")
            
            # Date windowing
            window_start = start_date
            total_records = 0
            
            while window_start < end_date:
                # Calculate window end (don't exceed today)
                window_end = min(
                    window_start + timedelta(days=self.date_window_days),
                    end_date
                )
                
                self.logger.info(
                    f"[{self.name}] Processing window: {window_start.date()} to {window_end.date()}"
                )
                
                # Paginate through this window with lastId
                last_id: str | None = None
                window_records = 0
                
                while True:
                    params = self._get_window_params(window_start, window_end, 1, last_id)
                    if shop_id:
                        params["shopId"] = shop_id
                    
                    self.logger.debug(f"[{self.name}] Window request params: {params}")
                    
                    prepared_request = self.build_prepared_request(
                        method="GET",
                        url=self.get_url(context),
                        params=params,
                        headers=self.http_headers,
                    )
                    
                    try:
                        response = self._request(prepared_request, context)
                    except Exception as e:
                        self.logger.error(f"[{self.name}] Request failed: {e}")
                        break
                    
                    if response.status_code != 200:
                        self.logger.error(
                            f"[{self.name}] API error {response.status_code}: {response.text[:500]}"
                        )
                        break
                    
                    # Parse response to get price rules (before flattening)
                    price_rules = list(super(DateWindowedStream, self).parse_response(response))
                    page_count = len(price_rules)
                    
                    if not price_rules:
                        break
                    
                    # Flatten and filter records
                    for price_rule in price_rules:
                        for record in self._flatten_price_rule(price_rule):
                            # Filter by replication key
                            if self._should_include_record(record, bookmark_date):
                                total_records += 1
                                window_records += 1
                                yield record
                    
                    # Get lastId from last price rule (not flattened record)
                    last_rule = price_rules[-1]
                    if isinstance(last_rule, dict) and self.last_id_field in last_rule:
                        last_id = str(last_rule[self.last_id_field])
                    else:
                        self.logger.warning(
                            f"[{self.name}] Could not find {self.last_id_field} in last rule"
                        )
                        break
                    
                    # If we got fewer than count, we're done with this window
                    if page_count < self.default_count:
                        break
                
                self.logger.info(
                    f"[{self.name}] Window {window_start.date()}-{window_end.date()} "
                    f"complete: {window_records} records"
                )
                
                # Move to next window
                window_start = window_end
            
            self.logger.info(
                f"[{self.name}] Shop {shop_id or 'all'} complete: {total_records} total records"
            )

    def _should_include_record(self, record: dict, bookmark_date: datetime | None) -> bool:
        """Check if record should be included based on replication key.
        
        Args:
            record: The flattened price record.
            bookmark_date: The bookmark date from state.
            
        Returns:
            True if record should be included (newer than bookmark or no bookmark).
        """
        if not bookmark_date:
            return True  # No bookmark, include all
        
        record_date = record.get(self.replication_key)
        if not record_date:
            # If no date_modified, include it (will be set to current time in post_process)
            return True
        
        # Parse record date if it's a string
        if isinstance(record_date, str):
            try:
                # Try ISO format first
                if "T" in record_date or "+" in record_date or "Z" in record_date:
                    record_date = datetime.fromisoformat(record_date.replace("Z", "+00:00"))
                else:
                    # Try date-only format
                    record_date = datetime.strptime(record_date, "%Y-%m-%d")
            except (ValueError, TypeError):
                # If we can't parse it, include it to be safe
                return True
        
        # Compare dates - include if record is newer than bookmark
        if isinstance(bookmark_date, str):
            try:
                bookmark_date = datetime.fromisoformat(bookmark_date.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                return True
        
        # Include if record date is >= bookmark date
        return record_date >= bookmark_date

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
            self.date_param_name: window_start.strftime("%Y-%m-%dT%H:%M:%S"),
        }
        
        # Price API uses lastId pagination
        if self.use_last_id_pagination:
            params[self.last_id_param] = last_id if last_id else "0"
        
        return params

    def _flatten_price_rule(self, price_rule: dict) -> t.Iterable[dict]:
        """Flatten a price rule into individual price records.

        Args:
            price_rule: The price rule containing SKU and prices.

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

        for price in prices:
            record = {
                "sku_tilroy_id": sku.get("tilroyId"),
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
                "date_modified": price.get("dateModified"),
                "tenant_id": tenant_id,
                "shop_tilroy_id": shop.get("tilroyId"),
                "shop_number": shop.get("number"),
                "country_tilroy_id": country.get("tilroyId"),
                "country_code": country.get("code"),
            }

            # Ensure date_modified is set (required for replication)
            if not record["date_modified"]:
                record["date_modified"] = (
                    record.get("date_created") or datetime.utcnow().isoformat()
                )

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
            row["date_modified"] = datetime.utcnow().isoformat()

        return row
