"""Prices stream for Tilroy API."""

from __future__ import annotations

import typing as t
from datetime import datetime, timedelta

from singer_sdk import typing as th

from tap_tilroy.client import TilroyStream

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class PricesStream(TilroyStream):
    """Stream for Tilroy price rules.

    Uses /price/rules endpoint with lastId pagination.
    Flattens nested price structures into individual records.
    Iterates through shop_ids internally to avoid per-partition state bloat.
    """

    name = "prices"
    path = "/priceapi/production/price/rules"
    primary_keys: t.ClassVar[list[str]] = ["sku_tilroy_id", "price_tilroy_id", "type"]
    replication_key = "date_modified"
    replication_method = "INCREMENTAL"
    records_jsonpath = "$[*]"
    default_count = 100

    # Price API uses dateModified instead of dateFrom
    last_id_field = "tilroyId"  # Top-level rule ID

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

    def _get_start_date(self) -> datetime:
        """Determine the start date for filtering from global bookmark.

        Returns:
            The start date for the query.
        """
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

    def _fetch_all_for_shop(
        self,
        shop_id: int | None,
        start_date: datetime,
    ) -> t.Iterable[dict]:
        """Fetch all price rules for a shop using lastId pagination.
        
        Args:
            shop_id: Shop tilroyId filter (optional).
            start_date: Start date for filtering.
            
        Yields:
            Flattened price records.
        """
        last_id: str | None = None

        while True:
            params = {
                "count": self.default_count,
                "dateModified": start_date.strftime("%Y-%m-%dT%H:%M:%S"),
            }
            
            if last_id:
                params["lastId"] = last_id
            
            if shop_id:
                params["shopId"] = shop_id

            self.logger.debug(f"[{self.name}] Requesting with params: {params}")

            prepared_request = self.build_prepared_request(
                method="GET",
                url=self.get_url(None),
                params=params,
                headers=self.http_headers,
            )

            response = self._request(prepared_request, None)

            if response.status_code != 200:
                self.logger.error(
                    f"[{self.name}] API error {response.status_code}: {response.text[:500]}"
                )
                break

            records = list(self.parse_response(response))
            self.logger.debug(f"[{self.name}] Retrieved {len(records)} price rules")

            if not records:
                break

            # Flatten and yield each price rule
            for price_rule in records:
                yield from self._flatten_price_rule(price_rule)

            # Get lastId for next page
            last_record = records[-1]
            if isinstance(last_record, dict) and self.last_id_field in last_record:
                last_id = str(last_record[self.last_id_field])
            else:
                self.logger.warning(
                    f"[{self.name}] Could not find {self.last_id_field} in last record"
                )
                break

            if len(records) < self.default_count:
                break

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Fetch prices, iterating through shops internally.
        
        This avoids partition-based state. Single global bookmark is maintained.
        """
        start_date = self._get_start_date()
        shop_ids = getattr(self._tap, "_resolved_shop_ids", [])
        
        if not shop_ids:
            # No shop filter - fetch all
            self.logger.info(f"[{self.name}] Fetching all prices from {start_date.date()}")
            yield from self._fetch_all_for_shop(None, start_date)
        else:
            # Iterate through each shop
            self.logger.info(
                f"[{self.name}] Fetching prices for shops {shop_ids} from {start_date.date()}"
            )
            for shop_id in shop_ids:
                self.logger.debug(f"[{self.name}] Fetching shop_id={shop_id}")
                yield from self._fetch_all_for_shop(shop_id, start_date)

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
