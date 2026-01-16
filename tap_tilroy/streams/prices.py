"""Prices stream for Tilroy API."""

from __future__ import annotations

import typing as t
from datetime import datetime

from singer_sdk import typing as th

from tap_tilroy.client import LastIdPaginatedStream

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class PricesStream(LastIdPaginatedStream):
    """Stream for Tilroy price rules.

    Uses /price/rules endpoint with lastId pagination.
    Flattens nested price structures into individual records.
    """

    name = "prices"
    path = "/priceapi/production/price/rules"
    primary_keys: t.ClassVar[list[str]] = ["sku_tilroy_id", "price_tilroy_id", "type"]
    replication_key = "date_modified"
    replication_method = "INCREMENTAL"
    records_jsonpath = "$[*]"
    default_count = 100

    # Price API uses dateModified instead of dateFrom
    date_param_name = "dateModified"
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

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: str | None,
    ) -> dict[str, t.Any]:
        """Return URL parameters for Price API.

        Price API uses dateModified (datetime) instead of dateFrom (date).
        """
        start_date = self._get_start_date(context)

        params = {
            "count": self.default_count,
            "dateModified": start_date.strftime("%Y-%m-%dT%H:%M:%S"),
        }

        if next_page_token:
            params["lastId"] = next_page_token

        # Optional shop filter (required for large tenants to avoid timeout)
        # API accepts shopId (tilroyId) - see /price/rules endpoint docs
        shop_id = self.config.get("prices_shop_id")
        if shop_id:
            params["shopId"] = shop_id

        return params

    def request_records(self, context: Context | None) -> t.Iterable[dict]:
        """Request and flatten price rule records.

        Each price rule can contain multiple prices, which we flatten
        into individual records.

        Args:
            context: Stream partition context.

        Yields:
            Flattened price records.
        """
        last_id: str | None = None

        while True:
            params = self.get_url_params(context, last_id)

            self.logger.info(f"[{self.name}] Requesting with params: {params}")

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

            records = list(self.parse_response(response))
            self.logger.info(f"[{self.name}] Retrieved {len(records)} price rules")

            if not records:
                break

            # Flatten and yield each price rule
            for price_rule in records:
                yield from self._flatten_price_rule(price_rule, context)

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

        self.logger.info(f"[{self.name}] Pagination complete")

    def _flatten_price_rule(
        self,
        price_rule: dict,
        context: Context | None,
    ) -> t.Iterable[dict]:
        """Flatten a price rule into individual price records.

        Args:
            price_rule: The price rule containing SKU and prices.
            context: Stream partition context.

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

            processed = self.post_process(record, context)
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
