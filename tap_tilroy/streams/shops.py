"""Shop stream for Tilroy API."""

from __future__ import annotations

import typing as t

from singer_sdk import typing as th

from tap_tilroy.client import TilroyStream

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class ShopsStream(TilroyStream):
    """Stream for Tilroy shops.

    Fetches shop/store information from the Tilroy API.
    The /shops endpoint returns all shops without pagination support.
    """

    name = "shops"
    path = "/shopapi/production/shops"
    primary_keys: t.ClassVar[list[str]] = ["tilroyId"]
    replication_key = None
    replication_method = "FULL_TABLE"
    records_jsonpath = "$[*]"

    schema = th.PropertiesList(
        th.Property("tilroyId", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("sourceId", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("number", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("name", th.CustomType({"type": ["string", "null"]})),
        th.Property("shortName", th.CustomType({"type": ["string", "null"]})),
        th.Property("activeShop", th.BooleanType),
        th.Property("type", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("subType", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("street", th.CustomType({"type": ["string", "null"]})),
        th.Property("houseNumber", th.CustomType({"type": ["string", "null"]})),
        th.Property("box", th.CustomType({"type": ["string", "null"]})),
        th.Property("postalCode", th.CustomType({"type": ["string", "null"]})),
        th.Property("city", th.CustomType({"type": ["string", "null"]})),
        th.Property("country", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("latitude", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("longitude", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("phone", th.CustomType({"type": ["string", "null"]})),
        th.Property("fax", th.CustomType({"type": ["string", "null"]})),
        th.Property("email", th.CustomType({"type": ["string", "null"]})),
        th.Property("webUrl", th.CustomType({"type": ["string", "null"]})),
        th.Property("language", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("currency", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("legalEntityId", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("legalEntity", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("nameStoreManager", th.CustomType({"type": ["string", "null"]})),
        th.Property("openingHours", th.CustomType({"type": ["string", "null"]})),
        th.Property("openHoursMon", th.CustomType({"type": ["string", "null"]})),
        th.Property("openHoursTue", th.CustomType({"type": ["string", "null"]})),
        th.Property("openHoursWed", th.CustomType({"type": ["string", "null"]})),
        th.Property("openHoursThu", th.CustomType({"type": ["string", "null"]})),
        th.Property("openHoursFri", th.CustomType({"type": ["string", "null"]})),
        th.Property("openHoursSat", th.CustomType({"type": ["string", "null"]})),
        th.Property("openHoursSun", th.CustomType({"type": ["string", "null"]})),
        th.Property("refillFromOtherShop", th.BooleanType),
        th.Property("refillShop", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("tenantDomain", th.CustomType({"type": ["string", "null"]})),
        th.Property("urlPrefix", th.CustomType({"type": ["string", "null"]})),
        th.Property("descriptions", th.CustomType({"type": ["array", "string", "null"]})),
        th.Property("tills", th.CustomType({"type": ["array", "string", "null"]})),
        th.Property("moneyLocations", th.CustomType({"type": ["array", "string", "null"]})),
    ).to_dict()

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: int | None,
    ) -> dict[str, t.Any]:
        """The /shops endpoint does not support pagination."""
        return {}

    def request_records(self, context: Context | None) -> t.Iterable[dict]:
        prepared_request = self.build_prepared_request(
            method="GET",
            url=self.get_url(context),
            params=self.get_url_params(context, None),
            headers=self.http_headers,
        )

        response = self._request_with_backoff(prepared_request, context)

        for record in self.parse_response(response):
            processed = self.post_process(record, context)
            if processed:
                yield processed

    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        row = super().post_process(row, context)
        if row:
            row = self._stringify_nested_objects(row)
        return row
