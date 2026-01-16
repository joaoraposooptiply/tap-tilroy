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

    # Complete schema based on Tilroy Shop API specification
    schema = th.PropertiesList(
        # Primary identifiers
        th.Property("tilroyId", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("sourceId", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("number", th.CustomType({"type": ["string", "number", "null"]})),

        # Basic info
        th.Property("name", th.CustomType({"type": ["string", "null"]})),
        th.Property("shortName", th.CustomType({"type": ["string", "null"]})),
        th.Property("activeShop", th.BooleanType),

        # Classification
        th.Property("type", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("subType", th.CustomType({"type": ["object", "string", "null"]})),

        # Location/Address
        th.Property("street", th.CustomType({"type": ["string", "null"]})),
        th.Property("houseNumber", th.CustomType({"type": ["string", "null"]})),
        th.Property("box", th.CustomType({"type": ["string", "null"]})),
        th.Property("postalCode", th.CustomType({"type": ["string", "null"]})),
        th.Property("city", th.CustomType({"type": ["string", "null"]})),
        th.Property("country", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("latitude", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("longitude", th.CustomType({"type": ["string", "number", "null"]})),

        # Contact
        th.Property("phone", th.CustomType({"type": ["string", "null"]})),
        th.Property("fax", th.CustomType({"type": ["string", "null"]})),
        th.Property("email", th.CustomType({"type": ["string", "null"]})),
        th.Property("webUrl", th.CustomType({"type": ["string", "null"]})),

        # Language & Currency
        th.Property("language", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("currency", th.CustomType({"type": ["object", "string", "null"]})),

        # Legal entity
        th.Property("legalEntityId", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("legalEntity", th.CustomType({"type": ["object", "string", "null"]})),

        # Store management
        th.Property("nameStoreManager", th.CustomType({"type": ["string", "null"]})),

        # Opening hours
        th.Property("openingHours", th.CustomType({"type": ["string", "null"]})),
        th.Property("openHoursMon", th.CustomType({"type": ["string", "null"]})),
        th.Property("openHoursTue", th.CustomType({"type": ["string", "null"]})),
        th.Property("openHoursWed", th.CustomType({"type": ["string", "null"]})),
        th.Property("openHoursThu", th.CustomType({"type": ["string", "null"]})),
        th.Property("openHoursFri", th.CustomType({"type": ["string", "null"]})),
        th.Property("openHoursSat", th.CustomType({"type": ["string", "null"]})),
        th.Property("openHoursSun", th.CustomType({"type": ["string", "null"]})),

        # Refill settings
        th.Property("refillFromOtherShop", th.BooleanType),
        th.Property("refillShop", th.CustomType({"type": ["object", "string", "null"]})),

        # URLs
        th.Property("tenantDomain", th.CustomType({"type": ["string", "null"]})),
        th.Property("urlPrefix", th.CustomType({"type": ["string", "null"]})),

        # Nested arrays (stored as JSON strings)
        th.Property("descriptions", th.CustomType({"type": ["array", "string", "null"]})),
        th.Property("tills", th.CustomType({"type": ["array", "string", "null"]})),
        th.Property("moneyLocations", th.CustomType({"type": ["array", "string", "null"]})),
    ).to_dict()

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: int | None,
    ) -> dict[str, t.Any]:
        """Return URL parameters for the shops endpoint.

        The /shops endpoint does NOT support pagination (page/count).
        It only supports filtering by sourceId, number, and fields.

        Args:
            context: Stream partition context.
            next_page_token: Not used - endpoint has no pagination.

        Returns:
            Empty dict (no required parameters).
        """
        # The /shops endpoint returns all shops in a single response
        # No pagination parameters needed
        return {}

    def request_records(self, context: Context | None) -> t.Iterable[dict]:
        """Request all shop records in a single API call.

        The /shops endpoint returns all shops without pagination,
        so we make one request and yield all results.

        Args:
            context: Stream partition context.

        Yields:
            Shop records from the API.
        """
        prepared_request = self.build_prepared_request(
            method="GET",
            url=self.get_url(context),
            params=self.get_url_params(context, None),
            headers=self.http_headers,
        )

        response = self._request(prepared_request, context)

        for record in self.parse_response(response):
            processed = self.post_process(record, context)
            if processed:
                yield processed

    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        """Post-process shop record.

        Converts nested objects and arrays to JSON strings for compatibility.
        """
        row = super().post_process(row, context)
        if row:
            row = self._stringify_nested_objects(row)
        return row
