"""Shop stream for Tilroy API."""

from __future__ import annotations

import typing as t

from singer_sdk import typing as th

from tap_tilroy.client import TilroyStream


class ShopsStream(TilroyStream):
    """Stream for Tilroy shops.
    
    Fetches shop/store information from the Tilroy API.
    This is typically a small dataset that can be fully synced.
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
        th.Property("name", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("type", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("subType", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("language", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("latitude", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("longitude", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("postalCode", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("street", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("houseNumber", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("legalEntityId", th.IntegerType),
        th.Property("country", th.CustomType({"type": ["object", "string", "null"]})),
    ).to_dict()

    def post_process(
        self,
        row: dict,
        context: t.Optional[dict] = None,
    ) -> dict | None:
        """Post-process shop record, converting nested objects to JSON strings."""
        row = super().post_process(row, context)
        if row:
            row = self._stringify_nested_objects(row)
        return row
