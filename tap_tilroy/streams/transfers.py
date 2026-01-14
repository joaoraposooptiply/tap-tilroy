"""Transfers stream for Tilroy API."""

from __future__ import annotations

import typing as t

from singer_sdk import typing as th

from tap_tilroy.client import DateFilteredStream

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class TransfersStream(DateFilteredStream):
    """Stream for Tilroy transfers (stock movements between shops).

    Uses /export/transfers endpoint with date filtering and page-based pagination.
    Tracks inventory transfers, requests, and receipts between locations.
    """

    name = "transfers"
    path = "/transferapi/production/export/transfers"
    primary_keys: t.ClassVar[list[str]] = ["idTilroy"]
    replication_key = "dateExported"
    replication_method = "INCREMENTAL"
    records_jsonpath = "$[*]"
    default_count = 100

    schema = th.PropertiesList(
        # Primary identifiers
        th.Property("idTilroy", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("idTenant", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("idSource", th.CustomType({"type": ["string", "number", "null"]})),

        # Transfer info
        th.Property("code", th.CustomType({"type": ["string", "null"]})),
        th.Property("reference", th.CustomType({"type": ["string", "null"]})),
        th.Property("description", th.CustomType({"type": ["string", "null"]})),
        th.Property("additionalInfo", th.CustomType({"type": ["string", "null"]})),

        # Status: N=New, Q=Request, T=Transferred, R=Received, C=Cancelled
        th.Property("status", th.CustomType({"type": ["string", "null"]})),

        # Flags
        th.Property("isRequest", th.BooleanType),
        th.Property("isExternalDelivery", th.BooleanType),
        th.Property("isDifference", th.BooleanType),
        th.Property("isPartial", th.BooleanType),

        # Dates
        th.Property("dateTransferred", th.CustomType({"type": ["string", "null"]})),
        th.Property("dateReceived", th.CustomType({"type": ["string", "null"]})),
        th.Property("dateRequested", th.CustomType({"type": ["string", "null"]})),
        th.Property("dateInserted", th.CustomType({"type": ["string", "null"]})),
        th.Property("dateModified", th.CustomType({"type": ["string", "null"]})),
        th.Property("dateExported", th.DateTimeType),

        # Shop references (nested objects -> JSON strings)
        th.Property("shopFrom", th.CustomType({"type": ["object", "string", "null"]})),
        # Flattened shopFrom fields for easier filtering
        th.Property("shop_from_tilroy_id", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("shop_from_number", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("shop_from_name", th.CustomType({"type": ["string", "null"]})),
        th.Property("shop_from_source_id", th.CustomType({"type": ["string", "null"]})),
        th.Property("shopTo", th.CustomType({"type": ["object", "string", "null"]})),
        # Flattened shopTo fields for easier filtering
        th.Property("shop_to_tilroy_id", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("shop_to_number", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("shop_to_name", th.CustomType({"type": ["string", "null"]})),
        th.Property("shop_to_source_id", th.CustomType({"type": ["string", "null"]})),

        # User references (nested objects -> JSON strings)
        th.Property("userRequested", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("userTransferred", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("userReceived", th.CustomType({"type": ["object", "string", "null"]})),

        # Related objects
        th.Property("basedOn", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("customerOrder", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("deliveryAddress", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("purchaseOrderId", th.CustomType({"type": ["string", "number", "null"]})),

        # Lines array (transfer line items)
        th.Property("lines", th.CustomType({"type": ["array", "string", "null"]})),
    ).to_dict()

    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        """Post-process transfer record.

        Flattens shop objects and converts nested objects to JSON strings.
        """
        row = super().post_process(row, context)
        if row:
            # Flatten shopFrom and shopTo for easier filtering
            row = self._flatten_shop(row, "shopFrom", "shop_from")
            row = self._flatten_shop(row, "shopTo", "shop_to")
            row = self._stringify_nested_objects(row)
        return row
