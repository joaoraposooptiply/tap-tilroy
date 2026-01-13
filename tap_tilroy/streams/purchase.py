"""Purchase Orders stream for Tilroy API."""

from __future__ import annotations

import typing as t
from datetime import datetime

from singer_sdk import typing as th

from tap_tilroy.client import DynamicRoutingStream

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class PurchaseOrdersStream(DynamicRoutingStream):
    """Stream for Tilroy purchase orders.

    Uses dynamic routing:
    - First sync: /purchaseorders with orderDateFrom for historical data
    - Subsequent syncs: /export/orders with dateFrom for delta updates
    """

    name = "purchase_orders"
    historical_path = "/purchaseapi/production/purchaseorders"
    incremental_path = "/purchaseapi/production/export/orders"
    primary_keys: t.ClassVar[list[str]] = ["tilroyId"]
    replication_key = "orderDate"
    replication_method = "INCREMENTAL"
    records_jsonpath = "$[*]"
    default_count = 500

    # Historical endpoint uses different date parameter
    date_param_name = "orderDateFrom"

    schema = th.PropertiesList(
        th.Property("tilroyId", th.CustomType({"type": ["string", "integer"]})),
        th.Property("number", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("orderDate", th.DateTimeType),
        th.Property("supplier", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("supplierReference", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("requestedDeliveryDate", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("warehouse", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("currency", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("prices", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("status", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("created", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("modified", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property(
            "lines",
            th.ArrayType(
                th.ObjectType(
                    th.Property(
                        "sku",
                        th.ObjectType(
                            th.Property("tilroyId", th.CustomType({"type": ["string", "number", "null"]})),
                            th.Property("sourceId", th.CustomType({"type": ["string", "number", "null"]})),
                        ),
                    ),
                    th.Property(
                        "warehouse",
                        th.ObjectType(
                            th.Property("number", th.IntegerType),
                            th.Property("name", th.CustomType({"type": ["string", "number", "null"]})),
                        ),
                    ),
                    th.Property("status", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property("requestedDeliveryDate", th.DateTimeType),
                    th.Property(
                        "qty",
                        th.ObjectType(
                            th.Property("ordered", th.IntegerType),
                            th.Property("delivered", th.IntegerType),
                            th.Property("backOrder", th.IntegerType),
                            th.Property("cancelled", th.IntegerType),
                        ),
                    ),
                    th.Property("prices", th.CustomType({"type": ["object", "string", "null"]})),
                    th.Property("discount", th.CustomType({"type": ["object", "string", "null"]})),
                    th.Property("id", th.CustomType({"type": ["string", "number", "null"]})),
                )
            ),
        ),
    ).to_dict()

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: int | None,
    ) -> dict[str, t.Any]:
        """Return URL parameters with appropriate date parameter name.

        Historical endpoint uses 'orderDateFrom', incremental uses 'dateFrom'.
        """
        params = {
            "count": self.default_count,
            "page": next_page_token or 1,
        }

        start_date = self._get_start_date(context)

        # Use different parameter name based on which path we're using
        if self._has_existing_state():
            params["dateFrom"] = start_date.strftime("%Y-%m-%d")
        else:
            params["orderDateFrom"] = start_date.strftime("%Y-%m-%d")

        return params

    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        """Post-process purchase order record.

        Validates required fields and converts date formats.
        """
        if not row:
            return None

        # Skip error responses
        if "code" in row and "message" in row:
            self.logger.warning(f"[{self.name}] Skipping error record: {row['message']}")
            return None

        # Validate orderDate exists
        if not row.get("orderDate"):
            self.logger.warning(f"[{self.name}] Skipping record without orderDate")
            return None

        # Parse orderDate string to datetime
        order_date = row["orderDate"]
        if isinstance(order_date, str):
            try:
                row["orderDate"] = datetime.fromisoformat(
                    order_date.replace("Z", "+00:00")
                )
            except ValueError:
                self.logger.warning(f"[{self.name}] Invalid orderDate format: {order_date}")
                return None

        return row
