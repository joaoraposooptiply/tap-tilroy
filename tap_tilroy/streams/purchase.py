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

        Historical endpoint uses 'orderDateFrom' + 'orderDateTo', incremental uses 'dateFrom'.
        Note: The historical endpoint REQUIRES orderDateTo to return any records.
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
            # Historical endpoint requires both orderDateFrom AND orderDateTo
            params["orderDateFrom"] = start_date.strftime("%Y-%m-%d")
            params["orderDateTo"] = datetime.now().strftime("%Y-%m-%d")

        # Add warehouse and status filters from context if provided
        # Note: API requires status filter when using warehouseNumber
        warehouse_number = (context or {}).get("warehouse_number")
        status = (context or {}).get("status")
        if warehouse_number:
            params["warehouseNumber"] = warehouse_number
        if status:
            params["status"] = status

        return params

    @property
    def partitions(self) -> list[dict] | None:
        """Return partitions for each warehouse number and status if configured.
        
        If purchase_orders_warehouse_numbers is configured, creates partitions
        for each warehouse + status combination. The API requires a status filter
        when using warehouseNumber, so we query for all common statuses.
        """
        warehouse_numbers = self.config.get("purchase_orders_warehouse_numbers", [])
        
        if not warehouse_numbers:
            return None  # No filter - get all warehouses
        
        # API requires status filter with warehouseNumber
        # Query each warehouse with each status to get complete data
        statuses = ["draft", "open", "delivered", "cancelled"]
        
        partitions = []
        for wh in warehouse_numbers:
            for status in statuses:
                partitions.append({"warehouse_number": wh, "status": status})
        
        self.logger.info(
            f"[{self.name}] Filtering by warehouse numbers: {warehouse_numbers} "
            f"with statuses: {statuses}"
        )
        return partitions

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
