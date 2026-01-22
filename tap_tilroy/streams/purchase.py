"""Purchase Orders stream for Tilroy API."""

from __future__ import annotations

import typing as t
from datetime import datetime, timedelta

from singer_sdk import typing as th

from tap_tilroy.client import TilroyStream

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class PurchaseOrdersStream(TilroyStream):
    """Stream for Tilroy purchase orders.

    Always uses /purchaseorders endpoint with orderDateFrom + orderDateTo.
    The /export/orders endpoint is for orders "created for export" which is
    a different concept - not suitable for general incremental extraction.
    
    Note: warehouseNumber filter requires status filter.
    """

    name = "purchase_orders"
    path = "/purchaseapi/production/purchaseorders"
    primary_keys: t.ClassVar[list[str]] = ["tilroyId"]
    replication_key = "orderDate"
    replication_method = "INCREMENTAL"
    records_jsonpath = "$[*]"
    default_count = 500

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

    def _get_start_date(self, context: Context | None) -> datetime:
        """Determine the start date for filtering.

        Args:
            context: Stream partition context.

        Returns:
            The start date for the query.
        """
        # Try to get from bookmark
        bookmark_date = self.get_starting_timestamp(context)

        if bookmark_date:
            # Go back 1 day to avoid missing records at boundary
            if hasattr(bookmark_date, "date"):
                date_only = bookmark_date.date()
            else:
                date_only = bookmark_date
            return datetime.combine(date_only - timedelta(days=1), datetime.min.time())

        # Fall back to config start_date
        config_start = self.config.get("start_date", "2010-01-01T00:00:00Z")
        date_part = config_start.split("T")[0]
        return datetime.strptime(date_part, "%Y-%m-%d")

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: int | None,
    ) -> dict[str, t.Any]:
        """Return URL parameters for /purchaseorders endpoint.

        Always uses orderDateFrom + orderDateTo + optional warehouseNumber + status.
        """
        params = {
            "count": self.default_count,
            "page": next_page_token or 1,
        }

        start_date = self._get_start_date(context)
        
        # Always use orderDateFrom and orderDateTo
        params["orderDateFrom"] = start_date.strftime("%Y-%m-%d")
        params["orderDateTo"] = datetime.now().strftime("%Y-%m-%d")
        
        # Add warehouseNumber + status filters (must be used together)
        warehouse_id = (context or {}).get("warehouse_id")
        status = (context or {}).get("status")
        if warehouse_id and status:
            params["warehouseNumber"] = warehouse_id
            params["status"] = status

        return params

    @property
    def partitions(self) -> list[dict] | None:
        """Return partitions for each warehouse ID and status if configured.
        
        API requires status when using warehouseNumber.
        """
        warehouse_ids = getattr(self._tap, "_resolved_shop_ids", [])
        
        if not warehouse_ids:
            return None  # No filter - get all warehouses
        
        # Must use status with warehouseNumber
        statuses = ["draft", "open", "delivered", "cancelled"]
        
        partitions = []
        for wh_id in warehouse_ids:
            for status in statuses:
                partitions.append({"warehouse_id": wh_id, "status": status})
        
        self.logger.info(
            f"[{self.name}] Filtering by warehouses {warehouse_ids} "
            f"with statuses {statuses}"
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
