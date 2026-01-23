"""Purchase Orders stream for Tilroy API."""

from __future__ import annotations

import typing as t
from datetime import datetime, timedelta

import requests
from singer_sdk import typing as th
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_tilroy.client import TilroyStream

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


# All statuses to query (API requires status with warehouseNumber)
PURCHASE_ORDER_STATUSES = ["draft", "open", "delivered", "cancelled"]


class PurchaseOrdersStream(TilroyStream):
    """Stream for Tilroy purchase orders.

    Always uses /purchaseorders endpoint with orderDateFrom + orderDateTo.
    Iterates through warehouse_id + status combinations internally to avoid
    per-partition state bloat. Maintains a single global bookmark.
    
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
                    th.Property("created", th.CustomType({"type": ["object", "string", "null"]})),
                    th.Property("modified", th.CustomType({"type": ["object", "string", "null"]})),
                )
            ),
        ),
    ).to_dict()

    def _get_start_date(self) -> datetime:
        """Determine the start date for filtering from global bookmark.

        Returns:
            The start date for the query.
        """
        # Try to get from bookmark (no context = global bookmark)
        bookmark_date = self.get_starting_timestamp(None)

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

    def _fetch_page(
        self,
        warehouse_id: int | None,
        status: str | None,
        page: int,
        start_date: datetime,
    ) -> tuple[list[dict], bool]:
        """Fetch a single page of purchase orders.
        
        Args:
            warehouse_id: Warehouse number filter (requires status).
            status: Status filter (required with warehouse_id).
            page: Page number.
            start_date: Start date for orderDateFrom.
            
        Returns:
            Tuple of (records, has_more_pages).
        """
        params = {
            "count": self.default_count,
            "page": page,
            "orderDateFrom": start_date.strftime("%Y-%m-%d"),
            "orderDateTo": datetime.now().strftime("%Y-%m-%d"),
        }
        
        if warehouse_id and status:
            params["warehouseNumber"] = warehouse_id
            params["status"] = status
        
        url = f"{self.url_base}{self.path}"
        
        response = requests.get(
            url,
            params=params,
            headers=self.http_headers,
            timeout=60,
        )
        response.raise_for_status()
        
        # Check pagination headers
        current_page = int(response.headers.get("X-Paging-CurrentPage", 1))
        total_pages = int(response.headers.get("X-Paging-PageCount", 1))
        has_more = current_page < total_pages
        
        data = response.json()
        records = list(extract_jsonpath(self.records_jsonpath, input=data))
        
        return records, has_more

    def _fetch_all_for_filter(
        self,
        warehouse_id: int | None,
        status: str | None,
        start_date: datetime,
    ) -> t.Iterable[dict]:
        """Fetch all pages for a warehouse/status combination.
        
        Args:
            warehouse_id: Warehouse number filter.
            status: Status filter.
            start_date: Start date for filtering.
            
        Yields:
            Purchase order records.
        """
        page = 1
        while True:
            records, has_more = self._fetch_page(warehouse_id, status, page, start_date)
            
            # Break if no records returned (avoid infinite loop)
            if not records:
                break
            
            for record in records:
                yield record
            
            if not has_more:
                break
            page += 1

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Fetch purchase orders, iterating through warehouse/status combos internally.
        
        This avoids partition-based state. Single global bookmark is maintained.
        """
        start_date = self._get_start_date()
        warehouse_ids = getattr(self._tap, "_resolved_shop_ids", [])
        
        if not warehouse_ids:
            # No warehouse filter - fetch all
            self.logger.info(f"[{self.name}] Fetching all purchase orders from {start_date.date()}")
            yield from self._fetch_all_for_filter(None, None, start_date)
        else:
            # Iterate through each warehouse + status combination
            self.logger.info(
                f"[{self.name}] Fetching for warehouses {warehouse_ids} from {start_date.date()}"
            )
            for wh_id in warehouse_ids:
                for status in PURCHASE_ORDER_STATUSES:
                    yield from self._fetch_all_for_filter(wh_id, status, start_date)

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
