"""Product and Supplier streams for Tilroy API."""

from __future__ import annotations

import typing as t
from datetime import datetime

from singer_sdk import typing as th

from tap_tilroy.client import DynamicRoutingStream, TilroyStream

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class ProductsStream(DynamicRoutingStream):
    """Stream for Tilroy products.

    Uses dynamic routing to switch between historical and incremental endpoints:
    - First sync (no state): Uses /products for full historical data
    - Subsequent syncs: Uses /export/products for delta updates
    
    Also collects SKU IDs for use by the StockStream.
    """

    name = "products"
    historical_path = "/product-bulk/production/products"
    incremental_path = "/product-bulk/production/export/products"
    primary_keys: t.ClassVar[list[str]] = ["tilroyId"]
    replication_key = "extraction_timestamp"  # Synthetic replication key
    replication_method = "INCREMENTAL"
    records_jsonpath = "$[*]"
    default_count = 1000  # Product API allows up to 1000 per page

    # Class-level storage for SKU IDs (shared across instances)
    _collected_sku_ids: t.ClassVar[list[str]] = []

    schema = th.PropertiesList(
        th.Property("tilroyId", th.CustomType({"type": ["string", "integer"]})),
        th.Property("sourceId", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("code", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property(
            "descriptions",
            th.ArrayType(
                th.ObjectType(
                    th.Property("languageCode", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property("standard", th.CustomType({"type": ["string", "number", "null"]})),
                )
            ),
        ),
        th.Property("supplier", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("brand", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property(
            "colours",
            th.ArrayType(
                th.ObjectType(
                    th.Property("tilroyId", th.CustomType({"type": ["string", "integer"]})),
                    th.Property("sourceId", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property("code", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property(
                        "skus",
                        th.ArrayType(
                            th.ObjectType(
                                th.Property("tilroyId", th.CustomType({"type": ["string", "integer"]})),
                                th.Property("sourceId", th.CustomType({"type": ["string", "number", "null"]})),
                                th.Property("costPrice", th.NumberType),
                                th.Property(
                                    "barcodes",
                                    th.ArrayType(
                                        th.ObjectType(
                                            th.Property("code", th.CustomType({"type": ["string", "number", "null"]})),
                                            th.Property("quantity", th.NumberType),
                                            th.Property("isInternal", th.BooleanType),
                                        )
                                    ),
                                ),
                                th.Property(
                                    "size",
                                    th.ObjectType(
                                        th.Property("code", th.CustomType({"type": ["string", "number", "null"]})),
                                    ),
                                ),
                                th.Property("rrp", th.CustomType({"type": ["array", "object", "string", "null"]})),
                            )
                        ),
                    ),
                    th.Property("pictures", th.ArrayType(th.ObjectType())),
                )
            ),
        ),
        th.Property("isUsed", th.BooleanType),
        th.Property("suppliers", th.CustomType({"type": ["array", "object", "string", "null"]})),
        th.Property("extraction_timestamp", th.DateTimeType),
    ).to_dict()

    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        """Post-process product record.

        Adds synthetic timestamp and collects SKU IDs for other streams.
        """
        if not row:
            return None

        # Add synthetic timestamp for incremental tracking
        row["extraction_timestamp"] = datetime.utcnow().isoformat()

        # Collect SKU IDs for use by StockStream
        self._collect_sku_ids(row)

        return row

    def _collect_sku_ids(self, product: dict) -> None:
        """Extract and store SKU IDs from product record.

        Args:
            product: The product record containing colours/skus.
        """
        colours = product.get("colours", [])
        if not isinstance(colours, list):
            return

        for colour in colours:
            skus = colour.get("skus", [])
            if not isinstance(skus, list):
                continue

            for sku in skus:
                sku_id = sku.get("tilroyId")
                if sku_id:
                    sku_id_str = str(sku_id)
                    if sku_id_str not in self._collected_sku_ids:
                        self._collected_sku_ids.append(sku_id_str)

                        # Log progress periodically
                        if len(self._collected_sku_ids) % 500 == 0:
                            self.logger.info(
                                f"[{self.name}] Collected {len(self._collected_sku_ids)} SKU IDs..."
                            )

    @classmethod
    def get_collected_sku_ids(cls) -> list[str]:
        """Get the collected SKU IDs for use by other streams.

        Returns:
            Copy of the collected SKU IDs list.
        """
        return cls._collected_sku_ids.copy()

    @classmethod
    def clear_collected_sku_ids(cls) -> None:
        """Clear the collected SKU IDs."""
        cls._collected_sku_ids.clear()

    def finalize_child_contexts(self) -> None:
        """Log final SKU collection statistics."""
        self.logger.info(
            f"[{self.name}] Final: {len(self._collected_sku_ids)} unique SKU IDs collected"
        )


class SuppliersStream(TilroyStream):
    """Stream for Tilroy suppliers.

    Fetches supplier master data. This is typically a small dataset.
    """

    name = "suppliers"
    path = "/product-bulk/production/suppliers"
    primary_keys: t.ClassVar[list[str]] = ["tilroyId"]
    replication_key = None
    replication_method = "FULL_TABLE"
    records_jsonpath = "$[*]"
    default_count = 10000  # Suppliers are typically few, fetch all at once

    schema = th.PropertiesList(
        th.Property("tilroyId", th.IntegerType),
        th.Property("code", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("name", th.CustomType({"type": ["string", "number", "null"]})),
    ).to_dict()
