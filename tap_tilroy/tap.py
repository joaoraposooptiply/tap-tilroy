"""Tilroy tap class."""

from __future__ import annotations

import logging
import typing as t

import requests
from singer_sdk import Tap
from singer_sdk import typing as th
from singer_sdk.streams import Stream

from tap_tilroy.streams import (
    PricesStream,
    ProductsStream,
    PurchaseOrdersStream,
    SalesStream,
    ShopsStream,
    StockChangesStream,
    StockDeltasStream,
    StockStream,
    SuppliersStream,
    TransfersStream,
)

# Stream types in default order
STREAM_TYPES: list[type[Stream]] = [
    ProductsStream,
    ShopsStream,
    PurchaseOrdersStream,
    StockChangesStream,
    StockDeltasStream,
    SalesStream,
    SuppliersStream,
    PricesStream,
    StockStream,
    TransfersStream,
]


class TapTilroy(Tap):
    """Tilroy tap for extracting data from the Tilroy API.

    This tap supports the following streams:
    - products: Product catalog with SKU information
    - shops: Store/location data
    - purchase_orders: Purchase order history
    - stock_changes: Inventory movement history (snapshots)
    - stock_deltas: Inventory change events with deltas (transfers, corrections, etc.)
    - sales: Sales transactions
    - suppliers: Supplier master data
    - prices: Price rules per SKU
    - stock: Current stock levels (depends on products for SKU IDs)
    - transfers: Stock movements between shops (inter-store transfers)
    """

    name = "tap-tilroy"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "tilroy_api_key",
            th.StringType,
            required=True,
            secret=True,
            description="The Tilroy API key for authentication",
        ),
        th.Property(
            "x_api_key",
            th.StringType,
            required=True,
            secret=True,
            description="The AWS API Gateway key for authentication",
        ),
        th.Property(
            "api_url",
            th.StringType,
            required=True,
            description="The base URL for the Tilroy API (e.g., https://api.tilroy.com)",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest date to sync data from (ISO 8601 format)",
        ),
        th.Property(
            "filter_shop_ids",
            th.ArrayType(th.IntegerType),
            description="Shop tilroyIds to filter streams (optional). Tap will auto-resolve shop numbers.",
        ),
        th.Property(
            "filter_shop_numbers",
            th.ArrayType(th.IntegerType),
            description="Shop numbers to filter streams (optional). Tap will auto-resolve tilroyIds.",
        ),
    ).to_dict()

    # Resolved shop mappings (populated in __init__)
    _resolved_shop_ids: list[int] = []
    _resolved_shop_numbers: list[int] = []

    def __init__(self, *args, **kwargs) -> None:
        """Initialize the tap.

        Suppresses excessive schema warnings for streams that return
        many undocumented fields. Also resolves shop ID/number mappings.
        """
        super().__init__(*args, **kwargs)

        # Suppress schema mismatch warnings for verbose streams
        for stream_name in ("sales", "products"):
            logging.getLogger(f"tap-tilroy.{stream_name}").setLevel(logging.ERROR)

        # Resolve shop mappings if filters are configured
        self._resolve_shop_mappings()

    def _resolve_shop_mappings(self) -> None:
        """Fetch shops and resolve ID/number mappings.
        
        If filter_shop_ids or filter_shop_numbers is configured, fetches
        the shops list and populates both _resolved_shop_ids and 
        _resolved_shop_numbers for use by streams.
        """
        filter_ids = self.config.get("filter_shop_ids", [])
        filter_numbers = self.config.get("filter_shop_numbers", [])
        
        if not filter_ids and not filter_numbers:
            self.logger.info("No shop filters configured - streams will fetch all data")
            return
        
        # Fetch shops from API
        self.logger.info("Fetching shops to resolve ID/number mappings...")
        try:
            response = requests.get(
                f"{self.config['api_url']}/shopapi/production/shops",
                headers={
                    "Tilroy-Api-Key": self.config["tilroy_api_key"],
                    "x-api-key": self.config["x_api_key"],
                },
                timeout=30,
            )
            response.raise_for_status()
            shops = response.json()
        except Exception as e:
            self.logger.error(f"Failed to fetch shops for mapping: {e}")
            # Fall back to using provided values directly
            self._resolved_shop_ids = list(filter_ids)
            self._resolved_shop_numbers = list(filter_numbers)
            return
        
        # Build lookup dicts (convert to int for consistent comparison)
        id_to_number = {}
        number_to_id = {}
        for s in shops:
            try:
                tid = int(s.get("tilroyId", 0))
                num = int(s.get("number", 0))
                id_to_number[tid] = num
                number_to_id[num] = tid
            except (ValueError, TypeError):
                continue
        
        # Resolve from IDs
        if filter_ids:
            self._resolved_shop_ids = list(filter_ids)
            self._resolved_shop_numbers = [
                id_to_number[sid] for sid in filter_ids if sid in id_to_number
            ]
            self.logger.info(
                f"Resolved from shop IDs {filter_ids} -> numbers {self._resolved_shop_numbers}"
            )
        
        # Resolve from numbers
        elif filter_numbers:
            self._resolved_shop_numbers = list(filter_numbers)
            self._resolved_shop_ids = [
                number_to_id[num] for num in filter_numbers if num in number_to_id
            ]
            self.logger.info(
                f"Resolved from shop numbers {filter_numbers} -> IDs {self._resolved_shop_ids}"
            )

    def discover_streams(self) -> list[Stream]:
        """Return list of discovered streams.

        Returns:
            List of stream instances.
        """
        return [stream_class(self) for stream_class in STREAM_TYPES]

    def sync_all(self) -> None:
        """Sync all streams with dependency ordering.

        Ensures ProductsStream runs first to collect SKU IDs that
        StockStream depends on.
        """
        self._reset_state_progress_markers()
        self._set_compatible_replication_methods()

        # Get streams with dependencies
        products_stream = self.streams.get("products")
        stock_stream = self.streams.get("stock")
        prices_stream = self.streams.get("prices")

        # Build ordered list: products first, dependent streams last
        ordered_streams = self._build_stream_order(
            products_stream=products_stream,
            stock_stream=stock_stream,
            prices_stream=prices_stream,
        )

        # Clear SKU collection before starting
        if products_stream:
            products_stream.clear_collected_sku_ids()

        # Execute streams in order
        for stream in ordered_streams:
            self._sync_stream(stream, products_stream)

        # Log sync costs
        for stream in self.streams.values():
            stream.log_sync_costs()

    def _build_stream_order(
        self,
        products_stream: Stream | None,
        stock_stream: Stream | None,
        prices_stream: Stream | None,
    ) -> list[Stream]:
        """Build ordered list of streams respecting dependencies.

        Args:
            products_stream: The products stream (runs first).
            stock_stream: The stock stream (runs after products).
            prices_stream: The prices stream (runs after products).

        Returns:
            Ordered list of streams.
        """
        dependent_streams = {products_stream, stock_stream, prices_stream}
        other_streams = [
            s for s in self.streams.values() if s not in dependent_streams
        ]

        ordered = []

        # Products must run first
        if products_stream:
            ordered.append(products_stream)

        # Other streams can run in any order
        ordered.extend(other_streams)

        # Dependent streams run last
        if prices_stream:
            ordered.append(prices_stream)
        if stock_stream:
            ordered.append(stock_stream)

        return ordered

    def _sync_stream(
        self,
        stream: Stream,
        products_stream: Stream | None,
    ) -> None:
        """Sync a single stream with proper handling.

        Args:
            stream: The stream to sync.
            products_stream: The products stream for finalization.
        """
        # Skip deselected streams
        if not stream.selected and not stream.has_selected_descendents:
            self.logger.info(f"Skipping deselected stream '{stream.name}'")
            return

        # Skip child streams (they're invoked by parents)
        if stream.parent_stream_type:
            self.logger.debug(
                f"Skipping child stream '{stream.name}' (called by parent)"
            )
            return

        # Execute sync
        stream.sync()
        stream.finalize_state_progress_markers()

        # Finalize SKU collection after products stream
        if stream is products_stream and hasattr(products_stream, "finalize_child_contexts"):
            products_stream.finalize_child_contexts()


if __name__ == "__main__":
    TapTilroy.cli()
