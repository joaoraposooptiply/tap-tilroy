"""Tilroy tap class."""

from __future__ import annotations

import logging

from singer_sdk import Tap
from singer_sdk.singerlib import StateMessage
from singer_sdk import typing as th  # JSON schema typing helpers
from tap_tilroy.streams import (
    ShopsStream,
    ProductsStream,
    PurchaseOrdersStream,
    StockChangesStream,
    SalesStream,
    SuppliersStream,
    PricesStream
)

# TODO: Import your custom stream types here:
from tap_tilroy import streams

STREAM_TYPES = [
    ProductsStream,
    ShopsStream,
    PurchaseOrdersStream,
    StockChangesStream,
    SalesStream,
    SuppliersStream,
    PricesStream
]

class TapTilroy(Tap):
    """Tilroy tap class."""

    name = "tap-tilroy"

    def __init__(self, *args, **kwargs):
        """Initialize the tap and suppress schema warnings for specific streams."""
        super().__init__(*args, **kwargs)

        # The API returns many fields not defined in the schema, causing excessive
        # warnings. This silences warnings for the 'sales' and 'products' streams
        # by setting their logger level to ERROR.
        logging.getLogger("tap-tilroy.sales").setLevel(logging.ERROR)
        logging.getLogger("tap-tilroy.products").setLevel(logging.ERROR)

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "tilroy_api_key",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="The token to authenticate against the Tilroy API service",
        ),
        th.Property(
            "x_api_key",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="The AWS API key for authentication",
        ),
                th.Property(
                    "api_url",
                    th.StringType,
                    required=True,
                    description="The URL for the Tilroy API service",
                ),
                th.Property(
                    "prices_shop_number",
                    th.StringType,
                    required=True,
                    description="The shop number for the Tilroy API service",
                ),
    ).to_dict()

    def discover_streams(self) -> list[streams.TilroyStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [stream(self) for stream in STREAM_TYPES]
    
    def sync_all(self) -> None:
        """
        Sync all streams in a custom order, ensuring backward compatibility.

        This method overrides the default singer-sdk Tap.sync_all() to enforce a
        specific execution order: ProductsStream must run to completion before
        PricesStream begins.

        To maintain backward compatibility with the SDK, this implementation
        integrates the essential setup and teardown procedures from the base
        class, such as state management, progress markers, and cost logging.
        """
        # 1. Perform setup from the base class
        self._reset_state_progress_markers()
        self._set_compatible_replication_methods()
        if self.state:
            self.write_message(StateMessage(value=self.state))

        # 2. Define the custom execution order
        products_stream = self.streams.get("products")
        prices_stream = self.streams.get("prices")
        other_streams = [
            s
            for s in self.streams.values()
            if s not in [products_stream, prices_stream]
        ]

        ordered_streams = []
        if products_stream:
            ordered_streams.append(products_stream)
        ordered_streams.extend(other_streams)
        if prices_stream:
            ordered_streams.append(prices_stream)

        # 3. Execute streams in the custom order
        if products_stream:
            products_stream.clear_collected_sku_ids()

        for stream in ordered_streams:
            if not stream.selected and not stream.has_selected_descendents:
                self.logger.info("Skipping deselected stream '%s'.", stream.name)
                continue

            # For general backward compatibility, skip any streams that are SDK-style
            # child streams, since they will be invoked by their parents.
            if stream.parent_stream_type:
                self.logger.debug(
                    "Child stream '%s' is expected to be called by its parent. "
                    "Skipping direct invocation in custom sync_all.",
                    stream.name,
                )
                continue

            stream.sync()
            stream.finalize_state_progress_markers()

            if stream is products_stream:
                # Custom logic: finalize SKU collection after Products stream
                products_stream.finalize_child_contexts()

        # 4. Perform finalization from the base class
        # This final loop ensures all streams log their costs.
        for stream in self.streams.values():
            stream.log_sync_costs()


if __name__ == "__main__":
    TapTilroy.cli()
