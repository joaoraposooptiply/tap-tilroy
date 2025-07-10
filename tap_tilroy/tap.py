"""Tilroy tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers
from tap_tilroy.streams import (
    ShopsStream,
    ProductsStream,
    PurchaseOrdersStream,
    StockChangesStream,
    SalesStream,
)

# TODO: Import your custom stream types here:
from tap_tilroy import streams

STREAM_TYPES = [
    ProductsStream,
    ShopsStream,
    PurchaseOrdersStream,
    StockChangesStream,
    SalesStream,
]

class TapTilroy(Tap):
    """Tilroy tap class."""

    name = "tap-tilroy"

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
    ).to_dict()

    def discover_streams(self) -> list[streams.TilroyStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            ShopsStream(self),
            ProductsStream(self),
            PurchaseOrdersStream(self),
            StockChangesStream(self),
            SalesStream(self),
        ]


if __name__ == "__main__":
    TapTilroy.cli()
