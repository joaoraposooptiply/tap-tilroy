"""Tilroy tap stream modules."""

from tap_tilroy.streams.shops import ShopsStream
from tap_tilroy.streams.products import ProductsStream, SuppliersStream
from tap_tilroy.streams.sales import SalesStream
from tap_tilroy.streams.stock import StockStream, StockChangesStream
from tap_tilroy.streams.prices import PricesStream
from tap_tilroy.streams.purchase import PurchaseOrdersStream

__all__ = [
    "ShopsStream",
    "ProductsStream",
    "SuppliersStream",
    "SalesStream",
    "StockStream",
    "StockChangesStream",
    "PricesStream",
    "PurchaseOrdersStream",
]
