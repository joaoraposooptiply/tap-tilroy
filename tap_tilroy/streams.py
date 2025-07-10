# All stream classes have been removed. Ready to start over.

import typing as t
from singer_sdk import typing as th  # JSON Schema typing helpers
from tap_tilroy.client import TilroyStream
import requests
from datetime import datetime, timedelta
from typing import Optional

class DateFilteredStream(TilroyStream):
    """Base class for streams that use date-based filtering."""
    
    def get_url_params(
        self,
        context: t.Optional[dict],
        next_page_token: t.Optional[t.Any] = None,
    ) -> dict[str, t.Any]:
        """Get URL query parameters.

        Args:
            context: Stream partition or context dictionary.
            next_page_token: Token for the next page of results.

        Returns:
            Dictionary of URL query parameters.
        """
        params = {}
        
        # Get the start date from the bookmark or use config
        start_date = self.get_starting_timestamp(context)
        if not start_date:
            # Get start date from config
            config_start_date = self.config["start_date"]
            # Extract just the date part from ISO format
            date_part = config_start_date.split('T')[0]
            start_date = datetime.strptime(date_part, "%Y-%m-%d")
        else:
            # If we have a bookmark, go back 1 day to ensure we don't miss any records
            # Convert to date only to avoid time component issues
            start_date = start_date.date()
            start_date = datetime.combine(start_date - timedelta(days=1), datetime.min.time())
        
        # Format the date as YYYY-MM-DD and ensure it's a string
        params["dateFrom"] = start_date.strftime("%Y-%m-%d")
        
        # Set page parameter for pagination
        if next_page_token:
            params["page"] = next_page_token
        else:
            params["page"] = 1
            
        # Set count parameter
        params["count"] = self.default_count
        
        return params

    def post_process(self, row: dict, context: t.Optional[dict] = None) -> dict:
        row = super().post_process(row, context)
        if not row:
            return None
        return row

class ShopsStream(DateFilteredStream):
    """Stream for Tilroy shops."""
    name = "shops"
    path = "/shopapi/production/shops"
    primary_keys: t.ClassVar[list[str]] = ["tilroyId"]
    replication_key = None
    records_jsonpath = "$[*]"
    next_page_token_jsonpath = None

    def post_process(self, row: dict, context: t.Optional[dict] = None) -> dict:
        """Post process the record."""
        row = super().post_process(row, context)
        if not row:
            return None
        return row

    schema = th.PropertiesList(
        th.Property("tilroyId", th.StringType),
        th.Property("sourceId", th.StringType, required=False),
        th.Property("number", th.StringType),
        th.Property("name", th.StringType),
        th.Property("type", th.ObjectType(
            th.Property("tilroyId", th.StringType),
            th.Property("code", th.StringType),
        )),
        th.Property("subType", th.ObjectType(
            th.Property("tilroyId", th.StringType),
            th.Property("code", th.StringType),
        )),
        th.Property("language", th.ObjectType(
            th.Property("tilroyId", th.StringType),
            th.Property("code", th.StringType),
        )),
        th.Property("latitude", th.StringType, required=False),
        th.Property("longitude", th.StringType, required=False),
        th.Property("postalCode", th.StringType, required=False),
        th.Property("street", th.StringType, required=False),
        th.Property("houseNumber", th.StringType, required=False),
        th.Property("legalEntityId", th.IntegerType),
        th.Property("country", th.ObjectType(
            th.Property("tilroyId", th.StringType),
            th.Property("countryCode", th.StringType),
        )),
    ).to_dict()

class ProductsStream(DateFilteredStream):
    """Stream for Tilroy products."""
    name = "products"
    path = "/product-bulk/production/products"
    primary_keys: t.ClassVar[list[str]] = ["tilroyId"]
    replication_key = None
    records_jsonpath = "$[*]"
    next_page_token_jsonpath = None
    default_count = 1000  # Override default count to 1000 for products

    def post_process(self, row: dict, context: t.Optional[dict] = None) -> dict:
        """Post process the record."""
        row = super().post_process(row, context)
        if not row:
            return None
        return row

    schema = th.PropertiesList(
        th.Property("tilroyId", th.StringType),
        th.Property("sourceId", th.StringType, required=False),
        th.Property("code", th.StringType),
        th.Property(
            "descriptions",
            th.ArrayType(
                th.ObjectType(
                    th.Property("languageCode", th.StringType),
                    th.Property("standard", th.StringType),
                )
            ),
        ),
        th.Property("brand", th.ObjectType(
            th.Property("code", th.StringType),
            th.Property(
                "descriptions",
                th.ArrayType(
                    th.ObjectType(
                        th.Property("languageCode", th.StringType, required=False),
                        th.Property("standard", th.StringType, required=False),
                    ),
                ),
            ),
        )),
        th.Property(
            "colours",
            th.ArrayType(
                th.ObjectType(
                    th.Property("tilroyId", th.StringType),
                    th.Property("sourceId", th.StringType, required=False),
                    th.Property("code", th.StringType),
                    th.Property(
                        "skus",
                        th.ArrayType(
                            th.ObjectType(
                                th.Property("tilroyId", th.StringType),
                                th.Property("sourceId", th.StringType, required=False),
                                th.Property("costPrice", th.NumberType),
                                th.Property(
                                    "barcodes",
                                    th.ArrayType(
                                        th.ObjectType(
                                            th.Property("code", th.StringType),
                                            th.Property("quantity", th.IntegerType),
                                            th.Property("isInternal", th.BooleanType),
                                        )
                                    ),
                                ),
                                th.Property(
                                    "size",
                                    th.ObjectType(
                                        th.Property("code", th.StringType),
                                    ),
                                ),
                                th.Property(
                                    "lifeStatus",
                                    th.ObjectType(
                                        th.Property("code", th.StringType),
                                    ),
                                ),
                                th.Property(
                                    "rrp",
                                    th.ArrayType(th.ObjectType()),
                                ),
                            )
                        ),
                    ),
                    th.Property(
                        "pictures",
                        th.ArrayType(th.ObjectType()),
                    ),
                )
            ),
        ),
        th.Property("isUsed", th.BooleanType),
    ).to_dict()

class PurchaseOrdersStream(DateFilteredStream):
    """Purchase Orders stream."""

    name = "purchase_orders"
    path = "/purchase-order-bulk/production/purchase-orders"
    primary_keys = ["tilroyId"]
    replication_key = "orderDate"
    replication_method = "INCREMENTAL"
    default_count = 500

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        """Post-process record."""
        if not row:
            return None

        # Handle error responses
        if "code" in row and "message" in row:
            self.logger.warning(f"Skipping record with error: {row['message']}")
            return None

        # Ensure orderDate exists and is valid
        if "orderDate" not in row or not row["orderDate"]:
            self.logger.warning(f"Skipping record without orderDate: {row}")
            return None

        # Convert orderDate to datetime if it's a string
        if isinstance(row["orderDate"], str):
            try:
                row["orderDate"] = datetime.fromisoformat(row["orderDate"].replace("Z", "+00:00"))
            except ValueError:
                self.logger.warning(f"Could not parse orderDate: {row['orderDate']}")
                return None

        return row

    schema = th.PropertiesList(
        th.Property("tilroyId", th.StringType),
        th.Property("number", th.StringType),
        th.Property("orderDate", th.DateTimeType),
        th.Property("supplier", th.ObjectType(
            th.Property("tilroyId", th.IntegerType),
            th.Property("code", th.StringType),
            th.Property("name", th.StringType),
        )),
        th.Property("supplierReference", th.StringType),
        th.Property("requestedDeliveryDate", th.StringType),
        th.Property("warehouse", th.ObjectType(
            th.Property("number", th.IntegerType),
            th.Property("name", th.StringType),
        )),
        th.Property("currency", th.ObjectType(
            th.Property("code", th.StringType),
        )),
        th.Property("prices", th.ObjectType(
            th.Property("tenantCurrency", th.ObjectType(
                th.Property("standardVatExc", th.NumberType),
                th.Property("standardVatInc", th.NumberType),
                th.Property("vatExc", th.NumberType),
                th.Property("vatInc", th.NumberType),
            )),
            th.Property("supplierCurrency", th.ObjectType(
                th.Property("standardVatExc", th.NumberType),
                th.Property("standardVatInc", th.NumberType),
                th.Property("vatExc", th.NumberType),
                th.Property("vatInc", th.NumberType),
            )),
        )),
        th.Property("status", th.StringType),
        th.Property("created", th.ObjectType(
            th.Property("user", th.ObjectType(
                th.Property("login", th.StringType),
                th.Property("sourceId", th.StringType),
            )),
            th.Property("timestamp", th.StringType),
        )),
        th.Property("modified", th.ObjectType(
            th.Property("user", th.ObjectType(
                th.Property("login", th.StringType),
                th.Property("sourceId", th.StringType),
            )),
            th.Property("timestamp", th.StringType),
        )),
        th.Property("lines", th.ArrayType(
            th.ObjectType(
                th.Property("sku", th.ObjectType(
                    th.Property("tilroyId", th.StringType),
                    th.Property("sourceId", th.StringType),
                )),
                th.Property("warehouse", th.ObjectType(
                    th.Property("number", th.IntegerType),
                    th.Property("name", th.StringType),
                )),
                th.Property("created", th.ObjectType(
                    th.Property("user", th.ObjectType(
                        th.Property("login", th.StringType),
                        th.Property("sourceId", th.StringType),
                    )),
                )),
                th.Property("modified", th.ObjectType(
                    th.Property("user", th.ObjectType(
                        th.Property("login", th.StringType),
                        th.Property("sourceId", th.StringType),
                    )),
                )),
                th.Property("status", th.StringType),
                th.Property("requestedDeliveryDate", th.StringType),
                th.Property("qty", th.ObjectType(
                    th.Property("ordered", th.IntegerType),
                    th.Property("delivered", th.IntegerType),
                    th.Property("backOrder", th.IntegerType),
                    th.Property("cancelled", th.IntegerType),
                )),
                th.Property("prices", th.ObjectType(
                    th.Property("tenantCurrency", th.ObjectType(
                        th.Property("vatExc", th.NumberType),
                        th.Property("vatInc", th.NumberType),
                        th.Property("unitVatExc", th.NumberType),
                        th.Property("unitVatInc", th.NumberType),
                        th.Property("standardUnitVatExc", th.NumberType),
                        th.Property("standardVatExc", th.NumberType),
                        th.Property("standardVatInc", th.NumberType),
                        th.Property("standardUnitVatInc", th.NumberType),
                    )),
                    th.Property("supplierCurrency", th.ObjectType(
                        th.Property("vatExc", th.NumberType),
                        th.Property("vatInc", th.NumberType),
                        th.Property("unitVatExc", th.NumberType),
                        th.Property("unitVatInc", th.NumberType),
                        th.Property("standardUnitVatExc", th.NumberType),
                        th.Property("standardVatExc", th.NumberType),
                        th.Property("standardVatInc", th.NumberType),
                        th.Property("standardUnitVatInc", th.NumberType),
                    )),
                )),
                th.Property("discount", th.ObjectType(
                    th.Property("amount", th.NumberType),
                    th.Property("percentage", th.NumberType),
                    th.Property("total", th.NumberType),
                    th.Property("newStandardPrice", th.NumberType),
                )),
                th.Property("id", th.StringType),
            )
        )),
    ).to_dict()

class StockChangesStream(DateFilteredStream):
    """Stream for Tilroy stock changes."""
    name = "stock_changes"
    path = "/stockapi/production/export/stockdeltas"
    primary_keys: t.ClassVar[list[str]] = ["tilroyId"]
    replication_key = "timestamp"
    replication_method = "INCREMENTAL"
    records_jsonpath = "$[*]"
    next_page_token_jsonpath = None
    default_count = 500  # Match SalesStream's default count

    def post_process(self, row: dict, context: t.Optional[dict] = None) -> dict:
        """Post process the record."""
        row = super().post_process(row, context)
        if not row:
            return None
        # Add timestamp if not present
        if "timestamp" not in row:
            row["timestamp"] = datetime.utcnow().isoformat()
        return row

    schema = th.PropertiesList(
        th.Property("tilroyId", th.StringType),
        th.Property("timestamp", th.DateTimeType),
        th.Property("sourceId", th.StringType),
        th.Property("reason", th.StringType),
        th.Property("shop", th.ObjectType(
            th.Property("number", th.IntegerType),
            th.Property("sourceId", th.StringType, required=False),
        )),
        th.Property("product", th.ObjectType(
            th.Property("code", th.StringType),
            th.Property("sourceId", th.StringType),
        )),
        th.Property("colour", th.ObjectType(
            th.Property("code", th.StringType),
            th.Property("sourceId", th.StringType),
        )),
        th.Property("size", th.ObjectType(
            th.Property("code", th.StringType),
        )),
        th.Property("sku", th.ObjectType(
            th.Property("barcode", th.StringType),
            th.Property("sourceId", th.StringType),
        )),
        th.Property("qtyDelta", th.IntegerType),
        th.Property("qtyTransferredDelta", th.IntegerType),
        th.Property("qtyReservedDelta", th.IntegerType),
        th.Property("qtyRequestedDelta", th.IntegerType),
        th.Property("cause", th.StringType, required=False),
    ).to_dict()

class SalesStream(DateFilteredStream):
    """Stream for Tilroy sales."""
    name = "sales"
    path = "/saleapi/production/export/sales"
    primary_keys: t.ClassVar[list[str]] = ["idTilroySale"]
    replication_key = "saleDate"
    records_jsonpath = "$[*]"
    next_page_token_jsonpath = None
    default_count = 500  # Default count per page

    def post_process(self, row: dict, context: t.Optional[dict] = None) -> dict:
        """Post process the record."""
        row = super().post_process(row, context)
        if not row:
            return None
        return row

    schema = th.PropertiesList(
        th.Property("idTilroySale", th.StringType),
        th.Property("idTenant", th.StringType),
        th.Property("idSession", th.StringType),
        th.Property("customer", th.ObjectType(
            th.Property("idTilroy", th.StringType, required=False),
            th.Property("idSource", th.StringType, required=False),
        )),
        th.Property("idSourceCustomer", th.StringType, required=False),
        th.Property("vatTypeCalculation", th.ObjectType(
            th.Property("UseCalculation", th.BooleanType),
            th.Property("IdVatType", th.StringType),
            th.Property("VatTypeCode", th.StringType),
            th.Property("VatExempt", th.BooleanType),
            th.Property("IsVatIncl", th.BooleanType),
            th.Property("IsIntraComm", th.BooleanType),
            th.Property("IsExport", th.BooleanType),
            th.Property("IsCustom", th.BooleanType),
            th.Property("IdCountryFrom", th.IntegerType),
            th.Property("CountryFromIsIntrastat", th.BooleanType),
            th.Property("IdCountryTo", th.IntegerType),
            th.Property("CountryToIsIntrastat", th.BooleanType),
            th.Property("Invoice", th.BooleanType),
            th.Property("VatNumber", th.StringType),
            th.Property("IdCustomer", th.StringType),
        )),
        th.Property("shop", th.ObjectType(
            th.Property("idTilroy", th.StringType),
            th.Property("idSource", th.StringType, required=False),
            th.Property("number", th.IntegerType),
            th.Property("name", th.StringType),
            th.Property("country", th.StringType),
        )),
        th.Property("till", th.ObjectType(
            th.Property("idTilroy", th.StringType),
            th.Property("number", th.IntegerType),
            th.Property("idSource", th.StringType, required=False),
        )),
        th.Property("saleDate", th.DateTimeType),
        th.Property("eTicket", th.BooleanType),
        th.Property("orderDate", th.StringType, required=False),
        th.Property("totalAmountStandard", th.NumberType),
        th.Property("totalAmountSell", th.NumberType),
        th.Property("totalAmountDiscount", th.NumberType),
        th.Property("totalAmountSellRounded", th.NumberType),
        th.Property("totalAmountSellRoundedPart", th.NumberType),
        th.Property("totalAmountSellNotRoundedPart", th.NumberType),
        th.Property("totalAmountOutstanding", th.NumberType),
        th.Property(
            "lines",
            th.ArrayType(
                th.ObjectType(
                    th.Property("idTilroySaleLine", th.StringType),
                    th.Property("type", th.StringType),
                    th.Property(
                        "sku",
                        th.ObjectType(
                            th.Property("idTilroy", th.StringType),
                            th.Property("idSource", th.StringType),
                        ),
                    ),
                    th.Property("description", th.StringType),
                    th.Property("quantity", th.IntegerType),
                    th.Property("quantityReturned", th.IntegerType),
                    th.Property("quantityNet", th.IntegerType),
                    th.Property("costPrice", th.NumberType),
                    th.Property("sellPrice", th.NumberType),
                    th.Property("standardPrice", th.NumberType),
                    th.Property("promoPrice", th.NumberType),
                    th.Property("rrp", th.NumberType),
                    th.Property("retailPrice", th.NumberType),
                    th.Property("discount", th.NumberType),
                    th.Property("discountType", th.IntegerType),
                    th.Property("lineTotalCost", th.NumberType),
                    th.Property("lineTotalStandard", th.NumberType),
                    th.Property("lineTotalSell", th.NumberType),
                    th.Property("lineTotalDiscount", th.NumberType),
                    th.Property("lineTotalVatExcl", th.NumberType),
                    th.Property("lineTotalVat", th.NumberType),
                    th.Property("vatPercentage", th.NumberType),
                    th.Property("code", th.StringType),
                    th.Property("comments", th.StringType, required=False),
                    th.Property("serialNumberSale", th.StringType, required=False),
                    th.Property("webDescription", th.StringType),
                    th.Property("colour", th.StringType),
                    th.Property("size", th.StringType),
                    th.Property("ean", th.StringType),
                    th.Property("timestamp", th.StringType),
                ),
            ),
        ),
        th.Property("totalAmountPaid", th.NumberType),
        th.Property(
            "payments",
            th.ArrayType(
                th.ObjectType(
                    th.Property("idTilroySalePayment", th.StringType),
                    th.Property(
                        "paymentType",
                        th.ObjectType(
                            th.Property("idTilroy", th.StringType),
                            th.Property("code", th.StringType),
                            th.Property("idSource", th.StringType, required=False),
                            th.Property(
                                "descriptions",
                                th.ArrayType(
                                    th.ObjectType(
                                        th.Property("description", th.StringType),
                                        th.Property("languageCode", th.StringType),
                                    ),
                                ),
                            ),
                            th.Property("reporting", th.BooleanType),
                        ),
                    ),
                    th.Property("amount", th.NumberType),
                    th.Property("paymentReference", th.StringType),
                    th.Property("timestamp", th.StringType),
                    th.Property("isPaid", th.BooleanType),
                ),
            ),
        ),
        th.Property(
            "vat",
            th.ArrayType(
                th.ObjectType(
                    th.Property("idTilroy", th.StringType),
                    th.Property("vatPercentage", th.NumberType),
                    th.Property("vatKind", th.StringType),
                    th.Property("amountNet", th.NumberType),
                    th.Property("amountLines", th.NumberType),
                    th.Property("amountTaxable", th.NumberType),
                    th.Property("amountVat", th.NumberType),
                    th.Property("vatAmount", th.NumberType),
                    th.Property("totalAmount", th.NumberType),
                    th.Property("timestamp", th.StringType),
                ),
            ),
        ),
        th.Property("legalEntity", th.ObjectType(
            th.Property("idTilroy", th.StringType),
            th.Property("code", th.StringType),
            th.Property("name", th.StringType),
            th.Property("vatNr", th.StringType),
        )),
    ).to_dict()
