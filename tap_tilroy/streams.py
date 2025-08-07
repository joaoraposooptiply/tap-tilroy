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

class DynamicRoutingStream(DateFilteredStream):
    """Base class for streams that can switch between historical and incremental endpoints.
    
    The path is dynamically selected at request time based on whether the stream has existing state:
    - No state (first run): Uses historical_path for full sync
    - Has state (subsequent runs): Uses incremental_path for delta sync
    """
    
    # These should be defined in subclasses
    historical_path: str = None
    incremental_path: str = None
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Don't set path here - make it dynamic via property
    
    @property
    def path(self) -> str:
        """Dynamic path property that evaluates at request time."""
        return self._get_dynamic_path()
    
    def _get_dynamic_path(self) -> str:
        """Determine which path to use based on whether we have existing state."""
        is_incremental = self._is_incremental_sync()
        selected_path = self.incremental_path if is_incremental else self.historical_path
        
        # Debug state availability
        state = self.tap_state or {}
        if "value" in state:
            # Singer state format: {"value": {"bookmarks": {...}}}
            bookmarks = state.get("value", {}).get("bookmarks", {})
        else:
            # Direct format: {"bookmarks": {...}}
            bookmarks = state.get("bookmarks", {})
        
        stream_state = bookmarks.get(self.name, {})
        
        self.logger.info(f"🔀 [{self.name}] Dynamic routing decision (evaluated at request time):")
        self.logger.info(f"   • Stream has replication_key: {bool(self.replication_key)}")
        self.logger.info(f"   • Tap state available: {bool(self.tap_state)}")
        self.logger.info(f"   • Stream state: {stream_state}")
        self.logger.info(f"   • Has existing state: {is_incremental}")
        self.logger.info(f"   • Selected sync type: {'INCREMENTAL' if is_incremental else 'HISTORICAL'}")
        self.logger.info(f"   • Selected path: {selected_path}")
        
        return selected_path
    
    def _is_incremental_sync(self) -> bool:
        """Check if this is an incremental sync (has existing state)."""
        if not self.replication_key:
            return False
        
        # Check if we have any existing state for this stream
        # Handle both direct state format and Singer state format with "value" wrapper
        state = self.tap_state or {}
        if "value" in state:
            # Singer state format: {"value": {"bookmarks": {...}}}
            bookmarks = state.get("value", {}).get("bookmarks", {})
        else:
            # Direct format: {"bookmarks": {...}}
            bookmarks = state.get("bookmarks", {})
        
        stream_state = bookmarks.get(self.name, {})
        has_state = bool(stream_state.get("replication_key_value"))
        
        if has_state:
            self.logger.info(f"📊 [{self.name}] Found existing bookmark: {stream_state.get('replication_key_value')}")
        
        return has_state
    
    def get_url_params(
        self,
        context: t.Optional[dict],
        next_page_token: t.Optional[t.Any] = None,
    ) -> dict[str, t.Any]:
        """Get URL query parameters with dynamic parameter naming."""
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
        
        # Use different parameter names based on sync type
        if self._is_incremental_sync():
            # Incremental sync uses dateFrom
            params["dateFrom"] = start_date.strftime("%Y-%m-%d")
            self.logger.info(f"📈 [{self.name}] Using incremental parameters: dateFrom={start_date.strftime('%Y-%m-%d')}")
        else:
            # Historical sync uses specific parameter names per stream
            historical_params = self._get_historical_date_params(start_date)
            params.update(historical_params)
            self.logger.info(f"📜 [{self.name}] Using historical parameters: {historical_params}")
        
        # Set page parameter for pagination
        if next_page_token:
            params["page"] = next_page_token
        else:
            params["page"] = 1
            
        # Set count parameter
        params["count"] = self.default_count
        
        self.logger.info(f"🔧 [{self.name}] Final URL params: {params}")
        return params
    
    def _get_historical_date_params(self, start_date: datetime) -> dict:
        """Get date parameters for historical sync. Override in subclasses if needed."""
        return {"dateFrom": start_date.strftime("%Y-%m-%d")}

class ShopsStream(DateFilteredStream):
    """Stream for Tilroy shops."""
    name = "shops"
    path = "/shopapi/production/shops"
    primary_keys: t.ClassVar[list[str]] = ["tilroyId"]
    replication_key = None
    records_jsonpath = "$[*]"
    next_page_token_jsonpath = None

    schema = th.PropertiesList(
        th.Property("tilroyId", th.StringType),
        th.Property("sourceId", th.StringType, required=False),
        th.Property("number", th.StringType),
        th.Property("name", th.StringType),
        th.Property("type", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("subType", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("language", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("latitude", th.StringType, required=False),
        th.Property("longitude", th.StringType, required=False),
        th.Property("postalCode", th.StringType, required=False),
        th.Property("street", th.StringType, required=False),
        th.Property("houseNumber", th.StringType, required=False),
        th.Property("legalEntityId", th.IntegerType),
        th.Property("country", th.CustomType({"type": ["object", "string", "null"]})),
    ).to_dict()

class ProductsStream(DynamicRoutingStream):
    """Stream for Tilroy products."""
    name = "products"
    historical_path = "/product-bulk/production/products"
    incremental_path = "/product-bulk/production/export/products"
    primary_keys: t.ClassVar[list[str]] = ["tilroyId"]
    replication_key = "extraction_timestamp"  # Synthetic replication key
    replication_method = "INCREMENTAL"
    records_jsonpath = "$[*]"
    next_page_token_jsonpath = None
    default_count = 1000  # Override default count to 1000 for products

    def post_process(self, row: dict, context: t.Optional[dict] = None) -> dict:
        """Post process the record and add synthetic timestamp."""
        if not row:
            return None
        
        # Add synthetic timestamp for incremental tracking
        # This allows us to use the export endpoint for delta syncs
        row["extraction_timestamp"] = datetime.utcnow().isoformat()
        
        return row

    schema = th.PropertiesList(
        th.Property("tilroyId", th.CustomType({"type": ["string", "integer"]})),
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
        th.Property("supplier", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("brand", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property(
            "colours",
            th.ArrayType(
                th.ObjectType(
                    th.Property("tilroyId", th.CustomType({"type": ["string", "integer"]})),
                    th.Property("sourceId", th.StringType, required=False),
                    th.Property("code", th.StringType),
                    th.Property(
                        "skus",
                        th.ArrayType(
                            th.ObjectType(
                                th.Property("tilroyId", th.CustomType({"type": ["string", "integer"]})),
                                th.Property("sourceId", th.StringType, required=False),
                                th.Property("costPrice", th.NumberType),
                                th.Property(
                                    "barcodes",
                                    th.ArrayType(
                                        th.ObjectType(
                                            th.Property("code", th.StringType),
                                            th.Property("quantity", th.NumberType),
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
                                
                               th.Property("rrp", th.CustomType({"type": ["array","object", "string", "null"]})),
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
        th.Property("extraction_timestamp", th.DateTimeType),  # Synthetic replication key
    ).to_dict()

class SuppliersStream(TilroyStream):
    """Stream for Tilroy suppliers."""
    name = "suppliers"
    path = "/product-bulk/production/suppliers"
    primary_keys = ["tilroyId"]
    replication_key = None
    replication_method = "FULL_TABLE"
    records_jsonpath = "$[*]"
    default_count = 10000

    schema = th.PropertiesList(
        th.Property("tilroyId", th.IntegerType),
        th.Property("code", th.StringType),
        th.Property("name", th.StringType),
    ).to_dict()

class PurchaseOrdersStream(DynamicRoutingStream):
    """Purchase Orders stream."""

    name = "purchase_orders"
    historical_path = "/purchaseapi/production/purchaseorders"
    incremental_path = "/purchaseapi/production/export/orders"
    primary_keys = ["tilroyId"]
    replication_key = "orderDate"
    replication_method = "INCREMENTAL"
    default_count = 500

    def _get_historical_date_params(self, start_date: datetime) -> dict:
        """Get date parameters for historical purchase orders sync (uses orderDateFrom)."""
        return {"orderDateFrom": start_date.strftime("%Y-%m-%d")}

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
    th.Property("tilroyId", th.CustomType({"type": ["string", "integer"]})),
    th.Property("number", th.StringType),
    th.Property("orderDate", th.DateTimeType),
    th.Property("supplier", th.CustomType({"type": ["object", "string", "null"]})),
    th.Property("supplierReference", th.StringType),
    th.Property("requestedDeliveryDate", th.StringType),
    th.Property("warehouse", th.CustomType({"type": ["object", "string", "null"]})),
    th.Property("currency", th.CustomType({"type": ["object", "string", "null"]})),
    th.Property("prices", th.CustomType({"type": ["object", "string", "null"]})),
    th.Property("status", th.StringType),
    th.Property("created", th.CustomType({"type": ["object", "string", "null"]})),
    th.Property("modified", th.CustomType({"type": ["object", "string", "null"]})),
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
                th.Property("requestedDeliveryDate", th.DateTimeType),
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
    path = "/stockapi/production/stockchanges"
    # primary_keys: t.ClassVar[list[str]] = ["tilroyId"]
    primary_keys = ["tilroyId"]
    replication_key = "dateUpdated"
    replication_method = "INCREMENTAL"
    records_jsonpath = "$[*]"
    next_page_token_jsonpath = None
    default_count = 100  # Match SalesStream's default count

    def get_url_params(
        self,
        context: t.Optional[dict],
        next_page_token: t.Optional[t.Any] = None,
    ) -> dict[str, t.Any]:
        """Get URL query parameters for stock changes with dateFrom and dateTo."""
        params = super().get_url_params(context, next_page_token)
        
        # Add dateTo parameter set to current date
        current_date = datetime.now().strftime("%Y-%m-%d")
        params["dateTo"] = current_date
        
        self.logger.info(f"📅 [{self.name}] Added dateTo parameter: {current_date}")
        
        return params



    schema = th.PropertiesList(
        th.Property("tilroyId", th.StringType),
        th.Property("timestamp", th.DateTimeType),
        th.Property("sourceId", th.StringType),
        th.Property("reason", th.StringType),
        th.Property("shop", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("product", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("qty",th.CustomType({"type": ["object", "string", "null"]})),
        # th.Property("colour", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("size", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("refill",th.IntegerType),
        th.Property("sku", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("qtyDelta", th.IntegerType),
        th.Property("qtyTransferredDelta", th.IntegerType),
        th.Property("qtyReservedDelta", th.IntegerType),
        th.Property("qtyRequestedDelta", th.IntegerType),
        th.Property("cause", th.StringType, required=False),
        th.Property("dateCreated", th.DateTimeType),
        th.Property("dateUpdated", th.DateTimeType),
    ).to_dict()

class SalesStream(DateFilteredStream):
    """Stream for Tilroy sales."""
    name = "sales"
    path = "/saleapi/production/export/sales"
    primary_keys: t.ClassVar[list[str]] = ["idTilroySale"]
    replication_key = "saleDate"
    replication_method = "INCREMENTAL"
    records_jsonpath = "$[*]"
    next_page_token_jsonpath = None
    default_count = 500  # Default count per page

    schema = th.PropertiesList(
        th.Property("idTilroySale", th.StringType),
        th.Property("idTenant", th.StringType),
        th.Property("idSession", th.StringType),
        th.Property("customer", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("idSourceCustomer", th.StringType, required=False),
        th.Property("vatTypeCalculation", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("shop", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("till", th.CustomType({"type": ["object", "string", "null"]})),
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
        th.Property("legalEntity", th.CustomType({"type": ["object", "string", "null"]})),
    ).to_dict()
