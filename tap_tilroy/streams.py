# All stream classes have been removed. Ready to start over.

import typing as t
from singer_sdk import typing as th  # JSON Schema typing helpers
from tap_tilroy.client import TilroyStream
import requests
from datetime import datetime, timedelta
from typing import Optional
import json
import http.client
from urllib.parse import urlencode

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
        # Check development page limit
        dev_max_pages = self.config.get("dev_max_pages")
        if dev_max_pages and next_page_token and next_page_token > dev_max_pages:
            self.logger.info(f"🛑 [{self.name}] Development page limit reached ({dev_max_pages})")
            return {}
        
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
        
        # Add development logging
        if dev_max_pages:
            current_page = next_page_token if next_page_token else 1
            self.logger.info(f"🔧 [{self.name}] Dev mode: page {current_page}/{dev_max_pages}")
        
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
    
    # Class variable to store SKU IDs for prices stream
    _collected_sku_ids: list[str] = []
    _child_contexts: list[dict] = []  # Store child contexts for batching
    _batch_size = 100  # API limit for prices

    def post_process(self, row: dict, context: t.Optional[dict] = None) -> dict:
        """Post process the record and add synthetic timestamp."""
        if not row:
            return None
        
        # Add synthetic timestamp for incremental tracking
        # This allows us to use the export endpoint for delta syncs
        row["extraction_timestamp"] = datetime.utcnow().isoformat()
        
        # Collect SKU IDs for prices stream
        self._collect_sku_ids_from_product(row)
        
        # Create child context for this record
        child_context = self.get_child_context(row, context)
        if child_context:
            self._child_contexts.append(child_context)
            
            # If we have 100 contexts, flush them
            if len(self._child_contexts) >= self._batch_size:
                self._flush_child_contexts()
        
        return row
    
    def get_child_context(self, record: dict, context: t.Optional[dict] = None) -> dict:
        """Return a child context object for a given record."""
        # Extract SKU IDs from this product record
        sku_ids = []
        if "colours" in record and isinstance(record["colours"], list):
            for colour in record["colours"]:
                if "skus" in colour and isinstance(colour["skus"], list):
                    for sku in colour["skus"]:
                        if "tilroyId" in sku:
                            sku_ids.append(str(sku["tilroyId"]))
        
        if sku_ids:
            return {
                "sku_ids": sku_ids,
                "product_id": record.get("tilroyId"),
                "context": context
            }
        return None
    
    def _flush_child_contexts(self) -> None:
        """Flush collected child contexts to PricesStream."""
        if not self._child_contexts:
            return
        
        # Combine all SKU IDs from contexts
        all_sku_ids = []
        for context in self._child_contexts:
            all_sku_ids.extend(context["sku_ids"])
        
        # Remove duplicates
        unique_sku_ids = list(set(all_sku_ids))
        
        # Add to global collection
        for sku_id in unique_sku_ids:
            if sku_id not in self._collected_sku_ids:
                self._collected_sku_ids.append(sku_id)
        
        self.logger.info(f"📦 [{self.name}] Flushed {len(self._child_contexts)} contexts, collected {len(unique_sku_ids)} unique SKU IDs")
        
        # Clear contexts
        self._child_contexts = []
    
    def finalize_child_contexts(self) -> None:
        """Finalize any remaining child contexts."""
        if self._child_contexts:
            self._flush_child_contexts()
        
        self.logger.info(f"✅ [{self.name}] Final SKU collection: {len(self._collected_sku_ids)} unique SKU IDs collected for PricesStream")
    
    def _collect_sku_ids_from_product(self, product: dict) -> None:
        """Extract SKU IDs from a product record and store them."""
        if "colours" in product and isinstance(product["colours"], list):
            for colour in product["colours"]:
                if "skus" in colour and isinstance(colour["skus"], list):
                    for sku in colour["skus"]:
                        if "tilroyId" in sku:
                            sku_id = str(sku["tilroyId"])
                            if sku_id not in self._collected_sku_ids:
                                self._collected_sku_ids.append(sku_id)
                                # Log every 100 SKU IDs collected for monitoring
                                if len(self._collected_sku_ids) % 100 == 0:
                                    self.logger.info(f"📦 [{self.name}] Collected {len(self._collected_sku_ids)} SKU IDs so far...")
    
    @classmethod
    def get_collected_sku_ids(cls) -> list[str]:
        """Get the collected SKU IDs for use by other streams."""
        return cls._collected_sku_ids.copy()
    
    @classmethod
    def clear_collected_sku_ids(cls) -> None:
        """Clear the collected SKU IDs (useful for testing or reset)."""
        cls._collected_sku_ids.clear()
    
    def finalize_sku_collection(self) -> None:
        """Log final SKU collection statistics."""
        self.logger.info(f"✅ [{self.name}] Final SKU collection: {len(self._collected_sku_ids)} unique SKU IDs collected for PricesStream")
    

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
        th.Property("suppliers", th.CustomType({"type": ["array","object", "string", "null"]})),
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

    def post_process(self, row: dict, context: t.Optional[dict] = None) -> t.Optional[dict]:
        """
        Post-process each record to handle potential API errors and ensure
        the replication key is present.
        """
        # The API can return error objects instead of valid records.
        # We need to identify and skip these.
        if "message" in row and "code" in row:
            self.logger.warning(
                f"Skipping record due to an API error: {row['message']}"
            )
            return None

        # The replication key 'saleDate' is essential for incremental sync.
        # If it is missing, the record is invalid and must be skipped.
        if self.replication_key not in row or not row[self.replication_key]:
            self.logger.warning(f"Skipping record without a '{self.replication_key}': {row}")
            return None

        return row

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


class PricesStream(TilroyStream):
    """Stream for Tilroy prices that collects SKU IDs from products and batches API calls."""
    
    name = "prices"
    path = "/priceapi/production/prices"
    # parent_stream_type = ProductsStream  # Declare parent stream
    primary_keys: t.ClassVar[list[str]] = ["sku_tilroy_id", "price_tilroy_id", "type"]
    replication_key = "date_created"
    replication_method = "INCREMENTAL"
    records_jsonpath = "$[*]"
    next_page_token_jsonpath = None
    
    # Store collected SKU IDs
    _sku_ids: list[str] = []
    _batch_size = 100  # API limit
    _current_start_idx = 0
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._sku_ids = []
        self._current_start_idx = 0
        self.logger.info(f"🔧 [{self.name}] PricesStream initialized")
    
    def _collect_sku_ids_from_products_stream(self) -> None:
        """Get SKU IDs that were collected by the ProductsStream."""
        self.logger.info(f"🔄 [{self.name}] Getting SKU IDs from ProductsStream...")
        
        try:
            # Get SKU IDs collected by ProductsStream
            self._sku_ids = ProductsStream.get_collected_sku_ids()
            
            if not self._sku_ids:
                self.logger.warning(f"⚠️ [{self.name}] No SKU IDs found from ProductsStream. Make sure ProductsStream runs before PricesStream.")
            else:
                self.logger.info(f"📦 [{self.name}] Retrieved {len(self._sku_ids)} SKU IDs from ProductsStream")
            
        except Exception as e:
            self.logger.error(f"❌ [{self.name}] Error getting SKU IDs from ProductsStream: {str(e)}")
            self._sku_ids = []
    
    def get_url_params(self, context: t.Optional[dict], next_page_token: t.Optional[t.Any] = None) -> dict:
        """Get URL parameters with SKU IDs for the current chunk."""
        self.logger.info(f"🔧 [{self.name}] get_url_params called")
        
        # If we don't have SKU IDs yet, try to collect them
        if not self._sku_ids:
            self.logger.info(f"📥 [{self.name}] No SKU IDs, collecting from ProductsStream...")
            self._collect_sku_ids_from_products_stream()
        
        # If still no SKU IDs, return empty params to skip this stream
        if not self._sku_ids:
            self.logger.warning(f"⚠️ [{self.name}] No SKU IDs collected, skipping prices stream")
            return {}
        
        # Get current chunk of SKU IDs (max 100)
        start_idx = getattr(self, '_current_start_idx', 0)
        end_idx = min(start_idx + self._batch_size, len(self._sku_ids))
        chunk_sku_ids = self._sku_ids[start_idx:end_idx]
        
        if not chunk_sku_ids:
            self.logger.info(f"🏁 [{self.name}] No more SKU IDs to process")
            return {}
        
        # Build parameters
        sku_ids_param = ",".join(chunk_sku_ids)
        params = {
            "skuIds": sku_ids_param,
            "shopNumber": str(self.config.get("prices_shop_number")),
            "priceRange": "retail"
        }
        
        self.logger.info(f"🔗 [{self.name}] Processing {len(chunk_sku_ids)} SKU IDs (indices {start_idx}-{end_idx-1})")
        self.logger.info(f"🌐 [{self.name}] URL params: {params}")
        
        # Update start index for next call
        self._current_start_idx = end_idx
        
        return params

    def request_records(self, context: t.Optional[dict]) -> t.Iterator[dict]:
        """Override to handle price flattening properly."""
        # Use parent's request_records but handle flattening
        for record in super().request_records(context):
            if "prices" in record and isinstance(record["prices"], list):
                # Flatten each price into a separate record
                for price in record["prices"]:
                    flattened_record = self._flatten_price_record(record["sku"], price)
                    if flattened_record:
                        yield flattened_record
            else:
                # Skip records without prices array
                continue
    
    def _flatten_price_record(self, sku: dict, price: dict) -> dict:
        """Flatten a price record to create a single record per price type."""
        if not sku or not price:
            return None
        
        record = {
            "sku_tilroy_id": sku.get("tilroyId"),
            "sku_source_id": sku.get("sourceId"),
            "price_tilroy_id": str(price.get("priceTilroyId", "")),
            "price_source_id": price.get("priceSourceId"),
            "price": price.get("price"),
            "type": price.get("type"),
            "quantity": price.get("quantity", 1),
            "run_tilroy_id": price.get("run", {}).get("tilroyId") if price.get("run") else None,
            "run_source_id": price.get("run", {}).get("sourceId") if price.get("run") else None,
            "label_type": price.get("labelType"),
            "end_date": price.get("endDate"),
            "start_date": price.get("startDate"),
            "date_created": price.get("dateCreated"),
        }
        
        # Ensure date_created is not None (required for replication key)
        if not record["date_created"]:
            record["date_created"] = datetime.utcnow().isoformat()
        
        return record
    


    schema = th.PropertiesList(
        th.Property("sku_tilroy_id", th.StringType),
        th.Property("sku_source_id", th.StringType, required=False),
        th.Property("price_tilroy_id", th.StringType),
        th.Property("price_source_id", th.StringType, required=False),
        th.Property("price", th.NumberType),
        th.Property("type", th.StringType),  # "standard" or "promo"
        th.Property("quantity", th.IntegerType),
        th.Property("run_tilroy_id", th.StringType, required=False),
        th.Property("run_source_id", th.StringType, required=False),
        th.Property("label_type", th.StringType, required=False),
        th.Property("end_date", th.DateTimeType, required=False),
        th.Property("start_date", th.DateTimeType, required=False),
        th.Property("date_created", th.DateTimeType),
    ).to_dict()


class StockStream(TilroyStream):
    """Stream for Tilroy stock that collects SKU IDs from products and batches API calls."""
    
    name = "stock"
    path = "/stockapi/production/stock"
    primary_keys: t.ClassVar[list[str]] = ["tilroyId"]
    replication_key = "dateUpdated"
    replication_method = "INCREMENTAL"
    records_jsonpath = "$[*]"
    next_page_token_jsonpath = None
    
    # Store collected SKU IDs
    _sku_ids: list[str] = []
    _batch_size = 50  # Reduced batch size to avoid URL length/header overflow errors
    _current_start_idx = 0
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._sku_ids = []
        self._current_start_idx = 0
        self.logger.info(f"🔧 [{self.name}] StockStream initialized")
    
    def _collect_sku_ids_from_products_stream(self) -> None:
        """Get SKU IDs that were collected by the ProductsStream."""
        self.logger.info(f"🔄 [{self.name}] Getting SKU IDs from ProductsStream...")
        
        try:
            # Get SKU IDs collected by ProductsStream
            self._sku_ids = ProductsStream.get_collected_sku_ids()
            
            if not self._sku_ids:
                self.logger.warning(f"⚠️ [{self.name}] No SKU IDs found from ProductsStream. Make sure ProductsStream runs before StockStream.")
            else:
                self.logger.info(f"📦 [{self.name}] Retrieved {len(self._sku_ids)} SKU IDs from ProductsStream")
            
        except Exception as e:
            self.logger.error(f"❌ [{self.name}] Error getting SKU IDs from ProductsStream: {str(e)}")
            self._sku_ids = []
    
    def get_url_params(self, context: t.Optional[dict], next_page_token: t.Optional[t.Any] = None) -> dict:
        """Get URL parameters with SKU IDs for the current chunk."""
        self.logger.info(f"🔧 [{self.name}] get_url_params called")
        
        # If we don't have SKU IDs yet, try to collect them
        if not self._sku_ids:
            self.logger.info(f"📥 [{self.name}] No SKU IDs, collecting from ProductsStream...")
            self._collect_sku_ids_from_products_stream()
        
        # If still no SKU IDs, return empty params to skip this stream
        if not self._sku_ids:
            self.logger.warning(f"⚠️ [{self.name}] No SKU IDs collected, skipping stock stream")
            return {}
        
        # Get current chunk of SKU IDs (max batch_size)
        start_idx = getattr(self, '_current_start_idx', 0)
        end_idx = min(start_idx + self._batch_size, len(self._sku_ids))
        chunk_sku_ids = self._sku_ids[start_idx:end_idx]
        
        if not chunk_sku_ids:
            self.logger.info(f"🏁 [{self.name}] No more SKU IDs to process")
            return {}
        
        # Build parameters - use skuTilroyId parameter as shown in the endpoint
        sku_ids_param = ",".join(chunk_sku_ids)
        params = {
            "skuTilroyId": sku_ids_param
        }
        
        self.logger.info(f"🔗 [{self.name}] Processing {len(chunk_sku_ids)} SKU IDs (indices {start_idx}-{end_idx-1})")
        self.logger.info(f"🌐 [{self.name}] Complete Request URL: {self.url_base}{self.path}?skuTilroyId={sku_ids_param[:100]}...")
        self.logger.info(f"📋 [{self.name}] Request params: {params}")
        
        # Update start index for next call
        self._current_start_idx = end_idx
        
        return params
    
    def request_records(self, context: t.Optional[dict]) -> t.Iterator[dict]:
        """Override to handle batching of SKU IDs across multiple requests."""
        from singer_sdk.helpers.jsonpath import extract_jsonpath
        import json
        import http.client
        
        # Collect SKU IDs if not already done
        if not self._sku_ids:
            self.logger.info(f"📥 [{self.name}] No SKU IDs, collecting from ProductsStream...")
            self._collect_sku_ids_from_products_stream()
        
        # If no SKU IDs, skip this stream
        if not self._sku_ids:
            self.logger.warning(f"⚠️ [{self.name}] No SKU IDs collected, skipping stock stream")
            return
        
        # Reset start index for this sync
        self._current_start_idx = 0
        
        # Process SKU IDs in batches
        total_sku_ids = len(self._sku_ids)
        self.logger.info(f"🚀 [{self.name}] Starting to process {total_sku_ids} SKU IDs in batches of {self._batch_size}")
        
        while self._current_start_idx < total_sku_ids:
            # Get current batch
            start_idx = self._current_start_idx
            end_idx = min(start_idx + self._batch_size, total_sku_ids)
            chunk_sku_ids = self._sku_ids[start_idx:end_idx]
            
            if not chunk_sku_ids:
                break
            
            # Build URL with SKU IDs
            sku_ids_param = ",".join(chunk_sku_ids)
            url = f"{self.url_base}{self.path}?skuTilroyId={sku_ids_param}"
            headers = self.get_headers(context)
            
            self.logger.info(f"🔗 [{self.name}] Processing batch {start_idx//self._batch_size + 1}: {len(chunk_sku_ids)} SKU IDs (indices {start_idx}-{end_idx-1})")
            self.logger.info(f"🌐 [{self.name}] Complete Request URL: {url[:200]}...")  # Log first 200 chars
            
            try:
                # Make the request
                conn = http.client.HTTPSConnection("api.tilroy.com")
                query_string = f"skuTilroyId={sku_ids_param}"
                conn.request("GET", f"{self.path}?{query_string}", "", headers)
                response = conn.getresponse()
                data = response.read().decode("utf-8")
                
                # Check for errors
                if response.status != 200:
                    self.logger.error(f"❌ [{self.name}] API returned status {response.status}: {data[:500]}")
                    # Continue to next batch instead of failing completely
                    self._current_start_idx = end_idx
                    continue
                
                # Parse the response
                data_json = json.loads(data)
                
                # Check if the response is an error object
                if isinstance(data_json, dict) and "code" in data_json and "message" in data_json:
                    self.logger.error(f"❌ [{self.name}] API returned error: {data_json.get('message', 'Unknown error')}")
                    # Continue to next batch instead of failing completely
                    self._current_start_idx = end_idx
                    continue
                
                # Extract records using jsonpath
                records = list(extract_jsonpath(self.records_jsonpath, data_json))
                self.logger.info(f"🔍 [{self.name}] Found {len(records)} records in this batch")
                
                # Yield each record
                for record in records:
                    # Post-process the record
                    processed_record = self.post_process(record, context)
                    if processed_record:
                        yield processed_record
                
                # Move to next batch
                self._current_start_idx = end_idx
                
            except Exception as e:
                self.logger.error(f"❌ [{self.name}] Error fetching batch (indices {start_idx}-{end_idx-1}): {str(e)}")
                # Continue to next batch instead of failing completely
                self._current_start_idx = end_idx
                continue
        
        self.logger.info(f"✅ [{self.name}] Finished processing all {total_sku_ids} SKU IDs")
    
    def post_process(self, row: dict, context: t.Optional[dict] = None) -> t.Optional[dict]:
        """Post-process each stock record."""
        if not row:
            return None
        
        # Handle error responses from the API
        if "code" in row and "message" in row:
            self.logger.warning(f"⚠️ [{self.name}] Skipping API error response: {row.get('message', 'Unknown error')}")
            return None
        
        # Ensure dateUpdated exists and is valid for replication key
        if "dateUpdated" not in row or not row["dateUpdated"]:
            self.logger.warning(f"⚠️ [{self.name}] Skipping record without dateUpdated: {row}")
            return None
        
        # Convert dateUpdated to datetime if it's a string
        if isinstance(row["dateUpdated"], str):
            try:
                row["dateUpdated"] = datetime.fromisoformat(row["dateUpdated"].replace("Z", "+00:00"))
            except ValueError:
                self.logger.warning(f"⚠️ [{self.name}] Could not parse dateUpdated: {row['dateUpdated']}")
                return None
        
        return row

    schema = th.PropertiesList(
        th.Property("tilroyId", th.StringType),
        th.Property(
            "sku",
            th.ObjectType(
                th.Property("tilroyId", th.StringType),
                th.Property("sourceId", th.StringType),
            )
        ),
        th.Property("location1", th.StringType, required=False),
        th.Property("location2", th.StringType, required=False),
        th.Property(
            "qty",
            th.ObjectType(
                th.Property("available", th.IntegerType),
                th.Property("ideal", th.IntegerType, required=False),
                th.Property("max", th.IntegerType, required=False),
                th.Property("requested", th.IntegerType),
                th.Property("transfered", th.IntegerType),
            )
        ),
        th.Property("refill", th.IntegerType),
        th.Property("dateUpdated", th.DateTimeType),
        th.Property(
            "shop",
            th.ObjectType(
                th.Property("tilroyId", th.StringType),
                th.Property("number", th.IntegerType),
            )
        ),
    ).to_dict()
