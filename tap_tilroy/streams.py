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
        th.Property("tilroyId", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("sourceId", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("number", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("name", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("type", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("subType", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("language", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("latitude", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("longitude", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("postalCode", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("street", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("houseNumber", th.CustomType({"type": ["string", "number", "null"]}), required=False),
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
        th.Property("sourceId", th.CustomType({"type": ["string", "number", "null"]}), required=False),
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
                    th.Property("sourceId", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("code", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property(
                        "skus",
                        th.ArrayType(
                            th.ObjectType(
                                th.Property("tilroyId", th.CustomType({"type": ["string", "integer"]})),
                                th.Property("sourceId", th.CustomType({"type": ["string", "number", "null"]}), required=False),
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
        th.Property("code", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("name", th.CustomType({"type": ["string", "number", "null"]})),
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
    th.Property("lines", th.ArrayType(
            th.ObjectType(
                th.Property("sku", th.ObjectType(
                    th.Property("tilroyId", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property("sourceId", th.CustomType({"type": ["string", "number", "null"]})),
                )),
                th.Property("warehouse", th.ObjectType(
                    th.Property("number", th.IntegerType),
                    th.Property("name", th.CustomType({"type": ["string", "number", "null"]})),
                )),
                th.Property("created", th.ObjectType(
                    th.Property("user", th.ObjectType(
                        th.Property("login", th.CustomType({"type": ["string", "number", "null"]})),
                        th.Property("sourceId", th.CustomType({"type": ["string", "number", "null"]})),
                    )),
                )),
                th.Property("modified", th.ObjectType(
                    th.Property("user", th.ObjectType(
                        th.Property("login", th.CustomType({"type": ["string", "number", "null"]})),
                        th.Property("sourceId", th.CustomType({"type": ["string", "number", "null"]})),
                    )),
                )),
                th.Property("status", th.CustomType({"type": ["string", "number", "null"]})),
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
                th.Property("id", th.CustomType({"type": ["string", "number", "null"]})),
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
        th.Property("tilroyId", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("timestamp", th.DateTimeType),
        th.Property("sourceId", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("reason", th.CustomType({"type": ["string", "number", "null"]})),
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
        th.Property("cause", th.CustomType({"type": ["string", "number", "null"]}), required=False),
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
        th.Property("idTilroySale", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("idTenant", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("idSession", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("customer", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("idSourceCustomer", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("vatTypeCalculation", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("shop", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("till", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("saleDate", th.DateTimeType),
        th.Property("eTicket", th.BooleanType),
        th.Property("orderDate", th.CustomType({"type": ["string", "number", "null"]}), required=False),
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
                    th.Property("idTilroySaleLine", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property("type", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property(
                        "sku",
                        th.ObjectType(
                            th.Property("idTilroy", th.CustomType({"type": ["string", "number", "null"]})),
                            th.Property("idSource", th.CustomType({"type": ["string", "number", "null"]})),
                        ),
                    ),
                    th.Property("description", th.CustomType({"type": ["string", "number", "null"]})),
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
                    th.Property("code", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property("comments", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("serialNumberSale", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("webDescription", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property("colour", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property("size", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property("ean", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property("timestamp", th.CustomType({"type": ["string", "number", "null"]})),
                ),
            ),
        ),
        th.Property("totalAmountPaid", th.NumberType),
        th.Property(
            "payments",
            th.ArrayType(
                th.ObjectType(
                    th.Property("idTilroySalePayment", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property(
                        "paymentType",
                        th.ObjectType(
                            th.Property("idTilroy", th.CustomType({"type": ["string", "number", "null"]})),
                            th.Property("code", th.CustomType({"type": ["string", "number", "null"]})),
                            th.Property("idSource", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                            th.Property(
                                "descriptions",
                                th.ArrayType(
                                    th.ObjectType(
                                        th.Property("description", th.CustomType({"type": ["string", "number", "null"]})),
                                        th.Property("languageCode", th.CustomType({"type": ["string", "number", "null"]})),
                                    ),
                                ),
                            ),
                            th.Property("reporting", th.BooleanType),
                        ),
                    ),
                    th.Property("amount", th.NumberType),
                    th.Property("paymentReference", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property("timestamp", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property("isPaid", th.BooleanType),
                ),
            ),
        ),
        th.Property(
            "vat",
            th.ArrayType(
                th.ObjectType(
                    th.Property("idTilroy", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property("vatPercentage", th.NumberType),
                    th.Property("vatKind", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property("amountNet", th.NumberType),
                    th.Property("amountLines", th.NumberType),
                    th.Property("amountTaxable", th.NumberType),
                    th.Property("amountVat", th.NumberType),
                    th.Property("vatAmount", th.NumberType),
                    th.Property("totalAmount", th.NumberType),
                    th.Property("timestamp", th.CustomType({"type": ["string", "number", "null"]})),
                ),
            ),
        ),
        th.Property("legalEntity", th.CustomType({"type": ["object", "string", "null"]})),
    ).to_dict()


class SalesProductionStream(DateFilteredStream):
    """Stream for Tilroy sales using the production sales endpoint with dateFrom and lastId pagination."""
    name = "sales_production"
    path = "/saleapi/production/sales"
    primary_keys: t.ClassVar[list[str]] = ["idTilroySale"]
    replication_key = "saleDate"
    replication_method = "INCREMENTAL"
    records_jsonpath = "$[*]"
    next_page_token_jsonpath = None
    default_count = 100  # As shown in curl example

    def get_url_params(
        self,
        context: t.Optional[dict],
        next_page_token: t.Optional[t.Any] = None,
    ) -> dict[str, t.Any]:
        """Get URL query parameters with dateFrom and lastId-based pagination.
        
        Args:
            context: Stream partition or context dictionary.
            next_page_token: The lastId from the previous response (if any).
            
        Returns:
            Dictionary of URL query parameters.
        """
        # Get dateFrom from the bookmark or use config (from DateFilteredStream)
        params = super().get_url_params(context, next_page_token)
        
        # Remove the 'page' parameter that DateFilteredStream adds (we use lastId instead)
        params.pop("page", None)
        
        # Add lastId if we have one (from previous response for pagination within same date)
        if next_page_token:
            params["lastId"] = next_page_token
            self.logger.info(f"📄 [{self.name}] Using lastId for pagination: {next_page_token}")
        else:
            self.logger.info(f"📄 [{self.name}] Starting pagination without lastId")
        
        return params

    def request_records(self, context: t.Optional[dict]) -> t.Iterator[dict]:
        """Request records using dateFrom and lastId-based pagination.
        
        Args:
            context: Stream partition or context dictionary.
            
        Yields:
            Records from the stream.
        """
        from singer_sdk.helpers.jsonpath import extract_jsonpath
        import json
        import http.client
        
        last_id = None  # Start without lastId for the current dateFrom
        
        while True:
            # Build URL parameters (includes dateFrom from DateFilteredStream)
            params = self.get_url_params(context, last_id)
            url = f"{self.url_base}{self.path}"
            headers = self.get_headers(context)
            
            # Build query string
            query_string = "&".join([f"{k}={v}" for k, v in params.items()])
            complete_url = f"{url}?{query_string}"
            
            self.logger.info(f"🌐 [{self.name}] Complete Request URL: {complete_url}")
            self.logger.info(f"📋 [{self.name}] Request params: {params}")
            
            try:
                # Make the request
                conn = http.client.HTTPSConnection("api.tilroy.com")
                conn.request("GET", f"{self.path}?{query_string}", "", headers)
                response = conn.getresponse()
                data = response.read().decode("utf-8")
                
                # Check for errors
                if response.status != 200:
                    self.logger.error(f"❌ [{self.name}] API returned status {response.status}: {data[:500]}")
                    break
                
                # Parse the response
                data_json = json.loads(data)
                
                # Check if the response is an error object
                if isinstance(data_json, dict) and "code" in data_json and "message" in data_json:
                    self.logger.error(f"❌ [{self.name}] API returned error: {data_json.get('message', 'Unknown error')}")
                    break
                
                # Extract records using jsonpath
                records = list(extract_jsonpath(self.records_jsonpath, data_json))
                self.logger.info(f"🔍 [{self.name}] Found {len(records)} records")
                
                if not records:
                    # No records returned, we've reached the end for this date
                    self.logger.info(f"🏁 [{self.name}] No more records, pagination complete")
                    break
                
                # Yield each record
                for record in records:
                    processed_record = self.post_process(record, context)
                    if processed_record:
                        yield processed_record
                
                # Extract lastId from the last record for next iteration (pagination within same dateFrom)
                # The lastId should be the idTilroySale from the last record
                if records:
                    last_record = records[-1]
                    if isinstance(last_record, dict) and "idTilroySale" in last_record:
                        last_id = str(last_record["idTilroySale"])
                        self.logger.info(f"📌 [{self.name}] Extracted lastId from last record: {last_id}")
                    else:
                        # If we can't find idTilroySale, we can't continue pagination
                        self.logger.warning(f"⚠️ [{self.name}] Could not find idTilroySale in last record. Record keys: {list(last_record.keys()) if isinstance(last_record, dict) else 'N/A'}")
                        # If we got fewer records than count, we're done
                        if len(records) < self.default_count:
                            break
                        # Otherwise, we can't continue pagination
                        self.logger.error(f"❌ [{self.name}] Cannot continue pagination without lastId")
                        break
                else:
                    # No records, we're done
                    break
                    
                # If we got fewer records than count, we've reached the end for this dateFrom
                if len(records) < self.default_count:
                    self.logger.info(f"🏁 [{self.name}] Received fewer records than count ({len(records)} < {self.default_count}), pagination complete for this date")
                    break
                    
            except Exception as e:
                self.logger.error(f"❌ [{self.name}] Error fetching records: {str(e)}")
                raise

    def post_process(self, row: dict, context: t.Optional[dict] = None) -> t.Optional[dict]:
        """
        Post-process each record to handle potential API errors, ensure
        the replication key is present, and convert Decimals to appropriate types.
        Arrays are preserved as arrays (not flattened).
        """
        if not row:
            return None
            
        # The API can return error objects instead of valid records.
        if "message" in row and "code" in row:
            self.logger.warning(
                f"[{self.name}] Skipping record due to an API error: {row['message']}"
            )
            return None

        # The replication key 'saleDate' is essential for incremental sync.
        # If it is missing, the record is invalid and must be skipped.
        if self.replication_key not in row or not row[self.replication_key]:
            self.logger.warning(f"[{self.name}] Skipping record without a '{self.replication_key}': {row}")
            return None

        # Convert ALL Decimal values and string numbers to appropriate types (strings for IDs, numbers for numeric fields)
        # This prevents Decimals and string numbers from causing validation errors
        # Arrays are preserved as arrays - they are NOT flattened
        from decimal import Decimal
        
        # Define numeric fields that should be converted from strings to numbers
        # These field names can appear at any nesting level
        numeric_fields = {
            "totalAmountStandard", "totalAmountSell", "totalAmountDiscount", 
            "totalAmountSellRounded", "totalAmountSellRoundedPart", 
            "totalAmountSellNotRoundedPart", "totalAmountOutstanding",
            "costPrice", "sellPrice", "standardPrice", "promoPrice", "rrp", 
            "retailPrice", "discount", "lineTotalCost", "lineTotalStandard", 
            "lineTotalSell", "lineTotalDiscount", "lineTotalVatExcl", 
            "lineTotalVat", "maxDiscount", "depositValue", "vatPercentage",
            "discountTransaction", "priceTransactionDiscount", "priceLineDiscount"
        }
        
        def is_numeric_string(s):
            """Check if a string represents a number."""
            if not isinstance(s, str):
                return False
            try:
                float(s)
                return True
            except (ValueError, TypeError):
                return False
        
        def convert_decimals_recursive(obj):
            """Recursively convert Decimal values and string numbers, preserving array structure."""
            if isinstance(obj, dict):
                result = {}
                for key, val in obj.items():
                    if isinstance(val, Decimal):
                        # ID fields should be strings
                        if key in ["idTilroySale", "idTenant", "idSession", "idSourceCustomer", 
                                  "idTilroySaleLine", "idTilroySalePayment", "idTilroy", "idSource"]:
                            if val == val.to_integral_value():
                                result[key] = str(int(val))
                            else:
                                result[key] = str(val)
                        # String fields that might be Decimals
                        elif key in ["code", "ean", "paymentReference", "advanceReference", 
                                    "comments", "description", "webDescription", "colour", "size",
                                    "serialNumberSale"]:
                            if val == val.to_integral_value():
                                result[key] = str(int(val))
                            else:
                                result[key] = str(val)
                        else:
                            # Numeric fields - convert to float
                            result[key] = float(val)
                    elif isinstance(val, str) and is_numeric_string(val) and key in numeric_fields:
                        # Convert string numbers to float for numeric fields (works at any nesting level)
                        result[key] = float(val)
                    elif isinstance(val, list):
                        # Preserve arrays - recursively process items but keep as list
                        result[key] = [convert_decimals_recursive(item) for item in val]
                    elif isinstance(val, dict):
                        result[key] = convert_decimals_recursive(val)
                    else:
                        result[key] = val
                return result
            elif isinstance(obj, list):
                # Preserve array structure
                return [convert_decimals_recursive(item) for item in obj]
            elif isinstance(obj, Decimal):
                # Standalone Decimal - convert to float
                return float(obj)
            elif isinstance(obj, str) and is_numeric_string(obj):
                # Standalone numeric string - convert to float
                return float(obj)
            else:
                return obj
        
        # Process the row, preserving arrays
        processed = convert_decimals_recursive(row)
        
        # Ensure top-level ID fields are strings
        if isinstance(processed, dict):
            id_fields = ["idTilroySale", "idTenant", "idSession", "idSourceCustomer"]
            for field in id_fields:
                if field in processed and processed[field] is not None:
                    processed[field] = str(processed[field])
        
        return processed

    schema = th.PropertiesList(
        th.Property("idTilroySale", th.CustomType({"type": ["string", "integer"]})),
        th.Property("idTenant", th.CustomType({"type": ["string", "integer"]})),
        th.Property("idSession", th.CustomType({"type": ["string", "integer", "null"]})),
        th.Property("customer", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("idSourceCustomer", th.CustomType({"type": ["string", "null"]}), required=False),
        th.Property("vatTypeCalculation", th.CustomType({"type": ["object", "string", "null"]}), required=False),
        th.Property("shop", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("till", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("saleDate", th.DateTimeType),
        th.Property("eTicket", th.BooleanType),
        th.Property("orderDate", th.CustomType({"type": ["string", "null"]}), required=False),
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
                    th.Property("idTilroySaleLine", th.CustomType({"type": ["string", "integer"]})),
                    th.Property("type", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property("sku", th.ObjectType()),
                    th.Property("description", th.CustomType({"type": ["string", "number", "null"]})),
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
                    th.Property("maxDiscount", th.NumberType, required=False),
                    th.Property("transformedToProduct", th.BooleanType, required=False),
                    th.Property("transformedIdPaymentType", th.CustomType({"type": ["string", "integer", "null"]}), required=False),
                    th.Property("idTransactionLine", th.IntegerType, required=False),
                    th.Property("orderId", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("orderLineId", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("depositValue", th.NumberType, required=False),
                    th.Property("vatPercentage", th.NumberType),
                    th.Property("discountTransaction", th.NumberType, required=False),
                    th.Property("discountReason", th.ObjectType(), required=False),
                    th.Property("priceTransactionDiscount", th.NumberType, required=False),
                    th.Property("priceLineDiscount", th.NumberType, required=False),
                    th.Property("returnReason", th.ObjectType(), required=False),
                    th.Property("code", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property("comments", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("serialNumberSale", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("serialNumberSaleActivator", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("promotionName", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("idSaleLineReturned", th.CustomType({"type": ["string", "integer", "null"]}), required=False),
                    th.Property("idRepair", th.IntegerType, required=False),
                    th.Property("idTillBasketLineOrig", th.CustomType({"type": ["string", "integer", "null"]}), required=False),
                    th.Property("orderDateOriginal", th.CustomType({"type": ["string", "null"]}), required=False),
                    th.Property("collectMethodCodeOriginal", th.CustomType({"type": ["string", "null"]}), required=False),
                    th.Property("orderNumberOriginal", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("idTillBasketLine", th.IntegerType, required=False),
                    th.Property("webDescription", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property("colour", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property("size", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property("ppPriceBundle", th.NumberType, required=False),
                    th.Property("ppPriceBuyAndGet", th.NumberType, required=False),
                    th.Property("ppPriceExceptional", th.NumberType, required=False),
                    th.Property("ppPriceLineDiscount", th.NumberType, required=False),
                    th.Property("ppPriceManual", th.NumberType, required=False),
                    th.Property("ppPricePromo", th.NumberType, required=False),
                    th.Property("ppPriceSellGross", th.NumberType, required=False),
                    th.Property("ppPriceSet", th.NumberType, required=False),
                    th.Property("ppBundleApplied", th.BooleanType, required=False),
                    th.Property("ppBundleDealID", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("ppBundleLineParent", th.BooleanType, required=False),
                    th.Property("ppActionID", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("ppBuyAndGetID", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("ppCopyFrom", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("ppDiscountReasonID", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("ppExceptionalPriceID", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("ppInBuying", th.BooleanType, required=False),
                    th.Property("ppLineDiscountID", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("ppReturnIncentiveID", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("ppSetID", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("ppTriggerID", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("basedOnSale", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("basedOnSaleLine", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("collectMethod", th.ObjectType(), required=False),
                    th.Property("generateVoucher", th.BooleanType, required=False),
                    th.Property("isAdvance", th.BooleanType, required=False),
                    th.Property("standardUnitPrice", th.NumberType, required=False),
                    th.Property("order", th.ObjectType(), required=False),
                    th.Property("collectShop", th.ObjectType(), required=False),
                    th.Property("idRental", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("reservationReference", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("reservation", th.BooleanType, required=False),
                    th.Property("configuratorType", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("configuratorCode", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("linkedObjectBarcode", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("linkedObjectDisplayValue", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("linkedObjectReference", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("linkedObjectReferenceType", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("netWeight", th.NumberType, required=False),
                    th.Property("useInNextSaleDiscountCalculations", th.BooleanType, required=False),
                    th.Property("generateActivationCode", th.BooleanType, required=False),
                    th.Property("standardPriceOrig", th.NumberType, required=False),
                    th.Property("stockIsReserved", th.BooleanType, required=False),
                    th.Property("lineNumber", th.IntegerType, required=False),
                    th.Property("timestamp", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property("picture", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("quantityPack", th.IntegerType, required=False),
                    th.Property("intrastatCode", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("ean", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property("isMasterLine", th.BooleanType, required=False),
                    th.Property("amountOpen", th.NumberType, required=False),
                    th.Property("combinedProductId", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("combinedProductCode", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("idPaymentRequest", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("idInvoicePayment", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("isUsedProduct", th.BooleanType, required=False),
                    th.Property("usedProductBarcode", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("isBuyBack", th.BooleanType, required=False),
                    th.Property("unknownPrice", th.BooleanType, required=False),
                    th.Property("certificateOnSale", th.BooleanType, required=False),
                    th.Property("isDiscountedBarcode", th.BooleanType, required=False),
                    th.Property("idAssetType", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("idAsset", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("idLeasing", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("insurances", th.ArrayType(th.ObjectType()), required=False),
                    th.Property("idDispatchMethod", th.IntegerType, required=False),
                    th.Property("dispatchMethodCode", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("dispatchMethodExtraData", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("shipment", th.ObjectType(), required=False),
                    th.Property("deliveryPromise", th.CustomType({"type": ["string", "null"]}), required=False),
                    th.Property("warrantyDate", th.CustomType({"type": ["string", "null"]}), required=False),
                    th.Property("warrantyDays", th.IntegerType, required=False),
                    th.Property("deliveryDate", th.CustomType({"type": ["string", "null"]}), required=False),
                    th.Property("idUserSalesPerson", th.IntegerType, required=False),
                    th.Property("userSalesPerson", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("cappedStock", th.BooleanType, required=False),
                    th.Property("icons", th.ArrayType(th.ObjectType()), required=False),
                    th.Property("registrationAtSupplierPossible", th.BooleanType, required=False),
                    th.Property("isPaymentExternalOrder", th.BooleanType, required=False),
                    th.Property("hasVatOverride", th.BooleanType, required=False),
                    th.Property("IsReturnOrderLine", th.BooleanType, required=False),
                    th.Property("isReturnOrderAdvance", th.BooleanType, required=False),
                    th.Property("manualPrice", th.CustomType({"type": ["number", "null"]}), required=False),
                    th.Property("taxes", th.ArrayType(th.ObjectType()), required=False),
                    th.Property("salesOrigin", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("idSourceDiscountReason", th.CustomType({"type": ["string", "integer", "null"]}), required=False),
                    th.Property("idSourceReturnReason", th.CustomType({"type": ["string", "integer", "null"]}), required=False),
                    th.Property("idSourceSku", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("wac", th.ObjectType(), required=False),
                    th.Property("_id", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                ),
            ),
        ),
        th.Property("totalAmountPaid", th.NumberType),
        th.Property(
            "payments",
            th.ArrayType(
                th.ObjectType(
                    th.Property("VatRatio", th.NumberType, required=False),
                    th.Property("idTilroySalePayment", th.CustomType({"type": ["string", "integer"]})),
                    th.Property("paymentType", th.ObjectType()),
                    th.Property("amount", th.NumberType),
                    th.Property("paymentReference", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property("paymentReferenceNegativeAdvance", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("paymentReferenceReturnOrderAdvance", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("czamReference", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("czamTicket", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("czamSignatureRequired", th.BooleanType, required=False),
                    th.Property("identificationRequired", th.BooleanType, required=False),
                    th.Property("merchantInfo", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("idRepair", th.IntegerType, required=False),
                    th.Property("idTillBasketLine", th.IntegerType, required=False),
                    th.Property("idTilroySaleLine", th.CustomType({"type": ["string", "integer", "null"]}), required=False),
                    th.Property("advanceSource", th.ObjectType(), required=False),
                    th.Property("advanceReference", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("idCollectMethod", th.IntegerType, required=False),
                    th.Property("idSkuTransform", th.CustomType({"type": ["string", "integer", "null"]}), required=False),
                    th.Property("amountOpen", th.NumberType, required=False),
                    th.Property("idRental", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("isRembours", th.BooleanType, required=False),
                    th.Property("linkedObjectBarcode", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("linkedObjectDisplayValue", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("linkedObjectReference", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("linkedObjectReferenceType", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("timestamp", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property("isRounding", th.BooleanType, required=False),
                    th.Property("idPaymentRequest", th.CustomType({"type": ["string", "number", "null"]}), required=False),
                    th.Property("vatPercentage", th.NumberType, required=False),
                    th.Property("printOnTicket", th.BooleanType, required=False),
                    th.Property("advanceAmountNegative", th.NumberType, required=False),
                    th.Property("advanceAmountReturnOrder", th.NumberType, required=False),
                    th.Property("isAdvanceCheckout", th.BooleanType, required=False),
                    th.Property("isReservation", th.BooleanType, required=False),
                    th.Property("isPaymentExternalOrder", th.BooleanType, required=False),
                    th.Property("isReturnOrderAdvance", th.BooleanType, required=False),
                    th.Property("isPaid", th.BooleanType),
                    th.Property("idSourcePaymentType", th.CustomType({"type": ["string", "integer", "null"]}), required=False),
                ),
            ),
        ),
        th.Property("vouchers", th.ArrayType(th.ObjectType()), required=False),
        th.Property(
            "vat",
            th.ArrayType(
                th.ObjectType(
                    th.Property("idTilroy", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property("vatPercentage", th.NumberType),
                    th.Property("vatKind", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property("amountNet", th.NumberType),
                    th.Property("amountLines", th.NumberType),
                    th.Property("amountTaxable", th.NumberType),
                    th.Property("amountVat", th.NumberType),
                    th.Property("vatAmount", th.NumberType),
                    th.Property("idVat", th.IntegerType, required=False),
                    th.Property("totalAmount", th.NumberType),
                    th.Property("cashDiscount", th.NumberType, required=False),
                    th.Property("amountExVat", th.NumberType, required=False),
                    th.Property("timestamp", th.CustomType({"type": ["string", "number", "null"]})),
                    th.Property("descriptions", th.ArrayType(th.ObjectType()), required=False),
                ),
            ),
        ),
        th.Property("activations", th.ArrayType(th.ObjectType()), required=False),
        th.Property("credits", th.ArrayType(th.ObjectType()), required=False),
        th.Property("saleCredits", th.ArrayType(th.ObjectType()), required=False),
        th.Property("transactionDiscountPercentage", th.NumberType, required=False),
        th.Property("ppCustomerGroupID", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("ppCustomerSiteID", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("ppDeliveryID", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("ppDeliveryMethodID", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("ppDeliveryMultipleID", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("ppDeliveryTimeID", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("ppEmailOnlyCustomer", th.BooleanType, required=False),
        th.Property("ppLanguageID", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("ppIsMember", th.BooleanType, required=False),
        th.Property("ppMemberCardFrom", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("ppMemberCardShopID", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("ppShopID", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("ppZoneID", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("ppPriceDelivery", th.NumberType, required=False),
        th.Property("ppPriceDeliveryFixedDay", th.IntegerType, required=False),
        th.Property("ppPriceDeliveryMultiple", th.NumberType, required=False),
        th.Property("ppPriceDeliveryNextDay", th.IntegerType, required=False),
        th.Property("ppPriceDeliveryNormal", th.IntegerType, required=False),
        th.Property("deliveryAddress", th.ObjectType(), required=False),
        th.Property("type", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("basedOnSale", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("customerPhone", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("customerMobile", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("customerIdCountry", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("customerCard", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("customerIdTitle", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("customerTitle", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("discountReason", th.ObjectType(), required=False),
        th.Property("idTillBasket", th.IntegerType, required=False),
        th.Property("customerCreditsOld", th.NumberType, required=False),
        th.Property("customerCreditsSale", th.NumberType, required=False),
        th.Property("customerCreditsUsed", th.NumberType, required=False),
        th.Property("noNextSaleDiscount", th.BooleanType, required=False),
        th.Property("ogoneReference", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("paymentProvider", th.ObjectType(), required=False),
        th.Property("customerFirstName", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("customerMiddleName", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("customerSurName", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("customerCompanyName", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("customerCode", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("customerAddress1", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("customerAddress2", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("customerAddress3", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("customerAddress4", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("customerAddress5", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("customerHouseNumber", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("customerBox", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("customerCity", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("customerPostalCode", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("customerCounty", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("customerCountry", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("customerVatNumber", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("customerEmail", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("emailPickupPoint", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("idLanguage", th.IntegerType, required=False),
        th.Property("orderNumber", th.CustomType({"type": ["string", "null"]}), required=False),
        th.Property("priceType", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("isVatInclusive", th.BooleanType, required=False),
        th.Property("isDispatch", th.BooleanType, required=False),
        th.Property("deliveryDate", th.CustomType({"type": ["string", "null"]}), required=False),
        th.Property("cashDiscount", th.NumberType, required=False),
        th.Property("cashDiscountDays", th.IntegerType, required=False),
        th.Property("paymentCondition", th.ObjectType(), required=False),
        th.Property("expirationDate", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("vatType", th.ObjectType(), required=False),
        th.Property("vatExempt", th.BooleanType, required=False),
        th.Property("isPeriodicInvoicing", th.BooleanType, required=False),
        th.Property("IsPeriodicInvoicingDiscountTransaction", th.BooleanType, required=False),
        th.Property("status", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("transactionDiscountAmount", th.NumberType, required=False),
        th.Property("basedOnSales", th.ArrayType(th.ObjectType()), required=False),
        th.Property("invoiceMethod", th.ObjectType(), required=False),
        th.Property("legalEntity", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("isOffline", th.BooleanType, required=False),
        th.Property("offlineTimestamp", th.CustomType({"type": ["string", "null"]}), required=False),
        th.Property("idShopMoneyLocation", th.IntegerType, required=False),
        th.Property("orderId", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("seal", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("sealNumber", th.IntegerType, required=False),
        th.Property("dispatchMethod", th.ObjectType(), required=False),
        th.Property("numberOfCollis", th.IntegerType, required=False),
        th.Property("additionalInfo", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("externalReference", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("internalReference", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("isService", th.BooleanType, required=False),
        th.Property("idService", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("ppDisable", th.BooleanType, required=False),
        th.Property("refundPaymentName", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("refundPaymentAccount", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("refundPaymentText", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("externalLoyaltyInfo", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("externalLoyaltyReference", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("externalLoyaltyCredits", th.NumberType, required=False),
        th.Property("activationCode", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("channel", th.ObjectType(), required=False),
        th.Property("currency", th.IntegerType, required=False),
        th.Property("currencyData", th.ObjectType(), required=False),
        th.Property("user", th.ObjectType(), required=False),
        th.Property("timestamp", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("invoice", th.ObjectType(), required=False),
        th.Property("invoice2", th.ObjectType(), required=False),
        th.Property("anonymous", th.BooleanType, required=False),
        th.Property("IdCustomerNSDOtherCustomer", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("isReturnRemake", th.BooleanType, required=False),
        th.Property("isReturn", th.BooleanType, required=False),
        th.Property("idPickupPoint", th.CustomType({"type": ["string", "integer", "null"]}), required=False),
        th.Property("barcode", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("idQuotation", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("passportId", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("legalentityVatNumber", th.CustomType({"type": ["string", "null"]}), required=False),
        th.Property("project", th.ObjectType(), required=False),
        th.Property("foreignVat", th.ObjectType(), required=False),
        th.Property("deliveryAllowedFrom", th.CustomType({"type": ["string", "null"]}), required=False),
        th.Property("expectedDeliveryDate", th.CustomType({"type": ["string", "null"]}), required=False),
        th.Property("isOrder", th.BooleanType, required=False),
        th.Property("isInvoiced", th.BooleanType, required=False),
        th.Property("isSaleWithShopRevenue", th.BooleanType, required=False),
        th.Property("idShopToTransferStockFrom", th.CustomType({"type": ["string", "integer", "null"]}), required=False),
        th.Property("isSaleWithCustomerInvoice", th.BooleanType, required=False),
        th.Property("icons", th.ArrayType(th.ObjectType()), required=False),
        th.Property("tags", th.ArrayType(th.ObjectType()), required=False),
        th.Property("registerAtSupplier", th.BooleanType, required=False),
        th.Property("customerDepartment", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("isSelfCheckout", th.BooleanType, required=False),
        th.Property("contactPerson", th.ObjectType(), required=False),
        th.Property("language", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("deliveryTrackingCode", th.CustomType({"type": ["string", "null"]}), required=False),
        th.Property("duplicatedForInvoice", th.BooleanType, required=False),
        th.Property("idSourceDiscountReason", th.CustomType({"type": ["string", "integer", "null"]}), required=False),
        th.Property("customerCountryCode", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("idSourceShop", th.CustomType({"type": ["string", "integer", "null"]}), required=False),
        th.Property("idSourceTill", th.CustomType({"type": ["string", "integer", "null"]}), required=False),
        th.Property("idSourceUser", th.CustomType({"type": ["string", "integer", "null"]}), required=False),
        th.Property("activationText", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("reInvoiced", th.BooleanType, required=False),
        th.Property("invoiceRequested", th.ObjectType(), required=False),
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
        th.Property("sku_tilroy_id", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("sku_source_id", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("price_tilroy_id", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("price_source_id", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("price", th.NumberType),
        th.Property("type", th.CustomType({"type": ["string", "number", "null"]})),  # "standard" or "promo"
        th.Property("quantity", th.IntegerType),
        th.Property("run_tilroy_id", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("run_source_id", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("label_type", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("end_date", th.DateTimeType, required=False),
        th.Property("start_date", th.DateTimeType, required=False),
        th.Property("date_created", th.DateTimeType),
    ).to_dict()


class StockStream(TilroyStream):
    """Stream for Tilroy stock that collects SKU IDs from products and batches API calls."""
    
    name = "stock"
    path = "/stockapi/production/stock"
    primary_keys: t.ClassVar[list[str]] = ["tilroyId"]
    replication_key = None  # No replication key - always fetches all stock for all products (child of products stream)
    replication_method = "FULL_TABLE"
    records_jsonpath = "$[*]"
    next_page_token_jsonpath = None
    
    # Store collected SKU IDs
    _sku_ids: list[str] = []
    _batch_size = 150  # Number of SKU IDs to send per batch (increased from 50 since pagination is working)
    _api_record_limit = 10  # API returns 10 items per page (pagination supported)
    _current_start_idx = 0
    _detected_api_limit: t.Optional[int] = None  # Track detected API response limit
    _response_counts: list[int] = []  # Track record counts per response for analysis
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._sku_ids = []
        self._current_start_idx = 0
        self._response_counts = []
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
                # Process all pages for this batch of SKU IDs
                page = 1
                total_records_for_batch = 0
                total_pages = None
                
                while True:
                    # Build URL with SKU IDs and page parameter
                    query_params = f"skuTilroyId={sku_ids_param}"
                    if page > 1:
                        query_params += f"&page={page}"
                    
                    # Make the request
                    conn = http.client.HTTPSConnection("api.tilroy.com")
                    conn.request("GET", f"{self.path}?{query_params}", "", headers)
                    response = conn.getresponse()
                    data = response.read().decode("utf-8")
                    
                    # Check for errors
                    if response.status != 200:
                        self.logger.error(f"❌ [{self.name}] API returned status {response.status}: {data[:500]}")
                        # Continue to next batch instead of failing completely
                        break
                    
                    # Extract pagination headers
                    paging_page_count = response.getheader("X-Paging-PageCount")
                    paging_items_per_page = response.getheader("X-Paging-ItemsPerPage")
                    paging_item_count = response.getheader("X-Paging-ItemCount")
                    
                    # Parse pagination info on first page
                    if page == 1:
                        if paging_page_count:
                            total_pages = int(paging_page_count)
                            self.logger.info(
                                f"📄 [{self.name}] Pagination detected: {total_pages} pages, "
                                f"{paging_items_per_page or '10'} items per page, "
                                f"{paging_item_count or '?'} total items"
                            )
                        else:
                            # If no pagination header, assume single page
                            total_pages = 1
                    
                    # Parse the response
                    data_json = json.loads(data)
                    
                    # Log response structure for debugging (first batch, first page only)
                    if start_idx == 0 and page == 1:
                        if isinstance(data_json, dict):
                            self.logger.info(f"📊 [{self.name}] API response structure (first batch): {list(data_json.keys())}")
                            # Check for pagination or match count in response
                            if "matchCount" in data_json or "match_count" in data_json:
                                match_count = data_json.get("matchCount") or data_json.get("match_count")
                                self.logger.info(f"📊 [{self.name}] API match count: {match_count}")
                            if "total" in data_json or "totalCount" in data_json:
                                total = data_json.get("total") or data_json.get("totalCount")
                                self.logger.info(f"📊 [{self.name}] API total count: {total}")
                        elif isinstance(data_json, list):
                            self.logger.info(f"📊 [{self.name}] API response is a list with {len(data_json)} items")
                    
                    # Check if the response is an error object
                    if isinstance(data_json, dict) and "code" in data_json and "message" in data_json:
                        self.logger.error(f"❌ [{self.name}] API returned error: {data_json.get('message', 'Unknown error')}")
                        break
                    
                    # Extract records using jsonpath
                    records = list(extract_jsonpath(self.records_jsonpath, data_json))
                    
                    # Track response counts for analysis
                    self._response_counts.append(len(records))
                    total_records_for_batch += len(records)
                    
                    # Extract unique SKU IDs from the records we got to see which SKUs we successfully retrieved
                    retrieved_sku_ids = set()
                    for record in records:
                        if isinstance(record, dict) and "sku" in record:
                            sku_obj = record.get("sku")
                            if isinstance(sku_obj, dict) and "tilroyId" in sku_obj:
                                retrieved_sku_ids.add(str(sku_obj["tilroyId"]))
                    
                    # Log page progress
                    if total_pages and total_pages > 1:
                        self.logger.info(
                            f"📄 [{self.name}] Batch {start_idx//self._batch_size + 1}, Page {page}/{total_pages}: "
                            f"Got {len(records)} stock records (total so far: {total_records_for_batch})"
                        )
                    else:
                        self.logger.info(
                            f"📊 [{self.name}] Batch {start_idx//self._batch_size + 1}: "
                            f"Got {len(records)} stock records"
                        )
                    
                    # Log the actual SKU IDs for the first batch, first page (for debugging)
                    if start_idx == 0 and page == 1:
                        self.logger.info(
                            f"📋 [{self.name}] First batch SKU IDs ({len(chunk_sku_ids)} total): "
                            f"{', '.join(chunk_sku_ids[:50])}{'...' if len(chunk_sku_ids) > 50 else ''}"
                        )
                    
                    # Yield each record
                    for record in records:
                        # Post-process the record
                        processed_record = self.post_process(record, context)
                        if processed_record:
                            yield processed_record
                    
                    # Check if we need to fetch more pages
                    if not records:
                        # No records on this page, we're done
                        break
                    
                    # If we know total pages, check if we've reached the last page
                    if total_pages and page >= total_pages:
                        break
                    
                    # If we got fewer records than items per page, we're likely on the last page
                    items_per_page = int(paging_items_per_page) if paging_items_per_page else 10
                    if len(records) < items_per_page:
                        break
                    
                    # Move to next page
                    page += 1
                
                # Log summary for this batch
                self.logger.info(
                    f"✅ [{self.name}] Batch {start_idx//self._batch_size + 1} complete: "
                    f"Retrieved {total_records_for_batch} total stock records across {page} page(s) "
                    f"for {len(chunk_sku_ids)} SKU IDs"
                )
                
                # Move to next batch
                self._current_start_idx = end_idx
                
            except Exception as e:
                self.logger.error(f"❌ [{self.name}] Error fetching batch (indices {start_idx}-{end_idx-1}): {str(e)}")
                # Continue to next batch instead of failing completely
                self._current_start_idx = end_idx
                continue
        
        # Log summary of response patterns
        if self._response_counts:
            unique_counts = set(self._response_counts)
            count_frequency = {}
            for count in self._response_counts:
                count_frequency[count] = count_frequency.get(count, 0) + 1
            
            self.logger.info(
                f"📈 [{self.name}] Response analysis: "
                f"Total batches: {len(self._response_counts)}, "
                f"Unique record counts: {sorted(unique_counts)}, "
                f"Most common: {max(count_frequency.items(), key=lambda x: x[1])}"
            )
            
            # Check if we consistently got exactly 10 records
            exactly_10_count = sum(1 for c in self._response_counts if c == 10)
            if exactly_10_count > 0:
                percentage = (exactly_10_count / len(self._response_counts)) * 100
                self.logger.warning(
                    f"⚠️ [{self.name}] {exactly_10_count}/{len(self._response_counts)} batches ({percentage:.1f}%) "
                    f"returned exactly 10 records, suggesting an API hard limit."
                )
        
        self.logger.info(f"✅ [{self.name}] Finished processing all {total_sku_ids} SKU IDs")
    
    def post_process(self, row: dict, context: t.Optional[dict] = None) -> t.Optional[dict]:
        """Post-process each stock record."""
        if not row:
            return None
        
        # Convert nested objects to JSON strings for CSV compatibility (like original)
        return self._stringify_objects(row)

    schema = th.PropertiesList(
        th.Property("tilroyId", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("sku", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("location1", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("location2", th.CustomType({"type": ["string", "number", "null"]}), required=False),
        th.Property("qty", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("refill", th.IntegerType),
        th.Property("dateUpdated", th.DateTimeType),
        th.Property("shop", th.CustomType({"type": ["object", "string", "null"]})),
    ).to_dict()
