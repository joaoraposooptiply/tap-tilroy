"""Sales stream for Tilroy API."""

from __future__ import annotations

import json
import math
import typing as t
from datetime import datetime
from decimal import Decimal

from singer_sdk import typing as th

from tap_tilroy.client import DateWindowedStream

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


# Fields that should remain as strings even if they look numeric
# Includes ID fields that have schema ['string', 'integer', 'null'] - must be string if decimal
STRING_FIELDS = frozenset({
    "idTilroySale", "idTenant", "idSession", "idSourceCustomer",
    "idTilroySaleLine", "idTilroySalePayment", "idTilroy", "idSource",
    "idSourceShop", "idSourceTill", "idSourceLegalEntity",
    "code", "ean", "paymentReference", "advanceReference",
    "comments", "description", "webDescription", "colour", "size",
    "serialNumberSale", "serialNumberSaleActivator", "promotionName",
    "orderId", "orderLineId", "orderNumberOriginal", "orderDateOriginal",
    "collectMethodCodeOriginal", "paymentReferenceNegativeAdvance",
    "paymentReferenceReturnOrderAdvance", "czamReference", "czamTicket",
    "merchantInfo", "linkedObjectBarcode", "linkedObjectDisplayValue",
    "linkedObjectReference", "linkedObjectReferenceType", "picture",
    "intrastatCode", "combinedProductCode", "usedProductBarcode",
    "dispatchMethodCode", "dispatchMethodExtraData", "userSalesPerson",
    "salesOrigin", "idSourceSku", "_id", "timestamp", "reservationReference",
    "configuratorCode", "basedOnSale", "basedOnSaleLine", "idRental",
    "ppBundleDealID", "ppActionID", "ppBuyAndGetID", "ppCopyFrom",
    "ppDiscountReasonID", "ppExceptionalPriceID", "ppLineDiscountID",
    "ppReturnIncentiveID", "ppSetID", "ppTriggerID", "idPaymentRequest",
    "idInvoicePayment", "idAssetType", "idAsset", "idLeasing",
    "idSourceDiscountReason", "idSourceReturnReason", "idSourcePaymentType",
    "idSourceSkuTransform", "idCollectMethod", "idTillBasketLine",
    "idTillBasketLineOrig", "idSaleLineReturned", "idRepair",
    "idTransactionLine", "idVat", "idUserSalesPerson", "idDispatchMethod",
    "combinedProductId", "ppCustomerGroupID", "ppCustomerSiteID", "ppDeliveryID",
    "ppDeliveryMethodID", "ppDeliveryMultipleID", "ppDeliveryTimeID",
    "ppLanguageID", "type", "vatKind", "warrantyDate",
    "deliveryDate", "wac", "collectMethod", "order", "collectShop",
    "discountReason", "returnReason", "sku", "advanceSource",
    "descriptions", "icons", "insurances", "shipment", "taxes",
    "paymentType", "customer", "shop", "till", "vatTypeCalculation",
    "tenantCurrency", "supplierCurrency", "discount", "legalEntity",
    "orderNumber", "currency",
})


def _is_numeric_string(value: str) -> bool:
    """Check if a string represents a number."""
    try:
        float(value)
        return True
    except (ValueError, TypeError):
        return False


def _convert_decimal(val: Decimal, force_string: bool = False) -> t.Union[int, float, str]:
    """Convert a Decimal to int, float, or string.
    
    Args:
        val: The Decimal value to convert.
        force_string: If True, always convert to string.
    
    Returns:
        int if whole number, float for decimals, or string if force_string.
    """
    if force_string:
        return str(int(val)) if val == val.to_integral_value() else str(val)
    elif val == val.to_integral_value():
        return int(val)
    else:
        return float(val)


def _convert_types_recursive(obj: t.Any, string_fields: frozenset = STRING_FIELDS) -> t.Any:
    """Recursively convert Decimals and string numbers to appropriate types.

    Args:
        obj: The object to convert.
        string_fields: Fields that should remain as strings.

    Returns:
        The converted object.
    """
    if isinstance(obj, dict):
        result = {}
        for key, val in obj.items():
            if isinstance(val, Decimal):
                # Convert Decimal - use string for fields in string_fields
                result[key] = _convert_decimal(val, force_string=(key in string_fields))
            elif isinstance(val, float):
                # Handle infinity and NaN - keep as float (or convert to None)
                if math.isinf(val) or math.isnan(val):
                    result[key] = None  # Convert inf/nan to null for JSON compatibility
                elif val == int(val):
                    # Convert whole number floats to int
                    result[key] = int(val)
                else:
                    result[key] = val
            elif isinstance(val, str) and _is_numeric_string(val) and key not in string_fields:
                num = float(val)
                if math.isinf(num) or math.isnan(num):
                    result[key] = None
                elif num == int(num):
                    result[key] = int(num)
                else:
                    result[key] = num
            elif isinstance(val, (list, dict)):
                result[key] = _convert_types_recursive(val, string_fields)
            else:
                result[key] = val
        return result
    elif isinstance(obj, list):
        return [_convert_types_recursive(item, string_fields) for item in obj]
    elif isinstance(obj, Decimal):
        return _convert_decimal(obj)
    elif isinstance(obj, str) and _is_numeric_string(obj):
        num = float(obj)
        if math.isinf(num) or math.isnan(num):
            return None
        return int(num) if num == int(num) else num
    return obj


class SalesStream(DateWindowedStream):
    """Stream for Tilroy sales transactions.

    Uses /sales endpoint with lastId pagination. Date windowing is applied
    to avoid API timeouts on large date ranges.
    
    Note: The /sales endpoint requires lastId=0 to start (not empty string).
    """

    name = "sales"
    path = "/saleapi/production/sales"
    primary_keys: t.ClassVar[list[str]] = ["idTilroySale"]
    replication_key = "saleDate"
    replication_method = "INCREMENTAL"
    records_jsonpath = "$[*]"
    default_count = 100

    # Date windowing configuration - 7 days at a time to avoid timeouts
    date_window_days = 7
    use_date_to = True
    date_to_param_name = "dateTo"

    # Use lastId pagination for /sales endpoint
    use_last_id_pagination = True
    last_id_field: str = "idTilroySale"
    last_id_param: str = "lastId"

    schema = th.PropertiesList(
        th.Property("idTilroySale", th.CustomType({"type": ["string", "integer"]})),
        th.Property("idTenant", th.CustomType({"type": ["string", "integer"]})),
        th.Property("idSession", th.CustomType({"type": ["string", "integer", "null"]})),
        th.Property("customer", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("idSourceCustomer", th.CustomType({"type": ["string", "null"]})),
        th.Property("shop", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("shop_tilroy_id", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("shop_number", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("shop_name", th.CustomType({"type": ["string", "null"]})),
        th.Property("shop_source_id", th.CustomType({"type": ["string", "null"]})),
        th.Property("till", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("legalEntity", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("saleDate", th.DateTimeType),
        th.Property("orderDate", th.CustomType({"type": ["string", "null"]})),
        th.Property("deliveryDate", th.CustomType({"type": ["string", "null"]})),
        th.Property("totalAmountStandard", th.NumberType),
        th.Property("totalAmountSell", th.NumberType),
        th.Property("totalAmountDiscount", th.NumberType),
        th.Property("totalAmountSellRounded", th.NumberType),
        th.Property("totalAmountSellRoundedPart", th.NumberType),
        th.Property("totalAmountSellNotRoundedPart", th.NumberType),
        th.Property("totalAmountOutstanding", th.NumberType),
        th.Property("totalAmountPaid", th.NumberType),
        th.Property("eTicket", th.BooleanType),
        th.Property("isOrder", th.BooleanType),
        th.Property("isReturn", th.BooleanType),
        th.Property("isInvoiced", th.BooleanType),
        th.Property("anonymous", th.BooleanType),
        th.Property("lines", th.CustomType({"type": ["array", "null"]})),
        th.Property("payments", th.CustomType({"type": ["array", "null"]})),
        th.Property("vat", th.CustomType({"type": ["array", "null"]})),
        th.Property("vouchers", th.CustomType({"type": ["array", "null"]})),
        th.Property("activations", th.CustomType({"type": ["array", "null"]})),
        th.Property("credits", th.CustomType({"type": ["array", "null"]})),
        th.Property("saleCredits", th.CustomType({"type": ["array", "null"]})),
        th.Property("basedOnSales", th.CustomType({"type": ["array", "null"]})),
        th.Property("icons", th.CustomType({"type": ["array", "null"]})),
        th.Property("tags", th.CustomType({"type": ["array", "null"]})),
        th.Property("vatTypeCalculation", th.CustomType({"type": ["object", "string", "null"]})),
        th.Property("currency", th.CustomType({"type": ["string", "integer", "null"]})),
        th.Property("currencyData", th.CustomType({"type": ["object", "null"]})),
        th.Property("orderNumber", th.CustomType({"type": ["string", "null"]})),
        th.Property("type", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("status", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("channel", th.CustomType({"type": ["object", "null"]})),
        th.Property("user", th.CustomType({"type": ["object", "null"]})),
        th.Property("invoice", th.CustomType({"type": ["object", "null"]})),
        th.Property("invoice2", th.CustomType({"type": ["object", "null"]})),
        th.Property("discountReason", th.CustomType({"type": ["object", "null"]})),
        th.Property("dispatchMethod", th.CustomType({"type": ["object", "null"]})),
        th.Property("deliveryAddress", th.CustomType({"type": ["object", "null"]})),
        th.Property("paymentCondition", th.CustomType({"type": ["object", "null"]})),
        th.Property("paymentProvider", th.CustomType({"type": ["object", "null"]})),
        th.Property("vatType", th.CustomType({"type": ["object", "null"]})),
        th.Property("contactPerson", th.CustomType({"type": ["object", "null"]})),
        th.Property("project", th.CustomType({"type": ["object", "null"]})),
        th.Property("foreignVat", th.CustomType({"type": ["object", "null"]})),
        th.Property("invoiceRequested", th.CustomType({"type": ["object", "null"]})),
        th.Property("deliveryPromise", th.CustomType({"type": ["object", "null"]})),
        th.Property("collectMethod", th.CustomType({"type": ["object", "null"]})),
        th.Property("returnReason", th.CustomType({"type": ["object", "null"]})),
        th.Property("transactionDiscountPercentage", th.NumberType),
        th.Property("transactionDiscountAmount", th.NumberType),
        th.Property("customerFirstName", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("customerSurName", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("customerEmail", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("customerPhone", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("customerMobile", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("customerCity", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("customerPostalCode", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("customerCountry", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("customerVatNumber", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("orderId", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("seal", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("barcode", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("timestamp", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("language", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("externalReference", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("internalReference", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("additionalInfo", th.CustomType({"type": ["string", "number", "null"]})),
        th.Property("sealNumber", th.IntegerType),
        th.Property("numberOfCollis", th.IntegerType),
        th.Property("idTillBasket", th.IntegerType),
        th.Property("idLanguage", th.IntegerType),
        th.Property("idShopMoneyLocation", th.IntegerType),
        th.Property("cashDiscountDays", th.IntegerType),
    ).to_dict()

    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        """Post-process sales record.

        Validates required fields and converts types.
        """
        if not row:
            return None

        # Skip error responses
        if "code" in row and "message" in row:
            self.logger.warning(f"[{self.name}] Skipping error record: {row['message']}")
            return None

        # Validate replication key
        if not row.get(self.replication_key):
            self.logger.warning(f"[{self.name}] Skipping record without {self.replication_key}")
            return None

        # Convert types
        row = _convert_types_recursive(row)

        # Flatten shop object for easier filtering
        row = self._flatten_shop(row, "shop", "shop")

        # Stringify object fields inside lines to match catalog schema
        # These fields can be objects but catalog defines them as strings
        row = self._stringify_lines_objects(row)

        # Ensure ID fields are strings
        for field in ("idTilroySale", "idTenant", "idSession", "idSourceCustomer"):
            if field in row and row[field] is not None:
                row[field] = str(row[field])

        return row

    def _stringify_lines_objects(self, row: dict) -> dict:
        """Convert specific object fields inside lines array to JSON strings.

        The Tilroy API returns deliveryPromise as an object, but the
        catalog schema expects it as a string. This converts it to
        a JSON string for compatibility.
        
        Only stringify fields that the catalog expects as strings but API
        returns as objects. Most fields should remain as objects.
        """
        # Only deliveryPromise needs to be stringified - catalog expects string
        # All other fields (sku, collectMethod, etc.) are expected as objects
        stringify_fields = {"deliveryPromise"}

        lines = row.get("lines")
        if isinstance(lines, list):
            for line in lines:
                if isinstance(line, dict):
                    for field in stringify_fields:
                        if field in line and isinstance(line[field], (dict, list)):
                            line[field] = json.dumps(line[field])

        return row
