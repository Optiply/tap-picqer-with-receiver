"""Stream type classes for tap-picqer."""

from datetime import datetime, timezone
from typing import Optional

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_picqer.client import picqerStream


class ProductsStream(picqerStream):
    """Define custom stream."""
    name = "products"
    path = "/products"
    pagination = True
    primary_keys = ["idproduct"]

    schema = th.PropertiesList(
        th.Property("idproduct", th.IntegerType),
        th.Property("idvatgroup", th.IntegerType),
        th.Property("name", th.StringType),
        th.Property("price", th.NumberType),
        th.Property("fixedstockprice", th.NumberType),
        th.Property("idsupplier", th.IntegerType),
        th.Property("productcode", th.StringType),
        th.Property("productcode_supplier", th.StringType),
        th.Property("deliverytime", th.IntegerType),
        th.Property("description", th.StringType),
        th.Property("barcode", th.StringType),
        th.Property("type", th.StringType),
        th.Property("stock", th.ArrayType(
            th.ObjectType(
                th.Property("idwarehouse", th.IntegerType),
                th.Property("idproduct", th.IntegerType),
                th.Property("stock", th.IntegerType),
                th.Property("reserved", th.IntegerType),
                th.Property("reservedbackorders", th.IntegerType),
                th.Property("reservedpicklists", th.IntegerType),
                th.Property("reservedallocations", th.IntegerType),
                th.Property("freestock", th.IntegerType)
            )
        )),
        th.Property("unlimitedstock", th.BooleanType),
        th.Property("weight", th.IntegerType),
        th.Property("length", th.IntegerType),
        th.Property("width", th.IntegerType),
        th.Property("height", th.IntegerType),
        th.Property("minimum_purchase_quantity", th.IntegerType),
        th.Property("purchase_in_quantities_of", th.IntegerType),
        th.Property("hs_code", th.StringType),
        th.Property("country_of_origin", th.StringType),
        th.Property("active", th.BooleanType),
        th.Property("idfulfilment_customer", th.IntegerType),
        th.Property("analysis_pick_amount_per_day", th.CustomType({"type": ["number", "string"]})),
        th.Property("pricelists", th.ArrayType(
            th.ObjectType(
                th.Property("idpricelist", th.IntegerType),
                th.Property("price", th.NumberType)
            )
        )),
        th.Property("created", th.DateTimeType),
        th.Property("updated", th.DateTimeType),
        th.Property("assembled", th.BooleanType),
        th.Property("show_on_portal", th.BooleanType),
        th.Property("entity_type", th.StringType),
        th.Property("received_at", th.DateTimeType),
        th.Property("action", th.StringType),

    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
             "idproduct": record["idproduct"],
             "type": record["type"]
        }

class ProductsInativeStream(picqerStream):
    """Define custom stream."""
    name = "products_inactive"
    path = "/products?inactive=true"
    pagination = True
    primary_keys = ["idproduct"]
    replication_key = "updated"

    schema = th.PropertiesList(
        th.Property("idproduct", th.IntegerType),	
        th.Property("idvatgroup", th.IntegerType),	
        th.Property("name", th.StringType),
        th.Property("price", th.NumberType),	
        th.Property("fixedstockprice", th.NumberType),	
        th.Property("idsupplier", th.IntegerType),	
        th.Property("productcode", th.StringType),	
        th.Property("productcode_supplier", th.StringType),
        th.Property("deliverytime", th.IntegerType),	
        th.Property("description", th.StringType),	
        th.Property("barcode", th.StringType),	
        th.Property("type", th.StringType),	
        th.Property("stock", th.ArrayType(
            th.ObjectType(
                th.Property("idwarehouse", th.IntegerType),
                th.Property("idproduct", th.IntegerType),
                th.Property("stock", th.IntegerType),
                th.Property("reserved", th.IntegerType),
                th.Property("reservedbackorders", th.IntegerType),
                th.Property("reservedpicklists", th.IntegerType),
                th.Property("reservedallocations", th.IntegerType),
                th.Property("freestock", th.IntegerType)
            )
        )),
        th.Property("unlimitedstock", th.BooleanType),
        th.Property("weight", th.IntegerType),	
        th.Property("length", th.IntegerType),	
        th.Property("width", th.IntegerType),	
        th.Property("height", th.IntegerType),	
        th.Property("minimum_purchase_quantity", th.IntegerType),	
        th.Property("purchase_in_quantities_of", th.IntegerType),	
        th.Property("hs_code", th.StringType),	
        th.Property("country_of_origin", th.StringType),
        th.Property("active", th.BooleanType),	
        th.Property("idfulfilment_customer", th.IntegerType),
        th.Property("analysis_pick_amount_per_day", th.CustomType({"type": ["number", "string"]})),
        th.Property("pricelists", th.ArrayType(
            th.ObjectType(
                th.Property("idpricelist", th.IntegerType),
                th.Property("price", th.NumberType)
            )
        )),
        th.Property("created", th.DateTimeType),
        th.Property("updated", th.DateTimeType),
        th.Property("assembled", th.BooleanType),
        th.Property("show_on_portal", th.BooleanType)

    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams.""" 
        return {
             "idproduct": record["idproduct"]
        }

    def post_process(self, row: dict, context: Optional[dict]) -> Optional[dict]:
        """Filter records client-side for incremental behavior."""
        start_ts = self.get_starting_timestamp(context)
        if not start_ts:
            return row

        updated_ts = self._parse_timestamp(row.get(self.replication_key))
        if not updated_ts:
            return row

        if self._normalize_timestamp(updated_ts) <= self._normalize_timestamp(start_ts):
            return None
        return row

    @staticmethod
    def _parse_timestamp(value: Optional[str]) -> Optional[datetime]:
        """Parse timestamp values returned by Picqer."""
        if not value:
            return None
        if isinstance(value, datetime):
            return value
        text_value = str(value)
        try:
            return datetime.fromisoformat(text_value)
        except ValueError:
            try:
                return datetime.strptime(text_value, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None

    @staticmethod
    def _normalize_timestamp(value: datetime) -> datetime:
        """Normalize to naive UTC for safe datetime comparison."""
        if value.tzinfo is None:
            return value
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    
    
class StockHistoryStream(picqerStream):
    name = "stock_history"
    path = "/stockhistory"
    pagination = True
    primary_keys = ["idproduct", "idwarehouse"]
    replication_key = "changed_at"

    schema = th.PropertiesList(
        th.Property("idproduct_stock_history", th.IntegerType),
        th.Property("idproduct", th.IntegerType),
        th.Property("idwarehouse", th.IntegerType),
        th.Property("iduser", th.IntegerType),
        th.Property("idlocation", th.IntegerType),
        th.Property("old_stock", th.IntegerType),
        th.Property("stock_change", th.IntegerType),
        th.Property("new_stock", th.IntegerType),
        th.Property("reason", th.StringType),
        th.Property("change_type", th.StringType),
        th.Property("changed_at", th.DateTimeType),  # replication_key; API returns this, not sincedate
    ).to_dict()

    @property
    def is_timestamp_replication_key(self) -> bool:
        """Avoid catalog schema override breaking type detection."""
        return True

    def post_process(self, row: dict, context: Optional[dict]) -> Optional[dict]:
        """Skip error/empty rows (API does not return sincedate; we use changed_at for bookmarking)."""
        if not row or row.get("error"):
            return None
        return row

class WareHouseProductsStream(picqerStream):
    name = "warehouse_products"
    path = "/products/{idproduct}/warehouses"
    pagination = False
    primary_keys = ["idwarehouse"]
    parent_stream_type = ProductsStream
    schema = th.PropertiesList(
        th.Property("idproduct", th.IntegerType), 
        th.Property("idwarehouse", th.IntegerType), 
        th.Property("stock_level_order", th.IntegerType), 
        th.Property("stock_level_desired", th.IntegerType), 
        th.Property("stock_level_maximum", th.IntegerType), 
        th.Property("stock_location", th.StringType)
    ).to_dict()


class StockProductsStream(picqerStream):
    name = "stock_products"
    path = "/products/{idproduct}/stock"
    primary_keys = ["idwarehouse"]
    pagination = False
    parent_stream_type = ProductsStream
    schema = th.PropertiesList(
        th.Property("idwarehouse", th.IntegerType),
        th.Property("idproduct", th.IntegerType),
        th.Property("stock", th.IntegerType),
        th.Property("reserved", th.IntegerType),
        th.Property("reservedbackorders", th.IntegerType),
        th.Property("reservedpicklists", th.IntegerType),
        th.Property("reservedallocations", th.IntegerType),
        th.Property("freestock", th.IntegerType)
    ).to_dict()


class ImageProductsStream(picqerStream):
    name = "image_products"
    path = "/products/{idproduct}/images"
    pagination = False
    primary_keys = ["idproduct_image"]
    parent_stream_type = ProductsStream
    schema = th.PropertiesList(
        th.Property("idproduct", th.IntegerType), 
        th.Property("idproduct_image", th.IntegerType),
        th.Property("url", th.StringType), 
        th.Property("contenttype", th.StringType),
        th.Property("size", th.IntegerType), 
    ).to_dict()


class LocationProductsStream(picqerStream):
    name = "location_products"
    path = "/products/{idproduct}/locations"
    pagination = False
    primary_keys = ["idlocation"]
    parent_stream_type = ProductsStream
    schema = th.PropertiesList(
        th.Property("idlocation", th.IntegerType),
        th.Property("idwarehouse", th.IntegerType),
        th.Property("parent_idlocation", th.IntegerType),
        th.Property("name", th.StringType),
        # th.Property("remarks", th.StringType) null,
        th.Property("unlink_on_empty", th.BooleanType) ,
        th.Property("location_type", th.ObjectType(
            th.Property("idlocation_type", th.IntegerType),
            th.Property("name", th.StringType),
            th.Property("default", th.BooleanType),
            th.Property("color", th.StringType)
        )),
        th.Property("is_bulk_location", th.BooleanType),
        th.Property("stock_for_product", th.ObjectType(
            th.Property("stock", th.IntegerType),
            th.Property("stock_reserved_picklists", th.IntegerType),        
        )) 
    ).to_dict()


class PartProductsStream(picqerStream):
    name = "part_products"
    path = "/products/{idproduct}/parts"
    pagination = False
    primary_keys = ["idproduct_part"]
    parent_stream_type = ProductsStream
    
    ALLOWED_PRODUCT_TYPES: Optional[tuple] = ("virtual_composition", "composition_with_stock")

    schema = th.PropertiesList(
        th.Property("idproduct_part", th.IntegerType),
        th.Property("idproduct_main", th.IntegerType), 
        th.Property("idproduct", th.IntegerType),
        th.Property("amount", th.IntegerType), 
        th.Property("productcode", th.StringType),
        th.Property("name", th.StringType), 
    ).to_dict()

    def get_records(self, context: Optional[dict]):
        """Skip requesting parts when parent product type is not in ALLOWED_PRODUCT_TYPES."""
        if self.ALLOWED_PRODUCT_TYPES is not None and context:
            product_type = context.get("type")
            if product_type not in self.ALLOWED_PRODUCT_TYPES:
                return
        yield from super().get_records(context)

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        if row.get('error') == True:
            return None
        else:
            row['idproduct_main'] = context['idproduct']
            return row

    

class SuppliersStream(picqerStream):
    name ="suppliers"
    path = "/suppliers"
    pagination = True
    primary_keys=["idsupplier"]
    schema = th.PropertiesList(
        th.Property("idsupplier", th.IntegerType),	
        th.Property("name", th.StringType),	
        th.Property("contactname", th.StringType),	
        th.Property("address", th.StringType),
        th.Property("address2", th.StringType),	
        th.Property("zipcode", th.StringType),
        th.Property("city", th.StringType),
        th.Property("region", th.StringType),
        th.Property("country", th.StringType),
        th.Property("telephone", th.StringType),	
        th.Property("emailaddress", th.StringType),	
        th.Property("remarks", th.StringType),	
        th.Property("language", th.StringType),	
    ).to_dict()


class WarehousesStream(picqerStream):
    name ="warehouses"
    path = "/warehouses"
    pagination = True
    primary_keys=["idwarehouse"]
    schema = th.PropertiesList(
        th.Property("idwarehouse", th.IntegerType),	
        th.Property("name", th.StringType),	
        th.Property("accept_orders", th.BooleanType),	
        th.Property("counts_for_general_stock", th.BooleanType),
        th.Property("priority", th.IntegerType),	
        th.Property("active", th.BooleanType),
    ).to_dict()


class ProductFieldsStream(picqerStream):
    name ="productfields"
    path = "/productfields"
    pagination = True
    primary_keys=["idproductfield"]
    schema = th.PropertiesList(
        th.Property("idproductfield", th.IntegerType),	
        th.Property("title", th.StringType),	
        th.Property("type", th.StringType),	
        th.Property("required", th.BooleanType),	
        th.Property("visible_picklist", th.BooleanType),
        th.Property("visible_invoice", th.BooleanType),
        th.Property("visible_shippinglist", th.BooleanType),
        th.Property("visible_portal", th.BooleanType),
        th.Property("visible_purchase_order", th.BooleanType),
    ).to_dict()


class OrdersStream(picqerStream):
    """Orders stream. Picqer API filters via query param sincedate (format: 2022-01-01 12:00:00)."""

    name = "orders"
    path = "/orders"
    pagination = True
    primary_keys = ["idorder"]
    replication_key = "created"
    schema = th.PropertiesList(
        th.Property("idorder", th.IntegerType), 
        th.Property("idcustomer", th.IntegerType),
        th.Property("idtemplate", th.IntegerType), 
        th.Property("idshippingprovider_profile", th.IntegerType), 
        th.Property("orderid", th.StringType), 
        th.Property("deliveryname", th.StringType), 
        th.Property("deliverycontactname", th.StringType), 
        th.Property("deliveryaddress", th.StringType), 
        th.Property("deliveryaddress2", th.StringType), 
        th.Property("deliveryzipcode", th.StringType), 
        th.Property("deliverycity", th.StringType), 
        th.Property("deliveryregion", th.StringType), 
        th.Property("deliverycountry", th.StringType), 
        th.Property("full_delivery_address", th.StringType), 
        th.Property("invoicename", th.StringType), 
        th.Property("invoicecontactname", th.StringType), 
        th.Property("invoiceaddress", th.StringType), 
        th.Property("invoiceaddress2", th.StringType), 
        th.Property("invoicezipcode", th.StringType), 
        th.Property("invoicecity", th.StringType), 
        th.Property("invoiceregion", th.StringType), 
        th.Property("invoicecountry", th.StringType), 
        th.Property("full_invoice_address", th.StringType), 
        th.Property("telephone", th.StringType), 
        th.Property("emailaddress", th.StringType), 
        th.Property("reference", th.StringType), 
        th.Property("customer_remarks", th.StringType), 
        th.Property("partialdelivery", th.BooleanType), 
        th.Property("discount", th.NumberType), 
        th.Property("invoiced", th.BooleanType), 
        th.Property("status", th.StringType), 
        th.Property("idfulfilment_customer", th.IntegerType), 
        th.Property("warehouses", th.ArrayType(th.IntegerType)), 
        th.Property("preferred_delivery_date", th.DateTimeType), 
        th.Property("language", th.StringType), 
        th.Property("products", th.ArrayType(
            th.ObjectType(
                th.Property("idproduct", th.IntegerType), 
                th.Property("amount", th.IntegerType), 
                th.Property("productcode", th.StringType), 
                th.Property("name", th.StringType), 
                th.Property("remarks", th.StringType), 
                th.Property("price", th.NumberType), 
                th.Property("idvatgroup", th.IntegerType), 
            )
        )), 
       
        th.Property("pricelists", th.CustomType({"type": ["array", "string"]})), 
        th.Property("picklists",  th.CustomType({"type": ["array", "string"]})), 
        th.Property("created", th.DateTimeType),
        th.Property("updated", th.DateTimeType)

    ).to_dict()


class OrderFieldsStream(picqerStream):
    name ="orderfields"
    path = "/orderfields"
    pagination = True
    primary_keys=["idorderfield"]
    schema = th.PropertiesList(
        th.Property("idorderfield", th.IntegerType),	
        th.Property("title", th.StringType),	
        th.Property("type", th.StringType),	
        th.Property("required", th.BooleanType),	
        th.Property("visible_picklist", th.BooleanType),
        th.Property("visible_portal", th.BooleanType),
    ).to_dict()


class BackOrdersStream(picqerStream):
    name ="backorders"
    path = "/backorders"
    pagination = True
    primary_keys=["idbackorder"]
    replication_key = "created_at"
    schema = th.PropertiesList(
        th.Property("idbackorder", th.IntegerType),	
        th.Property("idorder", th.IntegerType),	
        th.Property("idproduct", th.IntegerType),
        th.Property("idcustomer", th.IntegerType),
        th.Property("idwarehouse", th.IntegerType),	
        th.Property("amount", th.IntegerType),	
        th.Property("amount_available", th.IntegerType),	
        th.Property("priority", th.IntegerType),
        th.Property("created_at", th.DateTimeType),
        th.Property("date_available", th.DateTimeType),
    ).to_dict()


class PurchaseOrdersStream(picqerStream):
    name ="purchaseorders"
    path = "/purchaseorders"
    pagination = True
    primary_keys=["idpurchaseorder"]
    replication_key = "updated"

    schema = th.PropertiesList(
        th.Property("idpurchaseorder", th.IntegerType),	
        th.Property("idsupplier", th.IntegerType),	
        th.Property("idtemplate", th.IntegerType),
        th.Property("idwarehouse", th.IntegerType),
        th.Property("purchaseorderid", th.StringType),
        th.Property("supplier_name", th.StringType),
        th.Property("supplier_orderid", th.StringType),
        th.Property("updated", th.DateTimeType),
        th.Property("created", th.DateTimeType),
        th.Property("status", th.StringType),
        th.Property("remarks", th.StringType),
        th.Property("delivery_date", th.StringType),
        th.Property("language", th.StringType),
        th.Property("products", th.ArrayType(
            th.ObjectType(
                th.Property("idproduct", th.IntegerType),
                th.Property("amount", th.IntegerType),
                th.Property("price", th.NumberType),
            )
        )),
        th.Property("idfulfilment_customer", th.IntegerType)
       
    ).to_dict()


class PicklistsStream(picqerStream):
    """Define custom stream."""
    name = "picklists"
    path = "/picklists"
    primary_keys = ["idpicklist"]
    pagination = True
    replication_key = "created"
    schema = th.PropertiesList(
        th.Property("idpicklist", th.IntegerType),
        th.Property("picklistid", th.StringType),
        th.Property("idcustomer", th.IntegerType),
        th.Property("idorder", th.IntegerType),
        th.Property("idreturn", th.IntegerType),
        th.Property("idwarehouse", th.IntegerType),
        th.Property("idtemplate", th.IntegerType),
        th.Property("idshippingprovider_profile", th.IntegerType),
        th.Property("deliveryname", th.StringType),
        th.Property("deliverycontact", th.StringType),
        th.Property("deliveryaddress", th.StringType),
        th.Property("deliveryaddress2", th.StringType),
        th.Property("deliveryzipcode", th.StringType),
        th.Property("deliverycity", th.StringType),
        th.Property("deliveryregion", th.StringType),
        th.Property("deliverycountry", th.StringType),
        th.Property("emailaddress", th.StringType),
        th.Property("telephone", th.StringType),
        th.Property("reference", th.StringType),
        th.Property("assigned_to_iduser", th.IntegerType),
        th.Property("invoiced", th.BooleanType),
        th.Property("urgent", th.BooleanType),
        th.Property("status", th.StringType),
        th.Property("totalproducts", th.IntegerType),
        th.Property("totalpicked", th.IntegerType),
        th.Property("snoozed_until", th.DateTimeType),
        th.Property("closed_by_iduser", th.IntegerType),
        th.Property("closed_at", th.DateTimeType),
        th.Property("created", th.DateTimeType),
        th.Property("updated", th.DateTimeType),
        th.Property("products", th.ArrayType(th.ObjectType(
            th.Property("idpicklist_product", th.IntegerType),
            th.Property("idproduct", th.IntegerType),
            th.Property("idorder_product", th.IntegerType),
            th.Property("idreturn_product_replacement", th.IntegerType),
            th.Property("idvatgroup", th.IntegerType),
            th.Property("productcode", th.StringType),
            th.Property("name", th.StringType),
            th.Property("remarks", th.StringType),
            th.Property("amount", th.IntegerType),
            th.Property("amount_picked", th.IntegerType),
            th.Property("price", th.NumberType),
            th.Property("weight", th.IntegerType),
            th.Property("stocklocation", th.StringType),
            th.Property("has_parts", th.BooleanType),
            th.Property("pick_locations", th.ArrayType(th.ObjectType(
                th.Property("idlocation", th.IntegerType),
                th.Property("name", th.StringType),
                th.Property("amount", th.IntegerType)
            )))
        )))
    ).to_dict()



class PicklistsClosedStream(picqerStream):
    """Define custom stream."""
    name = "picklists_closed"
    path = "/picklists?status=closed"
    primary_keys = ["idpicklist"]
    pagination = True
    replication_key = "created"
    schema = th.PropertiesList(
        th.Property("idpicklist", th.IntegerType),
        th.Property("picklistid", th.StringType),
        th.Property("idcustomer", th.IntegerType),
        th.Property("idorder", th.IntegerType),
        th.Property("idreturn", th.IntegerType),
        th.Property("idwarehouse", th.IntegerType),
        th.Property("idtemplate", th.IntegerType),
        th.Property("idshippingprovider_profile", th.IntegerType),
        th.Property("deliveryname", th.StringType),
        th.Property("deliverycontact", th.StringType),
        th.Property("deliveryaddress", th.StringType),
        th.Property("deliveryaddress2", th.StringType),
        th.Property("deliveryzipcode", th.StringType),
        th.Property("deliverycity", th.StringType),
        th.Property("deliveryregion", th.StringType),
        th.Property("deliverycountry", th.StringType),
        th.Property("emailaddress", th.StringType),
        th.Property("telephone", th.StringType),
        th.Property("reference", th.StringType),
        th.Property("assigned_to_iduser", th.IntegerType),
        th.Property("invoiced", th.BooleanType),
        th.Property("urgent", th.BooleanType),
        th.Property("status", th.StringType),
        th.Property("totalproducts", th.IntegerType),
        th.Property("totalpicked", th.IntegerType),
        th.Property("snoozed_until", th.DateTimeType),
        th.Property("closed_by_iduser", th.IntegerType),
        th.Property("closed_at", th.DateTimeType),
        th.Property("created", th.DateTimeType),
        th.Property("updated", th.DateTimeType),
        th.Property("products", th.ArrayType(th.ObjectType(
            th.Property("idpicklist_product", th.IntegerType),
            th.Property("idproduct", th.IntegerType),
            th.Property("idorder_product", th.IntegerType),
            th.Property("idreturn_product_replacement", th.IntegerType),
            th.Property("idvatgroup", th.IntegerType),
            th.Property("productcode", th.StringType),
            th.Property("name", th.StringType),
            th.Property("remarks", th.StringType),
            th.Property("amount", th.IntegerType),
            th.Property("amount_picked", th.IntegerType),
            th.Property("price", th.NumberType),
            th.Property("weight", th.IntegerType),
            th.Property("stocklocation", th.StringType),
            th.Property("has_parts", th.BooleanType),
            th.Property("pick_locations", th.ArrayType(th.ObjectType(
                th.Property("idlocation", th.IntegerType),
                th.Property("name", th.StringType),
                th.Property("amount", th.IntegerType)
            )))
        )))
    ).to_dict()


class ReceiptsStream(picqerStream):
    """Define custom stream."""


    name = "receipts"
    path = "/receipts"
    pagination = True
    primary_keys = ["idreceipt"]
    replication_key = "updated"

    schema = th.PropertiesList(
        th.Property("idreceipt", th.IntegerType),
        th.Property("idwarehouse", th.IntegerType),
        th.Property("version", th.IntegerType),
        th.Property("supplier", th.ObjectType(
            th.Property("idsupplier", th.IntegerType),
            th.Property("name", th.StringType),
        )),
        th.Property("purchaseorder", th.ObjectType(
            th.Property("idpurchaseorder", th.IntegerType),
            th.Property("purchaseorderid", th.StringType),
        )),
        th.Property("receiptid", th.StringType),
        th.Property("status", th.StringType),
        th.Property("remarks", th.StringType),
        th.Property("completed_by", th.CustomType({"type": ["object", "null"], "properties": {"iduser": {"type": "integer"}, "name": {"type": "string"}}})),
        th.Property("amount_received", th.IntegerType),
        th.Property("amount_received_excessive", th.IntegerType),
        th.Property("completed_at", th.DateTimeType),
        th.Property("created", th.DateTimeType),
        th.Property("updated", th.DateTimeType),
        th.Property("products", th.ArrayType(
            th.ObjectType(
                th.Property("idreceipt_product", th.IntegerType),
                th.Property("idpurchaseorder_product", th.IntegerType),
                th.Property("idproduct", th.IntegerType),
                th.Property("idpurchaseorder", th.IntegerType),
                th.Property("productcode", th.StringType),
                th.Property("productcode_supplier", th.StringType),
                th.Property("name", th.StringType),
                th.Property("barcode", th.StringType),
                th.Property("amount", th.IntegerType),
                th.Property("amount_ordered", th.IntegerType),
                th.Property("amount_receiving", th.IntegerType),
                th.Property("added_by_receipt", th.BooleanType),
                th.Property("abc_classification", th.CustomType({"type": ["object", "string", "null"]})),
                th.Property("location", th.CustomType({"type": ["object", "array", "null"]})),
                th.Property("created_at", th.DateTimeType),
                th.Property("received_by_iduser", th.IntegerType),
                th.Property("reverted_at", th.DateTimeType),
                th.Property("reverted_by_iduser", th.IntegerType),
            )
        )),
    ).to_dict()

