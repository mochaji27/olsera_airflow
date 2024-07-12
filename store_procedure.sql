USE [Olsera]
GO

CREATE PROCEDURE [dbo].[sp_delete_close_order]
@outlet_name nvarchar(100) = NULL,
@start_date date,
@end_date date
AS
BEGIN
if (@outlet_name is NULL)
BEGIN
	DELETE close_order from close_order where order_date >= @start_date and order_date <= @end_date;
END
ELSE 
BEGIN
	DELETE close_order from close_order where order_date >= @start_date and order_date <= @end_date and outlet_name = @outlet_name;
END
END
GO

CREATE PROCEDURE [dbo].[sp_delete_close_order_detail]
@outlet_name nvarchar(100) = NULL,
@start_date date,
@end_date date
AS
BEGIN
if (@outlet_name is NULL)
BEGIN
	DELETE close_order_detail from close_order_detail WITH(nolock) left join close_order WITH(nolock) on close_order.id = close_order_detail.id 
	where close_order_detail.order_date >= @start_date 
		and close_order_detail.order_date <= @end_date;
END
ELSE 
BEGIN
	DELETE close_order_detail from close_order_detail WITH(nolock) left join close_order WITH(nolock) on close_order.id = close_order_detail.id 
	where close_order_detail.order_date >= @start_date 
		and close_order_detail.order_date <= @end_date 
		and close_order.outlet_name = @outlet_name;
END
END
GO

CREATE PROCEDURE [dbo].[sp_delete_close_order_detail_orderitem]
@outlet_name nvarchar(100) = NULL,
@start_date date,
@end_date date
AS
BEGIN
if (@outlet_name is NULL)
BEGIN
	delete close_order_detail_orderitem 
	from close_order_detail_orderitem 
	left join close_order 
		on close_order_detail_orderitem.sales_order_id = close_order.id 
	where close_order.order_date >= @start_date and close_order.order_date <= @end_date;
END
ELSE 
BEGIN
	delete close_order_detail_orderitem 
	from close_order_detail_orderitem 
	left join close_order 
		on close_order_detail_orderitem.sales_order_id = close_order.id 
	where close_order.order_date >= @start_date and close_order.order_date <= @end_date and outlet_name = @outlet_name;
END
END
GO

CREATE PROCEDURE [dbo].[sp_delete_products_combo_detail_items]
@outlet_name nvarchar(100) = NULL
AS
BEGIN
if (@outlet_name is NULL)
BEGIN
DELETE products_combo_detail_items from products_combo_detail_items left join products_combo with(nolock) on products_combo.id = products_combo_detail_items.product_combo_id where products_combo.outlet_name = @outlet_name;
END
ELSE 
BEGIN
DELETE products_combo_detail_items from products_combo_detail_items left join products_combo with(nolock) on products_combo.id = products_combo_detail_items.product_combo_id;
END
END
GO

CREATE PROCEDURE [dbo].[sp_insert_close_order_detail_orderitem]
@outlet_name nvarchar(100),
@start_date date,
@end_date date
AS
BEGIN
INSERT INTO dbo.close_order_detail_orderitem
SELECT 
	JSON_VALUE(detail.value, '$.id') AS id,
    NULLIF(JSON_VALUE(detail.value, '$.addon_price'), '') AS addon_price,
    NULLIF(JSON_VALUE(detail.value, '$.amount'), '') AS amount,
    NULLIF(JSON_VALUE(detail.value, '$.cost_amount'), '') AS cost_amount,
    NULLIF(JSON_VALUE(detail.value, '$.cost_price'), '') AS cost_price,
    JSON_VALUE(detail.value, '$.created_by') AS created_by,
    JSON_VALUE(detail.value, '$.created_from') AS created_from,
    JSON_VALUE(detail.value, '$.created_time') AS created_time,
    JSON_VALUE(detail.value, '$.deal_id') AS deal_id,
    JSON_VALUE(detail.value, '$.deal_photo') AS deal_photo,
    JSON_VALUE(detail.value, '$.deal_photo_md') AS deal_photo_md,
    JSON_VALUE(detail.value, '$.deal_photo_xs') AS deal_photo_xs,
    JSON_VALUE(detail.value, '$.deal_title') AS deal_title,
    NULLIF(JSON_VALUE(detail.value, '$.discount'), '') AS discount,
    replace(NULLIF(JSON_VALUE(detail.value, '$.famount'), ''), '.', '') AS famount,
    NULLIF(JSON_VALUE(detail.value, '$.fdiscount'), '') AS fdiscount,
    NULLIF(JSON_VALUE(detail.value, '$.fprice'), '') AS fprice,
    NULLIF(JSON_VALUE(detail.value, '$.ftotal_weight'), '') AS ftotal_weight,
    NULLIF(JSON_VALUE(detail.value, '$.fweight'), '') AS fweight,
    NULLIF(JSON_VALUE(detail.value, '$.loyalty_points'), '') AS loyalty_points,
    JSON_VALUE(detail.value, '$.modified_by') AS modified_by,
    JSON_VALUE(detail.value, '$.modified_from') AS modified_from,
    NULLIF(JSON_VALUE(detail.value, '$.modified_time'), '0000-00-00 00:00:00') AS modified_time,
    JSON_VALUE(detail.value, '$.note') AS note,
    NULLIF(JSON_VALUE(detail.value, '$.order_with_serial'), '') AS order_with_serial,
    JSON_VALUE(detail.value, '$.photo_md') AS photo_md,
    JSON_VALUE(detail.value, '$.photo_variant_md') AS photo_variant_md,
    JSON_VALUE(detail.value, '$.photo_variant_xs') AS photo_variant_xs,
    JSON_VALUE(detail.value, '$.photo_xs') AS photo_xs,
    NULLIF(JSON_VALUE(detail.value, '$.price'), '') AS price,
    JSON_VALUE(detail.value, '$.product_combo_id') AS product_combo_id,
    JSON_VALUE(detail.value, '$.product_combo_name') AS product_combo_name,
    JSON_VALUE(detail.value, '$.product_combo_photo') AS product_combo_photo,
    JSON_VALUE(detail.value, '$.product_combo_photo_md') AS product_combo_photo_md,
    JSON_VALUE(detail.value, '$.product_combo_photo_xs') AS product_combo_photo_xs,
    JSON_VALUE(detail.value, '$.product_id') AS product_id,
    JSON_VALUE(detail.value, '$.product_name') AS product_name,
    JSON_VALUE(detail.value, '$.product_photo') AS product_photo,
    JSON_VALUE(detail.value, '$.product_sku') AS product_sku,
    JSON_VALUE(detail.value, '$.product_variant_id') AS product_variant_id,
    JSON_VALUE(detail.value, '$.product_variant_name') AS product_variant_name,
    JSON_VALUE(detail.value, '$.product_variant_photo') AS product_variant_photo,
    JSON_VALUE(detail.value, '$.product_variant_sku') AS product_variant_sku,
    JSON_VALUE(detail.value, '$.products') AS products,
    NULLIF(JSON_VALUE(detail.value, '$.qty'), '') AS qty,
    JSON_VALUE(detail.value, '$.sales_order_id') AS sales_order_id,
    JSON_VALUE(detail.value, '$.serial_no') AS serial_no,
    JSON_VALUE(detail.value, '$.status') AS status,
    NULLIF(JSON_VALUE(detail.value, '$.total_loyalty_points'), '') AS total_loyalty_points,
    JSON_VALUE(detail.value, '$.total_weight') AS total_weight,
    JSON_VALUE(detail.value, '$.weight') AS weight
FROM 
    dbo.close_order_detail
left join dbo.close_order
	on close_order_detail.id = close_order.id
CROSS APPLY 
    OPENJSON(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(CAST(close_order_detail.orderitems AS NVARCHAR(MAX)), 'NULL', 'null'), 'pedas""', 'pedas"'), ': p', ': "p'), 'pedas" ', 'pedas '), 'panas""', 'panas"'), 'None', 'null'), ': j', ': "j'), '"note": n', '"note": "n')) AS detail
where CAST(close_order_detail.order_date as date) >= @start_date 
	and CAST(close_order_detail.order_date as date) <= @end_date 
	and close_order.outlet_name = @outlet_name
END
GO

CREATE PROCEDURE [dbo].[sp_insert_products_combo_detail_items]
@outlet_name nvarchar(100)
AS
BEGIN
INSERT INTO dbo.products_combo_detail_items
select
	JSON_VALUE(detail1.value, '$.id') AS id,
    JSON_VALUE(detail1.value, '$.parent_id') AS parent_id,
    JSON_VALUE(detail1.value, '$.product_combo_id') AS product_combo_id,
    JSON_VALUE(detail1.value, '$.product_id') AS product_id,
    JSON_VALUE(detail1.value, '$.product_variant_id') AS product_variant_id,
    JSON_VALUE(detail1.value, '$.qty') AS qty,
    JSON_VALUE(detail1.value, '$.view_order') AS view_order,
    JSON_VALUE(detail1.value, '$.photo_md') AS photo_md,
    JSON_VALUE(detail1.value, '$.photo_xs') AS photo_xs,
    JSON_VALUE(detail1.value, '$.product_name') AS product_name,
    JSON_VALUE(detail1.value, '$.product_sku') AS product_sku,
    JSON_VALUE(detail1.value, '$.product_group_name') AS product_group_name,
    JSON_VALUE(detail1.value, '$.product_variant_name') AS product_variant_name,
    JSON_VALUE(detail1.value, '$.product_variant_sku') AS product_variant_sku,
    JSON_VALUE(detail1.value, '$.product_has_variant') AS product_has_variant
from dbo.products_combo_detail
left join dbo.products_combo
	on products_combo_detail.id = products_combo.id
CROSS APPLY 
    OPENJSON(REPLACE(REPLACE(CAST(products_combo_detail.items AS NVARCHAR(MAX)), 'NULL', 'null'), 'None', 'null')) AS detail
CROSS APPLY 
    OPENJSON(detail.value) AS detail1
where products_combo.outlet_name = @outlet_name
END
GO