/*
===============================================================================
Project         : Supply Chain Analytics Platform
Module          : Executive Supply Chain Dashboard
Description     : End-to-end supply chain dashboard combining procurement,
                  production, quality, warehouse, logistics, orders and sales.
Author          : Celebal Technologies
===============================================================================
*/

USE CATALOG eagleeyelakebase_uc;

USE SCHEMA supply_chain_demo;

-- ============================================================================
-- Executive Supply Chain Dashboard
-- ============================================================================

SELECT

    pb.batch_id,
    pb.production_date,

    pm.plant_id,
    pm.plant_name,
    pm.city,
    pm.state,

    sm.supplier_id,
    sm.supplier_name,
    sm.supplier_rating,

    rmi.material_name,
    rmi.quantity_available,

    po.purchase_order_id,
    po.total_amount AS purchase_amount,

    qr.quality_score,
    qr.defect_count,

    we.warehouse_id,
    we.storage_location,

    sd.shipment_id,
    sd.shipment_status,
    sd.shipping_cost,

    co.customer_order_id,
    co.customer_id,
    co.order_amount,

    sf.sales_id,
    sf.sales_amount,
    sf.profit_amount

FROM production_batch pb

LEFT JOIN plant_master pm
ON pb.plant_id = pm.plant_id

LEFT JOIN quality_results qr
ON pb.batch_id = qr.batch_id

LEFT JOIN warehouse_entry we
ON pb.batch_id = we.batch_id

LEFT JOIN raw_material_inventory rmi
ON we.material_id = rmi.material_id

LEFT JOIN purchase_orders po
ON rmi.material_id = po.material_id

LEFT JOIN supplier_master sm
ON po.supplier_id = sm.supplier_id

LEFT JOIN customer_orders co
ON co.product_id = pb.product_id

LEFT JOIN shipment_details sd
ON co.customer_order_id = sd.customer_order_id

LEFT JOIN sales_fact sf
ON co.customer_order_id = sf.customer_order_id

ORDER BY
    pb.production_date DESC,
    sf.sales_amount DESC;