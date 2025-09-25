DELETE FROM dwh.rejected_orders;
DELETE FROM dwh.dq_issues;

-- Orders without items
INSERT INTO dwh.rejected_orders (order_id, reason)
SELECT o.id, 'NO_ITEMS'
FROM stg.orders o
LEFT JOIN stg.order_items oi ON oi.order_id = o.id
WHERE oi.order_id IS NULL
ON CONFLICT (order_id) DO NOTHING;

INSERT INTO dwh.dq_issues (entity, entity_id, issue_code, detail)
SELECT 'order', o.id, 'NO_ITEMS', 'Order has no items'
FROM stg.orders o
LEFT JOIN stg.order_items oi ON oi.order_id = o.id
WHERE oi.order_id IS NULL;

-- Invalid currencies (order and item)
INSERT INTO dwh.dq_issues (entity, entity_id, issue_code, detail)
SELECT 'order', id, 'INVALID_ORDER_CURRENCY', currency
FROM stg.orders
WHERE UPPER(currency) NOT IN ('USD','EUR','GBP');

INSERT INTO dwh.dq_issues (entity, entity_id, issue_code, detail)
SELECT 'order_item', id, 'INVALID_ITEM_CURRENCY', currency
FROM stg.order_items
WHERE UPPER(currency) NOT IN ('USD','EUR','GBP');

-- Order vs item mismatch
INSERT INTO dwh.dq_issues (entity, entity_id, issue_code, detail)
SELECT 'order_item', oi.id, 'CURRENCY_MISMATCH', 'order='||o.currency||' item='||oi.currency
FROM stg.order_items oi
JOIN stg.orders o ON o.id = oi.order_id
WHERE UPPER(oi.currency) <> UPPER(o.currency);

-- Items whose product_id does not exist in catalog
INSERT INTO dwh.dq_issues (entity, entity_id, issue_code, detail)
SELECT 'order_item', oi.id, 'MISSING_PRODUCT', 'product_id='||oi.product_id
FROM stg.order_items oi
LEFT JOIN stg.product_descriptions p ON p.id = oi.product_id
WHERE p.id IS NULL;

-- Items without a product that, if no other issues were present, would qualify for the fact
INSERT INTO dwh.dq_issues (entity, entity_id, issue_code, detail)
SELECT 'order_item', oi.id, 'MISSING_PRODUCT_ELIGIBLE', 'product_id='||oi.product_id
FROM stg.order_items oi
JOIN stg.orders o ON o.id = oi.order_id
LEFT JOIN stg.product_descriptions p ON p.id = oi.product_id
WHERE p.id IS NULL
  AND oi.quantity > 0
  AND UPPER(oi.currency) IN ('USD','EUR','GBP')
  AND NOT EXISTS (SELECT 1 FROM dwh.rejected_orders r WHERE r.order_id = o.id);