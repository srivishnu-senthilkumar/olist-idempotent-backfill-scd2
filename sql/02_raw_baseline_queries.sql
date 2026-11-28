-- Baseline on raw CSV (before Parquet). Recorded: 16.84 MB scanned for orders.
SELECT COUNT(*) FROM olist_raw.orders;      -- 99441
SELECT COUNT(*) FROM olist_raw.customers;   -- 99441
SELECT order_status, COUNT(*) AS n
FROM olist_raw.orders GROUP BY order_status ORDER BY n DESC;
