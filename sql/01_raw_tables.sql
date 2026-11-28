-- Raw layer: hand-defined CSV tables (OpenCSVSerde strips quotes, skips header).
-- Replace <your-bucket> with your S3 bucket name before running in Athena.
-- Note: the Glue crawler misread the CSV header, so these are defined manually.

DROP TABLE IF EXISTS olist_raw.orders;
DROP TABLE IF EXISTS olist_raw.customers;

CREATE EXTERNAL TABLE olist_raw.orders (
  order_id string,
  customer_id string,
  order_status string,
  order_purchase_timestamp string,
  order_approved_at string,
  order_delivered_carrier_date string,
  order_delivered_customer_date string,
  order_estimated_delivery_date string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar' = '\\')
STORED AS TEXTFILE
LOCATION 's3://<your-bucket>/raw/orders/'
TBLPROPERTIES ('skip.header.line.count' = '1');

CREATE EXTERNAL TABLE olist_raw.customers (
  customer_id string,
  customer_unique_id string,
  customer_zip_code_prefix string,
  customer_city string,
  customer_state string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar' = '\\')
STORED AS TEXTFILE
LOCATION 's3://<your-bucket>/raw/customers/'
TBLPROPERTIES ('skip.header.line.count' = '1');
