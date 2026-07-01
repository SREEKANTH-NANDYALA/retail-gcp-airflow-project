CREATE SCHEMA IF NOT EXISTS `my-retail-project-123456.retail_dw`;

CREATE OR REPLACE TABLE `my-retail-project-123456.retail_dw.orders_raw` (
  order_id STRING,
  customer_id STRING,
  customer_name STRING,
  email STRING,
  product_id STRING,
  product_name STRING,
  category STRING,
  quantity INT64,
  price FLOAT64,
  order_status STRING,
  order_date DATE,
  updated_at TIMESTAMP,
  ingestion_date TIMESTAMP
);

CREATE OR REPLACE TABLE `my-retail-project-123456.retail_dw.orders_staging` (
  order_id STRING,
  customer_id STRING,
  customer_name STRING,
  email STRING,
  product_id STRING,
  product_name STRING,
  category STRING,
  quantity INT64,
  price FLOAT64,
  order_status STRING,
  updated_at TIMESTAMP,
  order_date DATE
);

CREATE OR REPLACE TABLE `my-retail-project-123456.retail_dw.new_orders` (
  order_id STRING,
  customer_id STRING,
  customer_name STRING,
  email STRING,
  product_id STRING,
  product_name STRING,
  category STRING,
  quantity INT64,
  price FLOAT64,
  order_status STRING,
  order_date DATE,
  updated_at TIMESTAMP,
  detected_at TIMESTAMP
);