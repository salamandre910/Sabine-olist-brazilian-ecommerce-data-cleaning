-- ================================================================

--- TABLES DE DIMENSIONS ---

CREATE TABLE IF NOT EXISTS dim_customers (
  customer_id TEXT PRIMARY KEY,
  customer_unique_id TEXT,
  customer_zip_code_prefix TEXT,
  customer_city TEXT,
  customer_state TEXT
);

CREATE TABLE IF NOT EXISTS dim_products (
  product_id TEXT PRIMARY KEY,
    product_category_name TEXT,
    product_name_lenght INTEGER,
    product_description_lenght INTEGER,
    product_photos_qty INTEGER,
    product_weight_g INTEGER,
    product_length_cm INTEGER,
    product_height_cm INTEGER,
    product_width_cm INTEGER
);

CREATE TABLE IF NOT EXISTS dim_sellers (
  seller_id TEXT PRIMARY KEY,
    seller_zip_code_prefix TEXT,
    seller_city TEXT,
    seller_state TEXT
);

CREATE TABLE IF NOT EXISTS dim_date (
  date_id INTEGER PRIMARY KEY,   -- format YYYYMMDD
  date DATE,
  year INTEGER,
  month INTEGER,
  day INTEGER
);


-- ================================================================

-- TABLE DE FAITS ---

CREATE TABLE IF NOT EXISTS fact_order_items (
  order_id TEXT,
  order_item_id INTEGER,

  customer_id TEXT,
  product_id TEXT,
  seller_id TEXT,

  purchase_date_id INTEGER,
  delivered_date_id INTEGER,
  estimated_date_id INTEGER,

  price REAL,
  freight_value REAL,

  PRIMARY KEY (order_id, order_item_id),

  FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
  FOREIGN KEY (product_id) REFERENCES dim_products(product_id),
  FOREIGN KEY (seller_id) REFERENCES dim_sellers(seller_id),

  FOREIGN KEY (purchase_date_id) REFERENCES dim_date(date_id),
  FOREIGN KEY (delivered_date_id) REFERENCES dim_date(date_id),
  FOREIGN KEY (estimated_date_id) REFERENCES dim_date(date_id)
);

-- ================================================================