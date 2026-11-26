CREATE TABLE IF NOT EXISTS fakerdata (
  order_id TEXT,
  ordered_at TIMESTAMP,
  city TEXT,
  customer_name TEXT,
  address TEXT,
  item_category TEXT,
  order_amount_vnd INT,
  payment_method TEXT,
  delivery_minutes INT,
  customer_rating INT,
  PRIMARY KEY (city, ordered_at, order_id)
);