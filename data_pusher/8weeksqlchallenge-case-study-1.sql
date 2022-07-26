CREATE TABLE IF NOT EXISTS  learn_sales (
  customer_id String,
  order_date Date,
  product_id Int8
)
ENGINE=MergeTree() ORDER BY customer_id,

INSERT INTO learn_sales
  (customer_id, order_date, product_id)
VALUES
  ('A', '2021-01-01', '1'),
  ('A', '2021-01-01', '2'),
  ('A', '2021-01-07', '2'),
  ('A', '2021-01-10', '3'),
  ('A', '2021-01-11', '3'),
  ('A', '2021-01-11', '3'),
  ('B', '2021-01-01', '2'),
  ('B', '2021-01-02', '2'),
  ('B', '2021-01-04', '1'),
  ('B', '2021-01-11', '1'),
  ('B', '2021-01-16', '3'),
  ('B', '2021-02-01', '3'),
  ('C', '2021-01-01', '3'),
  ('C', '2021-01-01', '3'),
  ('C', '2021-01-07', '3')

CREATE TABLE IF NOT EXISTS learn_menu
(
    `product_id` Int8,
    `product_name` String,
    `price` Int8
)
ENGINE = MergeTree
ORDER BY product_id

INSERT INTO learn_menu
  (product_id, product_name, price)
VALUES
  ('1', 'sushi', '10'),
  ('2', 'curry', '15'),
  ('3', 'ramen', '12')

CREATE TABLE IF NOT EXISTS learn_members
(
    `customer_id` String,
    `join_date` DATE
)
ENGINE = MergeTree
ORDER BY customer_id

INSERT INTO learn_members
  (customer_id, join_date)
VALUES
  ('A', '2021-01-07'),
  ('B', '2021-01-09')