create database Olist_project;
USE olist_project;

CREATE TABLE olist_customers_dataset (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix VARCHAR(10),
    customer_city VARCHAR(100),
    customer_state CHAR(2));
SET GLOBAL local_infile = 1;
LOAD DATA LOCAL INFILE 'C:/Users/Tummi/OneDrive/Data analyst course/Projects/Olist_ E-Commerce Project/olist_customers_dataset.csv'
INTO TABLE olist_customers_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;
Select * from Olist_customers_dataset;
SELECT COUNT(*) FROM olist_customers_dataset;


CREATE TABLE olist_geolocation_dataset (
    geolocation_zip_code_prefix VARCHAR(10),
    geolocation_lat DECIMAL(10,6),
    geolocation_lng DECIMAL(10,6),
    geolocation_city VARCHAR(100),
    geolocation_state CHAR(2),
    PRIMARY KEY (geolocation_zip_code_prefix));
SET GLOBAL local_infile = 1;
LOAD DATA LOCAL INFILE 'C:/Users/Tummi/OneDrive/Data analyst course/Projects/Olist_ E-Commerce Project/olist_geolocation_dataset.csv'
INTO TABLE olist_geolocation_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;
select * from olist_geolocation_dataset;
SELECT COUNT(*) FROM olist_geolocation_dataset;


create table olist_order_items (
	order_id varchar(50) primary key,
	order_item_id varchar(50),
	product_id varchar(50),
	seller_id varchar(50),
	shipping_limit_date date,
	price float,
	freight_value float);
  SET GLOBAL local_infile = 1;  
LOAD DATA LOCAL INFILE 'C:/Users/Tummi/OneDrive/Data analyst course/Projects/Olist_ E-Commerce Project/olist_order_items_dataset.csv'
INTO TABLE olist_order_items
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;
select * from olist_order_items;
SELECT COUNT(*) FROM olist_order_items;

CREATE TABLE olist_order_payments (
    order_id VARCHAR(50) PRIMARY KEY,
    payment_sequential INT,
    payment_type VARCHAR(20),
    payment_installments INT,
    payment_value DECIMAL(10,2));
SET GLOBAL local_infile = 1;  
LOAD DATA LOCAL INFILE 'C:/Users/Tummi/OneDrive/Data analyst course/Projects/Olist_ E-Commerce Project/olist_order_payments_dataset.csv'
INTO TABLE olist_order_payments
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;
select * from olist_order_Payments;
SELECT COUNT(*) FROM olist_order_payments;


CREATE TABLE reviews_staging (
    review_id VARCHAR(50),
    order_id VARCHAR(50),
    review_score INT,
    review_comment_title VARCHAR(255),
    review_comment_message TEXT,
    review_creation_date VARCHAR(10),
    review_answer_timestamp VARCHAR(10)
);
LOAD DATA LOCAL INFILE 'C:/Users/Tummi/OneDrive/Data analyst course/Projects/Olist_ E-Commerce Project/olist_order_reviews_dataset.csv'
INTO TABLE reviews_staging
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

SELECT review_id, COUNT(*)
FROM reviews_staging
GROUP BY review_id
HAVING COUNT(*) > 1;

CREATE TABLE reviews_deduped AS
SELECT
    review_id,
    MIN(order_id) AS order_id,
    MIN(review_score) AS review_score,
    MIN(review_comment_title) AS review_comment_title,
    MIN(review_comment_message) AS review_comment_message,
    MIN(review_creation_date) AS review_creation_date,
    MIN(review_answer_timestamp) AS review_answer_timestamp
FROM reviews_staging
GROUP BY review_id;

INSERT INTO olist_order_reviews (
    review_id, order_id, review_score,
    review_comment_title, review_comment_message,
    review_creation_date, review_answer_timestamp
)
SELECT
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    STR_TO_DATE(review_creation_date, '%d-%m-%Y'),
    STR_TO_DATE(review_answer_timestamp, '%d-%m-%Y')
FROM reviews_deduped
WHERE NOT EXISTS (
    SELECT 1 FROM olist_order_reviews r WHERE r.review_id = reviews_deduped.review_id);
    
DROP TABLE reviews_staging;
RENAME TABLE reviews_deduped TO reviews_staging;
select * from olist_order_reviews;
SELECT COUNT(*) FROM olist_order_reviews;



CREATE TABLE orders_staging (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_status VARCHAR(20),
    order_purchase_timestamp VARCHAR(25),
    order_approved_at VARCHAR(25),
    order_delivered_carrier_date VARCHAR(25),
    order_delivered_customer_date VARCHAR(25),
    order_estimated_delivery_date VARCHAR(25));
SET GLOBAL local_infile = 1;  
LOAD DATA LOCAL INFILE 'C:/Users/Tummi/OneDrive/Data analyst course/Projects/Olist_ E-Commerce Project/olist_orders_dataset.csv'
INTO TABLE orders_staging
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

CREATE TABLE olist_orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    order_status VARCHAR(20),
    order_purchase_timestamp DATE,
    order_approved_at DATE,
    order_delivered_carrier_date DATE,
    order_delivered_customer_date DATE,
    order_estimated_delivery_date DATE);

INSERT INTO olist_orders (
    order_id, customer_id, order_status,
    order_purchase_timestamp, order_approved_at,
    order_delivered_carrier_date, order_delivered_customer_date,
    order_estimated_delivery_date)
SELECT
    order_id,
    customer_id,
    order_status,
    CASE WHEN TRIM(order_purchase_timestamp) = '' THEN NULL
         ELSE STR_TO_DATE(SUBSTRING_INDEX(order_purchase_timestamp, ' ', 1), '%d-%m-%Y') END,
    CASE WHEN TRIM(order_approved_at) = '' THEN NULL
         ELSE STR_TO_DATE(SUBSTRING_INDEX(order_approved_at, ' ', 1), '%d-%m-%Y') END,
    CASE WHEN TRIM(order_delivered_carrier_date) = '' THEN NULL
         ELSE STR_TO_DATE(SUBSTRING_INDEX(order_delivered_carrier_date, ' ', 1), '%d-%m-%Y') END,
    CASE WHEN TRIM(order_delivered_customer_date) = '' THEN NULL
         ELSE STR_TO_DATE(SUBSTRING_INDEX(order_delivered_customer_date, ' ', 1), '%d-%m-%Y') END,
    CASE WHEN TRIM(order_estimated_delivery_date) = '' THEN NULL
         ELSE STR_TO_DATE(SUBSTRING_INDEX(order_estimated_delivery_date, ' ', 1), '%d-%m-%Y') END
FROM orders_staging;
select * from olist_orders;
SELECT COUNT(*) FROM olist_orders;

SELECT count(*)
FROM olist_orders
WHERE
    order_purchase_timestamp IS NULL OR
    order_approved_at IS NULL OR
    order_delivered_carrier_date IS NULL OR
    order_delivered_customer_date IS NULL OR
    order_estimated_delivery_date IS NULL;
    
CREATE TABLE olist_products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_category_name VARCHAR(50),
    product_name_length INT,
    product_description_length INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT);
LOAD DATA LOCAL INFILE 'C:/Users/Tummi/OneDrive/Data analyst course/Projects/Olist_ E-Commerce Project/olist_products_dataset.csv'
INTO TABLE olist_products
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS; 
select * from olist_products;
SELECT COUNT(*) FROM olist_products;


CREATE TABLE olist_sellers (
    seller_id VARCHAR(50) PRIMARY KEY,
    seller_zip_code_prefix INT,
    seller_city VARCHAR(100),
    seller_state CHAR(2));
LOAD DATA LOCAL INFILE 'C:/Users/Tummi/OneDrive/Data analyst course/Projects/Olist_ E-Commerce Project/olist_sellers_dataset.csv'
INTO TABLE olist_sellers
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS; 
select * from olist_sellers;
SELECT COUNT(*) FROM olist_sellers;


CREATE TABLE product_category_translation (
    product_category_name VARCHAR(50) PRIMARY KEY,
    product_category_name_english VARCHAR(50));
LOAD DATA LOCAL INFILE 'C:/Users/Tummi/OneDrive/Data analyst course/Projects/Olist_ E-Commerce Project/product_category_name_translation.csv'
INTO TABLE product_category_translation
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS; 
select * from product_category_translation;
SELECT COUNT(*) FROM product_category_translation;


CREATE TABLE product_categories (
    product_id VARCHAR(50) PRIMARY KEY,
    product_category_name VARCHAR(50));
LOAD DATA LOCAL INFILE 'C:/Users/Tummi/OneDrive/Data analyst course/Projects/Olist_ E-Commerce Project/products_dataset.csv'
INTO TABLE product_categories
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS; 
select * from product_categories;
SELECT COUNT(*) FROM product_categories;


Select * from Olist_customers_dataset;
Select * from olist_geolocation_dataset;
Select * from Olist_orders;
Select * from Olist_order_payments;
Select * from olist_order_reviews;
Select * from olist_order_items;
Select * from olist_products;
Select * from olist_sellers;
Select * from product_categories;
Select * from product_category_translation;

### Weekday vs Weekend 
SELECT COUNT(*) FROM olist_orders;
SELECT COUNT(*) FROM olist_order_payments;

SELECT
  CASE
    WHEN WEEKDAY(order_purchase_timestamp) IN (5, 6) THEN 'Weekend'
    ELSE 'Weekday'
  END AS day_type,
  COUNT(*) AS total_orders,
  SUM(payment_value) AS total_payment,
  AVG(payment_value) AS avg_payment
FROM olist_orders o
JOIN olist_order_payments p ON o.order_id = p.order_id
GROUP BY day_type;
#### Along with percentage 
WITH payment_stats AS (
  SELECT
    CASE
      WHEN WEEKDAY(o.order_purchase_timestamp) IN (5, 6) THEN 'Weekend'
      ELSE 'Weekday'
    END AS day_type,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(p.payment_value) AS total_payment,
    AVG(p.payment_value) AS avg_payment
  FROM Olist_orders o
  JOIN Olist_order_payments p ON o.order_id = p.order_id
  GROUP BY day_type
),
total AS (
  SELECT SUM(total_payment) AS grand_total FROM payment_stats
)
SELECT
  ps.day_type,
  ps.total_orders,
  ps.total_payment,
  ps.avg_payment,
  ROUND((ps.total_payment / t.grand_total) * 100, 2) AS payment_percentage
FROM payment_stats ps, total t;

###Number of Orders with review score 5 and payment type as credit card.
SELECT COUNT(DISTINCT o.order_id) AS credit_card_5star_orders
FROM Olist_orders o
JOIN olist_order_reviews r ON o.order_id = r.order_id
JOIN Olist_order_payments p ON o.order_id = p.order_id
WHERE r.review_score = 5
  AND p.payment_type = 'credit_card';
  
###Average number of days taken for order_delivered_customer_date for pet_shop
SELECT
  ROUND(AVG(DATEDIFF(o.order_delivered_customer_date, o.order_purchase_timestamp)), 0) AS avg_delivery_days
FROM Olist_orders o
JOIN olist_order_items i ON o.order_id = i.order_id
JOIN olist_products p ON i.product_id = p.product_id
WHERE p.product_category_name = 'pet_shop'
  AND o.order_delivered_customer_date IS NOT NULL;
  
  ###Average price and payment values from customers of sao paulo city
SELECT
  ROUND(AVG(oi.price), 2) AS avg_item_price,
  ROUND(AVG(op.payment_value), 2) AS avg_payment_value
FROM Olist_customers_dataset c
JOIN Olist_orders o ON c.customer_id = o.customer_id
JOIN olist_order_items oi ON o.order_id = oi.order_id
JOIN Olist_order_payments op ON o.order_id = op.order_id
WHERE c.customer_city = 'sao paulo';

###Relationship between shipping days (order_delivered_customer_date - order_purchase_timestamp) Vs review scores.
SELECT
  r.review_score,
  ROUND(AVG(DATEDIFF(o.order_delivered_customer_date, o.order_purchase_timestamp)), 0) AS avg_shipping_days,
  COUNT(*) AS total_reviews
FROM Olist_orders o
JOIN olist_order_reviews r ON o.order_id = r.order_id
WHERE o.order_delivered_customer_date IS NOT NULL
GROUP BY r.review_score
ORDER BY r.review_score;

