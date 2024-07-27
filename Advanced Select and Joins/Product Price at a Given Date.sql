-- Databricks notebook source
Table: Products

+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| product_id    | int     |
| new_price     | int     |
| change_date   | date    |
+---------------+---------+
(product_id, change_date) is the primary key (combination of columns with unique values) of this table.
Each row of this table indicates that the price of some product was changed to a new price at some date.
 

Write a solution to find the prices of all products on 2019-08-16. Assume the price of all products before any change is 10.

Return the result table in any order.

The result format is in the following example.

 

Example 1:

Input: 
Products table:
+------------+-----------+-------------+
| product_id | new_price | change_date |
+------------+-----------+-------------+
| 1          | 20        | 2019-08-14  |
| 2          | 50        | 2019-08-14  |
| 1          | 30        | 2019-08-15  |
| 1          | 35        | 2019-08-16  |
| 2          | 65        | 2019-08-17  |
| 3          | 20        | 2019-08-18  |
+------------+-----------+-------------+
Output: 
+------------+-------+
| product_id | price |
+------------+-------+
| 2          | 50    |
| 1          | 35    |
| 3          | 10    |
+------------+-------+

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm('dbfs:/user/hive/warehouse/products', True)

-- COMMAND ----------

-- Create table
CREATE TABLE IF NOT EXISTS Products (
    product_id INT,
    new_price INT,
    change_date DATE
);

-- Truncate table
TRUNCATE TABLE Products;

-- Insert data
INSERT INTO Products (product_id, new_price, change_date) VALUES (1, 20, '2019-08-14');
INSERT INTO Products (product_id, new_price, change_date) VALUES (2, 50, '2019-08-14');
INSERT INTO Products (product_id, new_price, change_date) VALUES (1, 30, '2019-08-15');
INSERT INTO Products (product_id, new_price, change_date) VALUES (1, 35, '2019-08-16');
INSERT INTO Products (product_id, new_price, change_date) VALUES (2, 65, '2019-08-17');
INSERT INTO Products (product_id, new_price, change_date) VALUES (3, 20, '2019-08-18');


-- COMMAND ----------

SELECT
  product_id,
  IFNULL (price, 10) AS price
FROM
  (
    SELECT DISTINCT
      product_id
    FROM
      Products
  ) AS UniqueProducts
  LEFT JOIN (
    SELECT DISTINCT
      product_id,
      FIRST_VALUE (new_price) OVER (
        PARTITION BY
          product_id
        ORDER BY
          change_date DESC
      ) AS price
    FROM
      Products
    WHERE
      change_date <= '2019-08-16'
  ) AS LastChangedPrice USING (product_id);

-- COMMAND ----------


with aa as (select * from Products where change_date < '2019-08-17')
select p1.product_id, p1.new_price as price from aa p1 join (select product_id, max(change_date) as eeee from aa group by product_id ) j1 
on p1.product_id= j1.product_id and p1.change_date = j1.eeee 
union 
select product_id, product_id *0 +10 as price from Products where change_date > '2019-08-16' and product_id not in ( select product_id from aa)

-- COMMAND ----------


