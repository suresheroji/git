-- Databricks notebook source
Table: Triangle

+-------------+------+
| Column Name | Type |
+-------------+------+
| x           | int  |
| y           | int  |
| z           | int  |
+-------------+------+
In SQL, (x, y, z) is the primary key column for this table.
Each row of this table contains the lengths of three line segments.
 

Report for every three line segments whether they can form a triangle.

Return the result table in any order.

The result format is in the following example.

 

Example 1:

Input: 
Triangle table:
+----+----+----+
| x  | y  | z  |
+----+----+----+
| 13 | 15 | 30 |
| 10 | 20 | 15 |
+----+----+----+
Output: 
+----+----+----+----------+
| x  | y  | z  | triangle |
+----+----+----+----------+
| 13 | 15 | 30 | No       |
| 10 | 20 | 15 | Yes      |
+----+----+----+----------+

-- COMMAND ----------

-- Create table
CREATE TABLE IF NOT EXISTS Triangle (
    x INT,
    y INT,
    z INT
);

-- Truncate table
TRUNCATE TABLE Triangle;

-- Insert data
INSERT INTO Triangle (x, y, z) VALUES (13, 15, 30);
INSERT INTO Triangle (x, y, z) VALUES (10, 20, 15);


-- COMMAND ----------

select 
x,y,z,
case when (x+y) > z and (x+z) > y and (y+z) > x then 'Yes' else 'No' end as triangle
from Triangle 
