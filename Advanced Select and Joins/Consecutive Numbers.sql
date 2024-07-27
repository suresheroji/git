-- Databricks notebook source
Table: Logs

+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| id          | int     |
| num         | varchar |
+-------------+---------+
In SQL, id is the primary key for this table.
id is an autoincrement column.
 

Find all numbers that appear at least three times consecutively.

Return the result table in any order.

The result format is in the following example.

 

Example 1:

Input: 
Logs table:
+----+-----+
| id | num |
+----+-----+
| 1  | 1   |
| 2  | 1   |
| 3  | 1   |
| 4  | 2   |
| 5  | 1   |
| 6  | 2   |
| 7  | 2   |
+----+-----+
Output: 
+-----------------+
| ConsecutiveNums |
+-----------------+
| 1               |
+-----------------+
Explanation: 1 is the only number that appears consecutively for at least three times.

-- COMMAND ----------

-- Create table
CREATE TABLE IF NOT EXISTS Logs (
    id INT,
    num INT
);

-- Truncate table
TRUNCATE TABLE Logs;

-- Insert data
INSERT INTO Logs (id, num) VALUES (1, 1);
INSERT INTO Logs (id, num) VALUES (2, 1);
INSERT INTO Logs (id, num) VALUES (3, 1);
INSERT INTO Logs (id, num) VALUES (4, 2);
INSERT INTO Logs (id, num) VALUES (5, 1);
INSERT INTO Logs (id, num) VALUES (6, 2);
INSERT INTO Logs (id, num) VALUES (7, 2);


-- COMMAND ----------

WITH cte AS (
    SELECT
        num,
        LAG(num) OVER (ORDER BY id) AS prev_num,
        LEAD(num) OVER (ORDER BY id) AS next_num
    FROM Logs
)

SELECT num AS ConsecutiveNums
FROM cte
WHERE num = prev_num AND num = next_num
GROUP BY num;


-- COMMAND ----------


