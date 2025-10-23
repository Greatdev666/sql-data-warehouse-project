-- Identify the earliest and latest Dates (boundaries)
--Understand the scope of data and the timespan
-- Using MIN/MAX

SELECT 
MIN(order_date),
MAX(order_date),
DATEDIFF(year, MIN(order_date),  MAX(order_date))
FROM Gold.fact_sales

SELECT 
MIN(birth_date),
DATEDIFF(year, MIN(birth_date), GETDATE()),
MAX(birth_date),
DATEDIFF(year, MAX(birth_date), GETDATE())
FROM Gold.dim_customers