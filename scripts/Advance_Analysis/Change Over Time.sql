-- Trends Over Time

SELECT 
YEAR(order_date)order_year,
SUM(sales) as total_sales,
COUNT(DISTINCT customer_key) AS total_customers,
SUM(quantity) AS total_quantity
FROM Gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date)
ORDER BY YEAR(order_date)


SELECT 
YEAR(order_date) order_year,
MONTH(order_date) order_month,
SUM(sales) as total_sales,
COUNT(DISTINCT customer_key) AS total_customers,
SUM(quantity) AS total_quantity
FROM Gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date),MONTH(order_date)
ORDER BY YEAR(order_date),MONTH(order_date)

-- Instead of using month and year separately we ccan use the Datetrunc
-- DateTrunc: Rounds a date or timestamp to a specified date part
SELECT 
DATETRUNC(month,order_date)order_year,
SUM(sales) as total_sales,
COUNT(DISTINCT customer_key) AS total_customers,
SUM(quantity) AS total_quantity
FROM Gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(month,order_date)
ORDER BY DATETRUNC(month,order_date)

--using format, BUT note that you cant sort it cos data type will be string
SELECT 
FORMAT(order_date, 'yyyy-MMM')order_year,
SUM(sales) as total_sales,
COUNT(DISTINCT customer_key) AS total_customers,
SUM(quantity) AS total_quantity
FROM Gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY FORMAT(order_date, 'yyyy-MMM')
ORDER BY FORMAT(order_date, 'yyyy-MMM')