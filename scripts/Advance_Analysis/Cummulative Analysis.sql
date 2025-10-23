-- Cumulative Analysis
-- Calculate the total sales for each month and the running total of sales over time 
-- Second step
SELECT 
order_date,
total_sales,
-- window function, this will make our running total sales cos the default window 
-- frame is between unbounded preceding and current row
-- if we use partition by, it will only add running total of each year and reset when it gets to a new year
SUM(total_sales) OVER(ORDER BY order_date) running_total,
SUM(avg_price) OVER(ORDER BY order_date) moving_avg
FROM
(
	-- First step
	SELECT 
	DATETRUNC(YEAR,order_date) as order_date,
	SUM(sales) as total_sales,
	AVG(price) as avg_price
	FROM Gold.fact_sales
	WHERE order_date IS NOT NULL
	GROUP BY DATETRUNC(YEAR,order_date)
)t