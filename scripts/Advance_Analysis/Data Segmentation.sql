-- Data Segmentation
-- [Measure] / [Measure]
/* Segment products into cost ranges and Count how many products fall into each segment */
WITH product_segment AS  (
SELECT 
product_key,
product_name,
product_cost,
CASE WHEN product_cost < 100 THEN 'Below 100'
	 WHEN product_cost BETWEEN 100 AND 500 THEN '100-500'
	 WHEN product_cost BETWEEN 500 AND 1000 THEN '500-1000'
	 ELSE 'Above 1000'
END cost_range
FROM Gold.dim_products
)

SELECT 
cost_range,
COUNT(product_key) AS total_products
FROM product_segment 
GROUP BY cost_range 
ORDER BY total_products DESC;

/* Group customers into three segments based on their spending behavior;
- VIP: at least 12 months of history and spending more than 5,000 euro
- Regular: at least 12 months of history but spending <= 5,000
- New: lifespan less than 12 months
  And find the total number of customers by each group 
*/
WITH customer_spending AS (
SELECT 
c.customer_key,
SUM(f.sales) AS total_spending,
-- MIN(order_date) AS first_order,
-- MAX(order_date) AS last_order,
DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan
FROM Gold.fact_sales f
LEFT JOIN Gold.dim_customers c
ON f.customer_key = c.customer_key
GROUP BY c.customer_key
) 
SELECT 
customer_segments,
COUNT(customer_key) AS total_customers
FROM (
	SELECT 
	customer_key,
	CASE WHEN lifespan >= 12 AND total_spending > 5000 THEN 'VIP'
		 WHEN lifespan >= 12 AND total_spending <= 5000 THEN 'Regular'
		 ELSE 'New'
	END customer_segments
	FROM customer_spending) t
GROUP BY customer_segments
ORDER BY total_customers DESC