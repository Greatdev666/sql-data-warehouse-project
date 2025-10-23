-- Just like ranking countries by total sales etc

-- Which five products generate that highest revenue 
SELECT TOP 5
p.product_name,
SUM(f.sales) AS total_revenue
FROM Gold.fact_sales f
LEFT JOIN Gold.dim_products p
ON f.product_key = p.product_key
GROUP BY p.product_name 
ORDER BY total_revenue DESC

-- Using window function
SELECT * 
FROM 
(
SELECT
	p.product_name,
	SUM(f.sales) AS total_revenue,
	ROW_NUMBER() OVER(ORDER BY SUM(f.sales) DESC) AS rank_products
	FROM Gold.fact_sales f
	LEFT JOIN Gold.dim_products p
	ON f.product_key = p.product_key
	GROUP BY p.product_name 
)t 
WHERE rank_products <= 5


-- What are the 5 worst-performing products in terms of sales ? 
SELECT TOP 5
p.product_name,
SUM(f.sales) AS total_revenue
FROM Gold.fact_sales f
LEFT JOIN Gold.dim_products p
ON f.product_key = p.product_key
GROUP BY p.product_name 
ORDER BY total_revenue ASC

-- Using window function
SELECT * 
FROM 
(
SELECT
	p.product_name,
	SUM(f.sales) AS total_revenue,
	ROW_NUMBER() OVER(ORDER BY SUM(f.sales) ASC) AS rank_products
	FROM Gold.fact_sales f
	LEFT JOIN Gold.dim_products p
	ON f.product_key = p.product_key
	GROUP BY p.product_name 
)t 
WHERE rank_products <= 5

-- Find top ten customers who have generated the highest revenue
SELECT TOP 10
c.customer_key,
c.first_name,
c.last_name,
SUM(f.sales) AS total_revenue
FROM Gold.fact_sales f
LEFT JOIN Gold.dim_customers C
ON f.customer_key = c.customer_key
GROUP BY c.customer_key,c.first_name, c.last_name 
ORDER BY total_revenue DESC


-- Find the 3 customerswith the fewest orders placed
SELECT TOP 3
c.customer_key,
c.first_name,
c.last_name,
COUNT(DISTINCT order_number) AS total_orders
FROM Gold.fact_sales f
LEFT JOIN Gold.dim_customers C
ON f.customer_key = c.customer_key
GROUP BY c.customer_key,c.first_name, c.last_name 
ORDER BY total_orders ASC