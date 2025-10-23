/*
=======================================================================
Customer Report
=======================================================================
Purpose: 
		This report consolidates key customer metrics and behaviors
Highlights: 
		1. Gathers essential fields such as product name, category, subcategory and cost
		2. Segments products by revenue to identify High-Performer, Mid-Range or Low-Performers.
		3. Aggregates product-level metrics:
		   - total orders
		   - total sales
		   - total quantity sold
		   - total customers (unique)
		   - lifespan (in months)
		4. Calculate valuable KPIs
		   - recencey (months since last sale)
		   - average order revenue (ADR) 
		   - average monthly revenue
============================================================================
*/
CREATE VIEW Gold.product_reports AS 
WITH base_query AS (
/* ---------------------------------------------------------------------
1) Base Query: Retrieve core columns from tables
----------------------------------------------------------------------- */

SELECT
    p.product_key,
	p.product_name,
	p.category,
	p.subcategory,
	p.product_cost,
	f.customer_key,
	f.sales,
	f.quantity,
	f.order_number,
	f.order_date
FROM Gold.fact_sales f
LEFT JOIN Gold.dim_products p
ON f.product_key = p.product_key
WHERE order_date IS NOT NULL 
)

, product_aggregation AS (
	SELECT 
	    product_key,
		product_name,
		category,
		subcategory,
		product_cost,
		DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan,
		MAX(order_date) AS last_sale_date,
		COUNT(DISTINCT order_number) AS total_orders,
		COUNT(DISTINCT customer_key) AS total_customers,
		SUM(sales) AS total_sales,
		SUM(quantity) AS total_quantity,
		ROUND(AVG(CAST(sales AS FLOAT) / NULLIF(quantity, 0)), 1) AS avg_selling_price
	FROM base_query
	GROUP BY
	product_key,
	product_name,
	category,
	subcategory,
	product_cost

)

SELECT 
product_key,
product_name,
category,
subcategory,
product_cost,
total_sales,
last_sale_date,
DATEDIFF(Month, last_sale_date, GETDATE()) AS recency_in_months,
CASE WHEN total_sales < 100000 THEN 'Low-Performer'
     WHEN total_sales BETWEEN 100000 AND 500000 THEN 'Mid-Range'
	 ELSE 'High Performer'
END product_revenue_segments,
total_orders,
total_quantity,
total_customers,
lifespan,
-- Compute average order revenue (AVR)
CASE WHEN total_sales = 0 THEN 0 -- to avoid dividing by 0
	 ELSE total_sales / total_orders 
END AS aveg_order_revenue,
-- Compute average monthly revenue
CASE WHEN lifespan  = 0 THEN total_sales
	 ELSE total_sales / lifespan
END AS avg_monthly_revenue
FROM product_aggregation 
