-- Performance Analysis: Comparing the current value to a target value
-- Helps measure success and compare performance
-- Formula = Current[measure] - target[measure]

-- Task: Analyze the yearly performance of products by comparing each product's sales 
-- to both its average sales performance and the prevvious year's sales
WITH yearly_product_sales AS (
	SELECT 
	YEAR(f.order_date) as order_year,
	p.product_name,
	SUM(f.sales) as current_sales
	FROM Gold.fact_sales f
	LEFT JOIN Gold.dim_products p
	ON f.product_key = p.product_key
	WHERE f.order_date IS NOT NULL
	GROUP BY YEAR(f.order_date), p.product_name
)
SELECT 
order_year,
product_name,
current_sales,
AVG(current_sales) OVER(PARTITION BY product_name) as avg_sales,
current_sales - AVG(current_sales) OVER(PARTITION BY product_name) as diff_avg,
CASE  WHEN current_sales - AVG(current_sales) OVER(PARTITION BY product_name) > 0 THEN 'Above avg'
      WHEN current_sales - AVG(current_sales) OVER(PARTITION BY product_name) < 0 THEN 'Below avg'
	  ELSE 'Avg'
END avg_change,
-- YoY analysis
LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) as prev_year_sales,
current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) as prev_year_diff,
CASE  WHEN current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Increased'
      WHEN current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Decreased'
	  ELSE 'No change'
END avg_change
FROM yearly_product_sales
ORDER BY product_name, order_year 