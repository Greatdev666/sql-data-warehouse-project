-- Proportional Analysis Part to whole: Analyze how an individual part is performing compared to the overall, 
-- allowing us to understand which category has the  greatest impact on the business
-- ([Measure] / [Total[measure]) * 100 By [Dimension]
-- which categories contribute the most to overall sales ?
WITH category_sales AS (
SELECT 
p.category,
SUM(f.sales) as total_sales
FROM Gold.fact_sales f
LEFT JOIN Gold.dim_products p
ON f.product_key = p.product_key
GROUP BY p.category
)

SELECT 
category,
total_sales, 
SUM(total_sales) OVER() overall_sales,
CONCAT(ROUND((CAST (total_sales AS FLOAT) / SUM(total_sales) OVER()) * 100, 2), '%') as percentage_of_total 
FROM category_sales
ORDER BY total_sales DESC