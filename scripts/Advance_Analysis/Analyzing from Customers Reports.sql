SELECT * FROM Gold.customers_reports

SELECT 
customer_segments,
COUNT(customer_number) AS total_customers,
SUM(total_sales)
FROM Gold.customers_reports
GROUP BY customer_segments

SELECT * FROM Gold.product_reports