-- Calculate the key metric of the business (BIG NUMBERS) 
-- Highest Level of Aggregation | Lowest Level Details 


 --------Measures to be calculated
  -- Find the Total Sales
 -- Find how many items are sold 
 -- Find the average selling price 
 -- Find the Total number of Orders
 -- Find the Total number of Products
 -- Find the Total number of Customers
 -- Find the Total number of Customers that has placed an order


  -- Find the Total Sales
  SELECT SUM(sales) AS total_sales FROM Gold.fact_sales 

 -- Find how many items are sold 
 SELECT SUM(quantity) AS total_quantity FROM Gold.fact_sales 

 -- Find the average selling price 
 SELECT AVG(price) AS avg_price FROM Gold.fact_sales 

 -- Find the Total number of Orders
 SELECT COUNT(order_number) AS total_order FROM Gold.fact_sales
 SELECT COUNT(DISTINCT order_number) AS total_order FROM Gold.fact_sales
 -- Find the Total number of Products
 SELECT COUNT(product_name) AS total_product FROM Gold.dim_products
 SELECT COUNT(DISTINCT product_name) AS total_order FROM Gold.dim_products

 -- Find the Total number of Customers
SELECT COUNT(customer_key) AS total_product FROM Gold.dim_customers

 -- Find the Total number of Customers that has placed an order
SELECT COUNT(DISTINCT customer_key) AS total_product FROM Gold.fact_sales

 -- Generate a report that shows all key metrics of the business
   SELECT 'Total Sales' AS measure_name, SUM(sales) AS measure_value FROM Gold.fact_sales 
   UNION ALL
   SELECT 'Total Quantity' AS measure_name, SUM(quantity) AS measure_value FROM Gold.fact_sales
   UNION ALL 
   SELECT 'Avg Price' AS measure_name, AVG(price) AS measure_value FROM Gold.fact_sales
   UNION ALL
   SELECT 'Total No OF Orders' AS measure_name, COUNT(DISTINCT order_number) AS measure_value FROM Gold.fact_sales
   UNION ALL
   SELECT 'Total No OF Products' AS measure_name, COUNT(DISTINCT product_name) AS measure_value FROM Gold.dim_products
   UNION ALL 
   SELECT 'Total No OF Products' AS measure_name, COUNT(customer_key) AS measure_value FROM Gold.dim_customers
   UNION ALL 
   SELECT 'No Of Customers with Orders' AS measure_name, COUNT(DISTINCT customer_key) AS measure_value FROM Gold.fact_sales