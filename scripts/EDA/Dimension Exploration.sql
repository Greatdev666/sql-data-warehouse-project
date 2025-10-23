-- Its mostly about checking uniqueness using using distinct 
SELECT * FROM Gold.dim_customers

SELECT DISTINCT country 
FROM Gold.dim_customers

SELECT DISTINCT category  
FROM Gold.dim_products

SELECT DISTINCT category, subcategory, product_name
FROM Gold.dim_products
ORDER BY 1,2,3