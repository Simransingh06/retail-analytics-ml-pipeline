import sqlite3
import pandas as pd

print("Starting SQL Analytics Pipeline...")

conn = sqlite3.connect("online_retail_clean.db")

# -------------------------
# SALES SUMMARY
# -------------------------

sales_summary_query = """
SELECT
    customer_id,
    invoice,
    description,
    country,
    year,
    month,
    week,
    price,
    quantity AS total_quantity,
    quantity * price AS total_revenue,
    invoicedate
FROM retail_data
WHERE quantity > 0
"""

sales_summary = pd.read_sql_query(sales_summary_query, conn)

# -------------------------
# TOP PRODUCTS
# -------------------------

products_query = """
SELECT
    stockcode,
    description,
    SUM(quantity) AS total_quantity_sold,
    SUM(quantity * price) AS total_revenue
FROM retail_data
WHERE quantity > 0
GROUP BY stockcode, description
ORDER BY total_quantity_sold DESC
LIMIT 20
"""

top_products = pd.read_sql_query(products_query, conn)

# -------------------------
# CUSTOMER ANALYTICS
# -------------------------

customer_query = """
SELECT
    customer_id,
    SUM(quantity * price) AS total_spent,
    COUNT(DISTINCT invoice) AS total_orders,
    MAX(invoicedate) AS last_purchase_date
FROM retail_data
WHERE quantity > 0
GROUP BY customer_id
ORDER BY total_spent DESC
"""

customer_activity = pd.read_sql_query(customer_query, conn)

# -------------------------
# RETURNS ANALYSIS
# -------------------------

returns_query = """
SELECT
    stockcode,
    description,
    SUM(quantity) AS total_returns_quantity,
    SUM(quantity * price) AS total_returns_value
FROM retail_data
WHERE quantity < 0
GROUP BY stockcode, description
ORDER BY total_returns_quantity ASC
"""

returns_summary = pd.read_sql_query(returns_query, conn)

# -------------------------
# MONTHLY SALES TREND
# -------------------------

monthly_sales_query = """
SELECT
    year,
    month,
    SUM(quantity * price) AS monthly_revenue
FROM retail_data
WHERE quantity > 0
GROUP BY year, month
ORDER BY year, month
"""

monthly_sales = pd.read_sql_query(monthly_sales_query, conn)

conn.close()

print("Saving results for Power BI...")

sales_summary.to_csv("sales_summary.csv", index=False)
top_products.to_csv("top_products.csv", index=False)
customer_activity.to_csv("customer_activity.csv", index=False)
returns_summary.to_csv("returns_summary.csv", index=False)
monthly_sales.to_csv("monthly_sales.csv", index=False)

print("SQL analytics pipeline completed successfully!")