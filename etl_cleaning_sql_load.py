import pandas as pd
import sqlite3

print("Starting ETL Pipeline...")

# -------------------------
# EXTRACT
# -------------------------

print("Loading datasets...")

df1 = pd.read_excel("online_retail_II.xlsx", sheet_name="Year 2009-2010")
df2 = pd.read_excel("online_retail_II.xlsx", sheet_name="Year 2010-2011")

df_comb = pd.concat([df1, df2], ignore_index=True)

print("Total rows loaded:", len(df_comb))

# -------------------------
# TRANSFORM - CLEANING
# -------------------------

print("Cleaning dataset...")

# Drop missing values
df_nonan = df_comb.dropna().copy()

# Remove duplicates
df_clean = df_nonan.drop_duplicates().copy()

# Convert InvoiceDate
df_clean["InvoiceDate"] = pd.to_datetime(df_clean["InvoiceDate"], errors="coerce")

# Feature Engineering
df_clean["Year"] = df_clean["InvoiceDate"].dt.year
df_clean["Month"] = df_clean["InvoiceDate"].dt.month
df_clean["DayOfWeek"] = df_clean["InvoiceDate"].dt.day_name()
df_clean["Week"] = df_clean["InvoiceDate"].dt.isocalendar().week
df_clean["Quarter"] = df_clean["InvoiceDate"].dt.quarter

# Convert Customer ID
df_clean["Customer ID"] = df_clean["Customer ID"].astype("int")

# Remove invalid transactions
df_clean = df_clean[(df_clean["Quantity"] > 0) & (df_clean["Price"] > 0)]

# Create Revenue column
df_clean["Revenue"] = df_clean["Quantity"] * df_clean["Price"]

# Rename columns for SQL compatibility
df_clean.columns = df_clean.columns.str.lower().str.replace(" ", "_")

print("Rows after cleaning:", len(df_clean))

# -------------------------
# LOAD
# -------------------------

print("Saving to SQLite database...")

conn = sqlite3.connect("online_retail_clean.db")

df_clean.to_sql("retail_data", conn, index=False, if_exists="replace")

conn.close()

print("ETL pipeline completed successfully!")
# Optimized SQL connection
