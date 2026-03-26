# SALES REVENUE PREDICTION MODEL

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score

import joblib


print("Starting Sales Prediction Model Pipeline...")

# --------------------------------------------------
# STEP 1 — LOAD DATA FROM DATABASE
# --------------------------------------------------

print("Connecting to database...")

conn = sqlite3.connect("online_retail_clean.db")

query = """
SELECT
    year,
    month,
    SUM(quantity * price) AS revenue
FROM retail_data
WHERE quantity > 0
GROUP BY year, month
ORDER BY year, month
"""

df = pd.read_sql_query(query, conn)

conn.close()

print("Dataset loaded successfully")
print(df.head())

# --------------------------------------------------
# STEP 2 — FEATURE ENGINEERING
# --------------------------------------------------

# Features (inputs)
X = df[["year", "month"]]

# Target (what we want to predict)
y = df["revenue"]

# --------------------------------------------------
# STEP 3 — TRAIN TEST SPLIT
# --------------------------------------------------

X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
    test_size=0.2,
    random_state=42
)

print("Training samples:", len(X_train))
print("Testing samples:", len(X_test))

# --------------------------------------------------
# STEP 4 — MODEL TRAINING
# --------------------------------------------------

print("Training Linear Regression model...")

model = LinearRegression()

model.fit(X_train, y_train)

print("Model training completed")

# --------------------------------------------------
# STEP 5 — MODEL PREDICTION
# --------------------------------------------------

y_pred = model.predict(X_test)

# --------------------------------------------------
# STEP 6 — MODEL EVALUATION
# --------------------------------------------------

mse = mean_squared_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)

print("\nModel Performance Metrics")
print("-------------------------")
print("Mean Squared Error:", mse)
print("R² Score:", r2)

# --------------------------------------------------
# STEP 7 — SAVE TRAINED MODEL
# --------------------------------------------------

joblib.dump(model, "sales_prediction_model.pkl")

print("Model saved as sales_prediction_model.pkl")

# --------------------------------------------------
# STEP 8 — VISUALIZATION
# --------------------------------------------------

plt.figure(figsize=(10,6))

plt.scatter(range(len(y_test)), y_test, label="Actual Revenue")
plt.scatter(range(len(y_pred)), y_pred, label="Predicted Revenue")

plt.title("Actual vs Predicted Monthly Revenue")
plt.xlabel("Test Data Points")
plt.ylabel("Revenue")

plt.legend()

plt.savefig("revenue_prediction_plot.png")

plt.show()

print("Visualization saved as revenue_prediction_plot.png")

print("\nSales Prediction Pipeline Completed Successfully!")