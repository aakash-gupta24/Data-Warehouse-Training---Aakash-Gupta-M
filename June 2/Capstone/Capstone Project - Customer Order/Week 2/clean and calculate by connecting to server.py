import pandas as pd
import pyodbc

# --- Setup connection to MySQL via ODBC ---

server = 'AK-XIAOMI'    
database = 'customerOrders'


# Define the connection string for MySQL ODBC Driver
conn_str = (
    'DRIVER={SQL Server};'
    f'SERVER={server};'
    f'DATABASE={database};'
    'Trusted_Connection=yes;'
)

# Connect
conn = pyodbc.connect(conn_str)

# Load data into pandas DataFrames
customers = pd.read_sql('SELECT * FROM customers', conn)
orders = pd.read_sql('SELECT * FROM orders', conn)
delivery_status = pd.read_sql('SELECT * FROM delivery_status', conn)

# Close connection
conn.close()

# --- Data cleaning ---
customers.dropna(inplace=True)
orders.dropna(inplace=True)
delivery_status.dropna(inplace=True)

# --- Convert timestamps ---
orders['order_date'] = pd.to_datetime(orders['order_date'])
orders['delivery_date'] = pd.to_datetime(orders['delivery_date'])
delivery_status['status_date'] = pd.to_datetime(delivery_status['status_date'])

# --- Calculate delivery delay ---
orders['delivery_delay'] = (orders['delivery_date'] - orders['order_date']).dt.days

# --- Merge delivery status ---
orders = orders.merge(delivery_status[['order_id', 'current_status']], on='order_id', how='left')

# --- Merge with customers ---
orders = orders.merge(customers, on='customer_id', how='left')

# --- Top delayed customers ---
top_delayed_customers = orders.groupby('name')['delivery_delay'].mean().sort_values(ascending=False).head(10)
print("Top 10 customers by average delivery delay:")
print(top_delayed_customers)

# --- Most common delivery issues ---
issues = delivery_status[delivery_status['current_status'] != 'Delivered']
common_issues = issues['current_status'].value_counts()
print("\nMost common delivery issues:")
print(common_issues)
