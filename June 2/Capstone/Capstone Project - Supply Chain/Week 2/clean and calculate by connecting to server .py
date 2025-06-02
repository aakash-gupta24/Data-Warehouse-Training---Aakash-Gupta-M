import pandas as pd
import numpy as np
import pyodbc

# --- SQL Server connection details ---
server = 'AK-XIAOMI'
database = 'SupplyChainMonitoring'

conn_str = (
    'DRIVER={SQL Server};'
    f'SERVER={server};'
    f'DATABASE={database};'
    'Trusted_Connection=yes;'
)

# --- Connect to SQL Server ---
conn = pyodbc.connect(conn_str)

# --- Load data from SQL tables instead of CSV ---
orders = pd.read_sql('SELECT * FROM orders', conn)
inventory = pd.read_sql('SELECT * FROM inventory', conn)

# Close connection if not used further
conn.close()

# --- Clean column names ---
orders.columns = orders.columns.str.strip().str.lower()
inventory.columns = inventory.columns.str.strip().str.lower()

# --- Convert order_date to datetime ---
orders['order_date'] = pd.to_datetime(orders['order_date'], errors='coerce')

# --- Convert delivery_date if it exists ---
if 'delivery_date' in orders.columns:
    orders['delivery_date'] = pd.to_datetime(orders['delivery_date'], errors='coerce')
    orders['delivery_date'] = orders['delivery_date'].fillna(pd.Timestamp.today())
    orders['delay_days'] = (orders['delivery_date'] - orders['order_date']).dt.days
    orders['delayed'] = np.where(orders['delay_days'] > 3, 1, 0)
else:
    orders['delay_days'] = np.nan
    orders['delayed'] = 0

# --- Clean orders ---
orders.dropna(subset=['order_date', 'supplier_id', 'quantity', 'status'], inplace=True)
orders['quantity'] = pd.to_numeric(orders['quantity'], errors='coerce')
orders.dropna(subset=['quantity'], inplace=True)

# --- Clean inventory ---
inventory.dropna(subset=['supplier_id', 'item_name', 'quantity'], inplace=True)
inventory['quantity'] = pd.to_numeric(inventory['quantity'], errors='coerce')
inventory.dropna(subset=['quantity'], inplace=True)

# --- Total quantity ordered per supplier ---
total_ordered = orders.groupby('supplier_id')['quantity'].sum().reset_index().rename(columns={'quantity': 'total_ordered'})

# --- Merge inventory with total ordered ---
supplier_stock = pd.merge(inventory, total_ordered, on='supplier_id', how='left')
supplier_stock['total_ordered'] = supplier_stock['total_ordered'].fillna(0)

# --- Estimated stock after fulfilling orders ---
supplier_stock['estimated_stock'] = supplier_stock['quantity'] - supplier_stock['total_ordered']

# --- Display results ---
print("\nOrders Data (Cleaned & Processed):")
print(orders.head())

print("\nInventory with Estimated Stock Levels:")
print(supplier_stock.head())

# --- Optional: Save results locally ---
orders.to_csv('cleaned_orders.csv', index=False)
supplier_stock.to_csv('supplier_stock_levels.csv', index=False)
