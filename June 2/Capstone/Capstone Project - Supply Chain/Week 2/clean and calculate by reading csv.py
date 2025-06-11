import pandas as pd
import numpy as np

# Load orders.csv and inventory CSV files
orders = pd.read_csv('orders.csv')
inventory = pd.read_csv('inventory.csv')

# Clean columns names
orders.columns = orders.columns.str.strip().str.lower()
inventory.columns = inventory.columns.str.strip().str.lower()

# Convert order_date to datetime
orders['order_date'] = pd.to_datetime(orders['order_date'], errors='coerce')

# Handle delivery_date if exists (your SQL data doesn't have delivery_date, so this part is optional)
if 'delivery_date' in orders.columns:
    orders['delivery_date'] = pd.to_datetime(orders['delivery_date'], errors='coerce')
    orders['delivery_date'] = orders['delivery_date'].fillna(pd.Timestamp.today())
    orders['delay_days'] = (orders['delivery_date'] - orders['order_date']).dt.days
    orders['delayed'] = np.where(orders['delay_days'] > 3, 1, 0)
else:
    orders['delay_days'] = np.nan
    orders['delayed'] = 0

# Drop rows with missing critical data in orders.csv
orders.dropna(subset=['order_date', 'supplier_id', 'quantity', 'status'], inplace=True)

# Convert numeric columns in orders.csv
orders['quantity'] = pd.to_numeric(orders['quantity'], errors='coerce')
orders.dropna(subset=['quantity'], inplace=True)

# For inventory: drop rows with missing critical data
inventory.dropna(subset=['supplier_id', 'item_name', 'quantity'], inplace=True)

# Convert quantity in inventory to numeric
inventory['quantity'] = pd.to_numeric(inventory['quantity'], errors='coerce')
inventory.dropna(subset=['quantity'], inplace=True)

# Calculate total quantity ordered per supplier
total_ordered = orders.groupby('supplier_id')['quantity'].sum().reset_index().rename(columns={'quantity': 'total_ordered'})

# Merge inventory with total orders.csv to calculate current stock levels
supplier_stock = pd.merge(inventory, total_ordered, on='supplier_id', how='left')

# Fill missing total_ordered with 0
supplier_stock['total_ordered'] = supplier_stock['total_ordered'].fillna(0)

# Calculate estimated stock after orders.csv
supplier_stock['estimated_stock'] = supplier_stock['quantity'] - supplier_stock['total_ordered']

# Display results
print("\nOrders Data (Cleaned & Processed):")
print(orders.head())

print("\nInventory with Estimated Stock Levels:")
print(supplier_stock.head())

# Save cleaned and processed data if needed
orders.to_csv('cleaned_orders.csv', index=False)
supplier_stock.to_csv('supplier_stock_levels.csv', index=False)
