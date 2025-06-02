import pandas as pd
import numpy as np

# Load CSV files
customers = pd.read_csv('customers.csv')
orders = pd.read_csv('orders.csv')
delivery_status = pd.read_csv('delivery_status.csv')

# Data Cleaning: drop rows with missing values
customers.dropna(inplace=True)
orders.dropna(inplace=True)
delivery_status.dropna(inplace=True)

# Convert date columns to datetime
orders['order_date'] = pd.to_datetime(orders['order_date'])
orders['delivery_date'] = pd.to_datetime(orders['delivery_date'])
delivery_status['status_date'] = pd.to_datetime(delivery_status['status_date'])

# Calculate delivery delay (days between delivery_date and order_date)
orders['delivery_delay'] = (orders['delivery_date'] - orders['order_date']).dt.days

# Merge orders with delivery_status on order_id to get status for each order
orders = orders.merge(delivery_status[['order_id', 'current_status']], left_index=True, right_on='order_id', how='left')

# Merge orders with customers on customer_id to get customer info
orders = orders.merge(customers, left_on='customer_id', right_index=True, how='left')

# Show top delayed customers: group by customer name, average delay
top_delayed_customers = orders.groupby('name')['delivery_delay'].mean().sort_values(ascending=False).head(10)
print("Top 10 customers by average delivery delay:")
print(top_delayed_customers)

# Most common delivery issues: count statuses excluding 'Delivered'
issues = delivery_status[delivery_status['current_status'] != 'Delivered']
common_issues = issues['current_status'].value_counts()
print("\nMost common delivery issues:")
print(common_issues)
