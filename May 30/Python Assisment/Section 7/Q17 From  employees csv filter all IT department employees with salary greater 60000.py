import pandas as pd

employees_df = pd.read_csv("employees.csv")

filtered_df = employees_df[(employees_df['Department'] == 'IT') & (employees_df['Salary'] > 60000)]

print(filtered_df)
