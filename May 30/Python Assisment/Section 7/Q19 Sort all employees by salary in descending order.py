import pandas as pd

employees_df = pd.read_csv("employees.csv")

sorted_df = employees_df.sort_values(by='Salary', ascending=False)

print(sorted_df)
