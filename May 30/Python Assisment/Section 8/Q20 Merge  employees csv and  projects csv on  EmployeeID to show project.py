import pandas as pd

employees_df = pd.read_csv("employees.csv")
projects_df = pd.read_csv("projects.csv")

merged_df = pd.merge(employees_df, projects_df, on='EmployeeID')

result = merged_df[['EmployeeID', 'Name', 'ProjectName']]

print(result)
