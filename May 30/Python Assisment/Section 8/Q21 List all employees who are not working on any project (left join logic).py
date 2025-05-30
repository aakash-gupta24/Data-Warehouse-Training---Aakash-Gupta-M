import pandas as pd

employees_df = pd.read_csv("employees.csv")
projects_df = pd.read_csv("projects.csv")

merged_df = pd.merge(employees_df, projects_df, on='EmployeeID', how='left')

no_project_employees = merged_df[merged_df['ProjectName'].isna()]

print(no_project_employees[['EmployeeID', 'Name', 'Department', 'Salary']])
