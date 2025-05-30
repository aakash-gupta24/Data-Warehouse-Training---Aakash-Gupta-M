import pandas as pd

employees_df = pd.read_csv("employees.csv")
projects_df = pd.read_csv("projects.csv")

merged_df = pd.merge(employees_df, projects_df, on='EmployeeID')

merged_df['TotalCost'] = merged_df['HoursAllocated'] * (merged_df['Salary'] / 160)

print(merged_df[['EmployeeID', 'Name', 'ProjectName', 'HoursAllocated', 'Salary', 'TotalCost']])
