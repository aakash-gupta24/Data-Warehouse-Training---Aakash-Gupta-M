import pandas as pd

employees_df=pd.read_csv("employees.csv",index_col='EmployeeID')
project_df=pd.read_csv("projects.csv",index_col='ProjectID')

print("first 2 employee details:\n",employees_df.head(2))
print("")
unique_departments = employees_df['Department'].unique()
print("Unique Departments:", unique_departments)
print("")
avg_salary_by_dept = employees_df.groupby('Department')['Salary'].mean()
print("Average Salary by Department:\n", avg_salary_by_dept)
