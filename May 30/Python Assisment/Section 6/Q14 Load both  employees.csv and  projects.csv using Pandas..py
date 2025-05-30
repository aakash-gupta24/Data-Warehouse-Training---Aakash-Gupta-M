import pandas as pd

employees_df=pd.read_csv("employees.csv",index_col='EmployeeID')
project_df=pd.read_csv("projects.csv",index_col='ProjectID')

print("Employees csv:")
print(employees_df)

print("\nProject csv:")
print(project_df)
