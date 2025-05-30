import pandas as pd

employees_df = pd.read_csv("employees.csv")

department_stats = employees_df.groupby('Department').agg(
    EmployeeCount=('EmployeeID', 'count'),
    TotalSalary=('Salary', 'sum'),
    AverageSalary=('Salary', 'mean')
)

print(department_stats)
