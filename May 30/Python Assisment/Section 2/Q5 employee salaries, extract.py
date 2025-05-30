import pandas as pd
import numpy as np

df=pd.read_csv("employees.csv")
salary=df["Salary"].values
print(f"\nThe maximum salary is {np.max(salary)}")
avg_salary=np.mean(salary)
print(f"\nThe salary above the avg salary {avg_salary} are:")
for x in salary:
    if(x>=avg_salary):
        print(x)

sorted_desc = np.sort(salary)[::-1]
print("\nSalary sorted in decending order:")
for x in sorted_desc:
    print(x)


