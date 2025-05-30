import pandas as pd
from datetime import datetime

employees_df=pd.read_csv("employees.csv",index_col='EmployeeID')
project_df=pd.read_csv("projects.csv",index_col='ProjectID')

employees_df['JoiningDate'] = pd.to_datetime(employees_df['JoiningDate'])

current_year = datetime.now().year

employees_df['TenureInYears'] = current_year - employees_df['JoiningDate'].dt.year

print(employees_df[['Name', 'JoiningDate', 'TenureInYears']])