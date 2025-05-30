import pandas as pd

df=pd.read_csv("employees.csv")
it_employees=df[df['Department']=='IT']['Name']


with open("Employees_Belong_to_IT.txt","w") as f:
    for x in it_employees:
        f.write(x+"\n")

