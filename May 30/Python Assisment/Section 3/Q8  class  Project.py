class Employee():
    def __init__(self,name=None,salary=0):
        self.name=name
        self.salary=salary
    def display(self):
        print(f"Employee {self.name} salary is {self.salary}")
    def is_higher_earner(self):
        return self.salary>60000

class Project(Employee):
    def __init__(self,name=None,salary=0,project_name=None,hours_allocated=0):
        super().__init__(name,salary)
        self.project_name=project_name
        self.hours_allocated=hours_allocated

    def display(self):
        print("")
        print(f"Employee '{self.name}' salary is {self.salary}")
        print(f"Project allocated '{self.project_name}' with {self.hours_allocated} hours.")

try:
    name=input("Enter the name of the Employee:")
    salary=int(input("Enter the salary of the Employee:"))
    project_name=input("Enter the name of the project to be allocated:")
    hours=int(input("Enter hours to be allocated:"))
    proj=Project(name,salary,project_name,hours)
    proj.display()

except ValueError as e:
    print(e)


