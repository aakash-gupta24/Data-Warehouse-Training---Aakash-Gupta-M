class Employee():
    def __init__(self,name=None,salary=0):
        self.name=name
        self.salary=salary
    def display(self):
        print(f"Employee '{self.name}' salary is '{self.salary}'")
    def is_higher_earner(self):
        return self.salary>60000

try:
    name=input("Enter the name of the employee:")
    salary=int(input("Enter the salary of the employee:"))
    emp=Employee(name,salary)
    print("")
    emp.display()
    print(f"'{emp.name}' is a high earner ? -> {emp.is_higher_earner()}")
except ValueError as e:
    print(e)
