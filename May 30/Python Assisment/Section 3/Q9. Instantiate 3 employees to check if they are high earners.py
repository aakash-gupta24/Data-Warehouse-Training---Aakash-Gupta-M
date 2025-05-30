class Employee():
    def __init__(self,name=None,salary=0):
        self.name=name
        self.salary=salary
    def display(self):
        print(f"Employee '{self.name}' salary is '{self.salary}'")
    def is_higher_earner(self):
        return self.salary>60000
employees=[]
for x in range(3):
    try:
        name=input("Enter the name of the employee:")
        salary=int(input("Enter the salary of the employee:"))
        employees.append(Employee(name,salary))
    except ValueError as e:
        print(e)
for employee in employees:
    print("")
    employee.display()
    print(f"'{employee.name}' is a high earner ? -> {employee.is_higher_earner()}")
