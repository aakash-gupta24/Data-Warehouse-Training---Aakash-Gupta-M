students = {}

for i in range(3):
    name = input(f"Enter name of student {i+1}: ")
    marks = float(input(f"Enter marks for {name}: "))
    students[name] = marks

search_name = input("Enter the name of the student to view their grade: ")

if search_name in students:
    score = students[search_name]
    if score >= 90:
        grade = 'A'
    elif score >= 75:
        grade = 'B'
    else:
        grade = 'C'
    print(f"{search_name}'s grade is: {grade}")
else:
    print("Student not found.")