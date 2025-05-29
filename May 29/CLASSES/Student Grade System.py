class Student:
    def __init__(self, name, marks):
        self.name = name
        self.marks = marks

    def average(self):
        return sum(self.marks) / len(self.marks)

    def grade(self):
        avg = self.average()
        if avg >= 90:
            return 'A'
        elif avg >= 75:
            return 'B'
        elif avg >= 60:
            return 'C'
        else:
            return 'F'

name = input("Enter student name: ")
marks =[]
subs=int(input("Enter number of subjects:"))
for x in range(subs):
    try:
        marks.append(int(input(f"Enter the mark of sub{x+1} :")))
    except Exception as e:
        print(e)
        break
student = Student(name, marks)
print("")
print(f"Average: {student.average():.2f}")
print(f"Grade: {student.grade()}")
