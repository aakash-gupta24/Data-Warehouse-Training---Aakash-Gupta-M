def calculate_grade(percentage):
    if percentage >= 90:
        return "A+"
    elif percentage >= 80:
        return "A"
    elif percentage >= 70:
        return "B"
    elif percentage >= 60:
        return "C"
    elif percentage >= 50:
        return "D"
    else:
        return "F"

marks = []
for i in range(5):
    try:
        score = float(input(f"Enter marks for subject {i+1}: "))
        if 0 <= score <= 100:
            marks.append(score)
        else:
            print("Marks should be between 0 and 100.")
            break
    except ValueError:
        print("Invalid input. Please enter a number.")
        break
else:
    total = sum(marks)
    average = total / 5
    percentage = (total / 500) * 100
    grade = calculate_grade(percentage)

    print("\n--- Result ---")
    print(f"Total Marks: {total}/500")
    print(f"Average Marks: {average:.2f}")
    print(f"Percentage: {percentage:.2f}%")
    print(f"Grade: {grade}")
