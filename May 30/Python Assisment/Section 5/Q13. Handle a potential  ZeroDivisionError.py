def divide(number1,number2):
    try:
        return number1/number2
    except ZeroDivisionError as ze:
        print("\nError Division by 0.")
        exit()
    except Exception as e:
        print("\nError occured!!!")
        exit()
try:
    number1=int(input("Enter the numerator :"))
    number2 = int(input("Enter the denominator :"))
    print(f"\n{number1} / {number2} => {divide(number1,number2)}")



except ValueError as e:
    print("Enter a number!!! Invalid input.")
