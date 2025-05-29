def factorial(n):
    if n < 0:
        raise ValueError("Factorial is not defined for negative numbers.")
    elif n == 0 or n == 1:
        return 1
    else:
        return n * factorial(n - 1)

num = int(input("Enter a number: "))
try:
    result = factorial(num)
    print(f"Factorial of {num} is {result}")
except ValueError as e:
    print(e)
